# Servidor central de campeonato - Placar de Tenis de Mesa
# Modos:
#   absoluto -> chaveamento em dupla eliminacao (2 derrotas eliminam,
#               chave de vencedores + chave de perdedores + grande final)
#   grupos   -> fase de grupos (2 por grupo avancam para a chave)
#   chave    -> mata-mata (eliminacao simples)
#   misto    -> sistema misto: fase de grupos (rodizio) e depois duas
#               chaves de eliminacao simples: DIVISAO OURO (classificados
#               dos grupos) e DIVISAO PRATA (nao classificados)
# Cabecas de chave (ranking) sao distribuidas para nao se enfrentarem no inicio.
# O ESP32 envia o resultado via POST /api/partida ao apertar NOVO JOGO.
# Tambem aceita registro manual pela pagina web.

import os
import sys
import json
import math
import copy
import random
import socket
import threading
import datetime
import time
import concurrent.futures
import urllib.request
import urllib.parse

_bateria_lock = threading.Lock()

_rede = set()      # IPs das placas conhecidas (outbound, nao depende de entrada no notebook)
_ultima_varredura_completa = 0
_campeao_push_ts = 0.0
_DEBUG_LOG_PATH = os.path.join(
    os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__)),
    "debug_campeonato.log")


def _log(mensagem):
    try:
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as arquivo:
            arquivo.write("%s  %s\n" % (datetime.datetime.now().strftime("%H:%M:%S"), mensagem))
    except Exception:
        pass


def _meu_ip_privado():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(2)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "192.168.0.22"
    finally:
        s.close()


def _subnets():
    base = ".".join(_meu_ip_privado().split(".")[0:3])
    return [base]


def porta_80_aberta(ip, timeout=0.35):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((ip, 80))
        return True
    except Exception:
        return False
    finally:
        s.close()


def _ja_registrado(a, b, sA, sB):
    """Evita gravar duplicado quando o mesmo resultado chega de novo (puxada + manual)."""
    par = par_chave(a, b)
    for m in estado.get("partidas", [])[-5:]:
        if par_chave(m.get("A", ""), m.get("B", "")) == par \
                and m.get("setsA") == sA and m.get("setsB") == sB:
            return True
    return False


def puxar_mesa(ip):
    try:
        r = urllib.request.urlopen("http://%s/api/coleta" % ip, timeout=2).read().decode("utf-8", "replace")
        d = json.loads(r)
        mesa = d.get("mesa")
        if not mesa:
            return False
        mesa_norm = limpar_nome(mesa)
        try:
            b = int(d.get("b", -1))
        except (TypeError, ValueError):
            b = -1
        agora = datetime.datetime.now()
        with _bateria_lock:
            for k in list(estado["baterias"]):
                if k != mesa_norm and estado["baterias"][k].get("ip") == ip:
                    estado["baterias"].pop(k, None)
            estado["baterias"][mesa_norm] = {
                "b": b,
                "v": d.get("tensao", ""),
                "hora": agora.strftime("%H:%M"),
                "ts": agora.timestamp(),
                "ip": ip,
            }
        _rede.add(ip)

        seq_max = 0
        seq_flush = 0
        for res in (d.get("resultados") or []):
            try:
                seq = int(res.get("seq", 0))
                a = res.get("A") or ""
                bb = res.get("B") or ""
                if not a or not bb:
                    if seq > seq_flush:
                        seq_flush = seq
                    continue
                sA, sB = int(res.get("sA", 0)), int(res.get("sB", 0))
                sets_texto = res.get("sets") or ""
                sets = None
                if sets_texto.strip():
                    sets, _err = parse_sets(sets_texto)
                    if not sets:
                        sets = None
                if _ja_registrado(a, bb, sA, sB):
                    if seq > seq_flush:
                        seq_flush = seq
                    continue
                ok, erro = registrar_partida(a, bb, sA, sB, sets=sets, mesa=mesa_norm)
                if ok:
                    seq_max = max(seq_max, seq)
                    print("Coleta: partida %s x %s (%dx%d) registrada da mesa %s" % (
                        a, bb, sA, sB, mesa_norm), flush=True)
                else:
                    _log("REJEITA mesa=%s %s x %s (%dx%d) sets=%r -> %s [modo=%s elim=%s pendentes=%s]" % (
                        mesa_norm, a, bb, sA, sB, sets_texto, erro,
                        estado.get("modo"), em_eliminacao(),
                        [[p[0], p[1]] for p in pares_pendentes()[:10]]))
            except Exception:
                continue
        seq_ok = seq_flush if seq_flush >= seq_max else seq_max
        if seq_ok > 0:
            try:
                urllib.request.urlopen("http://%s/api/coletado" % ip,
                                       data=("ate=%d" % seq_ok).encode(), timeout=2)
            except Exception:
                pass

        livre = bool(d.get("livre"))
        cmp_mesa = bool(d.get("cmp"))
        if estado.get("iniciado") and livre:
            # Auto-cura do campeao: se esta mesa perdeu o push (placa estava fora
            # de _rede, reinicio do servidor ou acao cancelar) e esta livre,
            # reenvia NA HORA o campeao, sem esperar o throttle de 20s.
            texto = texto_campeao_atual()
            if texto and d.get("camp") != texto:
                try:
                    body = urllib.parse.urlencode({"acao": "campeao", "nome": texto}).encode()
                    urllib.request.urlopen("http://%s/api/ordem" % ip, data=body, timeout=2)
                    print("Campeao reenviado a mesa %s" % mesa_norm, flush=True)
                except Exception:
                    pass
        if estado.get("iniciado"):
            if livre:
                p = proxima_partida(mesa_norm)
                if p:
                    if not enviar_chamada(ip, p[0], p[1], mesa_norm):
                        _liberar_mesa(mesa_norm)
                        print("Placa %s nao aceitou %s x %s - jogo liberado" % (mesa_norm, p[0], p[1]), flush=True)
        elif cmp_mesa:
            # Campeonato nao esta ativo mas a placa ainda esta travada em modo
            # campeonato (sobra de uma sessao anterior): devolve para o modo
            # livre/avulso, liberando a mesa para uso normal no placar.
            try:
                body = urllib.parse.urlencode({"acao": "avulso"}).encode()
                urllib.request.urlopen("http://%s/api/ordem" % ip, data=body, timeout=2)
            except Exception:
                pass
        return True
    except Exception:
        import traceback
        traceback.print_exc()
        return False


def comandar_placas(acao):
    """Envia um comando /api/ordem para todas as placas conhecidas da rede.
    Usado para 'campeonato' (armar as mesas ao iniciar) e 'avulso'
    (liberar as mesas ao zerar/cancelar o campeonato)."""
    enviado = 0
    for ip in list(_rede):
        try:
            body = urllib.parse.urlencode({"acao": acao}).encode()
            urllib.request.urlopen("http://%s/api/ordem" % ip, data=body, timeout=2)
            enviado += 1
        except Exception:
            pass
    return enviado


def enviar_chamada(ip, a, b, mesa):
    """Envia uma chamada de partida para a placa. Retorna True se a placa aceitou."""
    try:
        fmt_efetivo = int(formato_da_partida(a, b))
        modo_placa = ("chave" if em_eliminacao() else estado.get("modo", ""))
        fm = int(estado.get("formato_mata") or 3)
        fg = int(estado.get("formato_grupos") or 3)
        ff = int(estado.get("formato_final") or 3)
        body = urllib.parse.urlencode({
            "acao": "chamar", "A": a, "B": b,
            "modo": modo_placa,
            "formato": fmt_efetivo,
            "formatoMata": fmt_efetivo if modo_placa == "chave" else fm,
            "formatoGrupos": fmt_efetivo if modo_placa != "chave" else fg,
            "formatoFinal": ff,
        }).encode()
        resp = urllib.request.urlopen("http://%s/api/ordem" % ip, data=body, timeout=2).read().decode("utf-8", "ignore")
        return '"ok":true' in resp or '"ok": true' in resp
    except Exception:
        return False


def _liberar_mesa(mesa):
    """Libera o jogo atribuido a uma mesa (placa recusou, falha ou saiu da rede)."""
    estado.setdefault("mesas", {}).pop(mesa, None)
    em = estado.get("em_andamento")
    if em and em.get("mesa") == mesa:
        estado["em_andamento"] = None
    salvar()


def cancelar_chamado(mesa):
    """Cancela na placa a partida chamada (atribuicao) sem apagar resultados."""
    info = estado.get("baterias", {}).get(mesa) or {}
    ip = info.get("ip") or ""
    if ip:
        try:
            body = urllib.parse.urlencode({"acao": "cancelar"}).encode()
            urllib.request.urlopen("http://%s/api/ordem" % ip, data=body, timeout=2)
        except Exception:
            pass
    estado.setdefault("mesas", {}).pop(mesa, None)
    em = estado.get("em_andamento")
    if em and em.get("mesa") == mesa:
        estado["em_andamento"] = None
    salvar()


def _avancar_mesa(mesa):
    """Apos um registro manual (sem mesa): reseta a placa que exibia o jogo
    registrado e chama a proxima partida pendente para ela."""
    info = estado.get("baterias", {}).get(mesa) or {}
    ip = info.get("ip") or ""
    if ip:
        # A placa ainda acha que o jogo esta em andamento; reseta para ela
        # aceitar a proxima chamada (sem apagar nenhum resultado).
        try:
            body = urllib.parse.urlencode({"acao": "cancelar"}).encode()
            urllib.request.urlopen("http://%s/api/ordem" % ip, data=body, timeout=2)
        except Exception:
            pass
    p = proxima_partida(mesa)
    if not p or not ip:
        salvar()
        return
    aceita = False
    try:
        fmt_efetivo = int(formato_da_partida(p[0], p[1]))
        modo_placa = "chave" if em_eliminacao() else estado.get("modo", "")
        fm = int(estado.get("formato_mata") or 3)
        fg = int(estado.get("formato_grupos") or 3)
        ff = int(estado.get("formato_final") or 3)
        body = urllib.parse.urlencode({
            "acao": "chamar", "A": p[0], "B": p[1],
            "modo": modo_placa,
            "formato": fmt_efetivo,
            "formatoMata": fmt_efetivo if modo_placa == "chave" else fm,
            "formatoGrupos": fmt_efetivo if modo_placa != "chave" else fg,
            "formatoFinal": ff,
        }).encode()
        resp = urllib.request.urlopen("http://%s/api/ordem" % ip, data=body, timeout=2).read().decode("utf-8", "ignore")
        aceita = '"ok":true' in resp or '"ok": true' in resp
    except Exception:
        pass
    if not aceita:
        _liberar_mesa(mesa)
        print("Placa %s nao aceitou %s x %s - jogo liberado" % (mesa, p[0], p[1]), flush=True)
    salvar()


def varredura_periodica():
    """Mantem as mesas online puxando de cada ESP (out -> ESP), sem depender
    do POST /api/bateria (que falha quando o notebook nao aceita entrada)."""
    global _ultima_varredura_completa
    while True:
        try:
            notificar_campeao()
            agora = time.time()
            for m in list(estado.get("mesas", {})):
                b = estado.get("baterias", {}).get(m)
                if not b or not b.get("ts") or agora - b["ts"] > 120:
                    _liberar_mesa(m)
                    print("Mesa %s sem contato - jogo liberado" % m, flush=True)
            alvos = set(_rede)
            if agora - _ultima_varredura_completa > 20:   # varre a rede toda a cada com a
                _ultima_varredura_completa = agora
                for base in _subnets():
                    for i in range(1, 255):
                        alvos.add("%s.%d" % (base, i))
            abertas = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=50) as ex:
                for ok, ip in zip(ex.map(porta_80_aberta, list(alvos)), list(alvos)):
                    if ok:
                        abertas.append(ip)
            for ip in abertas:
                puxar_mesa(ip)
        except Exception:
            pass
        time.sleep(2)

from flask import Flask, request, jsonify, make_response, Response

app = Flask(__name__)

PORTA_DESCOBERTA = 7777
MSG_DESCOBERTA = b"CAMP_DESCOBERTA"
MSG_RESPOSTA = b"CAMP_AQUI"


def servico_descoberta():
    """Escuta UDP broadcast das placas para anunciar o IP deste servidor."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("", PORTA_DESCOBERTA))
    except OSError:
        return
    s.settimeout(1.0)
    while True:
        try:
            dados, origem = s.recvfrom(1024)
        except socket.timeout:
            continue
        except OSError:
            break
        if dados.startswith(MSG_DESCOBERTA):
            try:
                s.sendto(MSG_RESPOSTA, (origem[0], PORTA_DESCOBERTA))
            except OSError:
                pass


def iniciar_descoberta():
    t = threading.Thread(target=servico_descoberta, daemon=True)
    t.start()


@app.after_request
def no_cache(resp):
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

def _caminho_bundle():
    if getattr(sys, "frozen", False):
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))


def _caminho_dados():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def _ler_html():
    """Carrega o template principal (templates/index.html), com fallback para
    o diretorio do codigo, do exec or do bundle do PyInstaller."""
    candidatos = [
        os.path.join(_caminho_bundle(), "templates", "index.html"),
        os.path.join(_caminho_bundle(), "index.html"),
        os.path.join(_caminho_dados(), "templates", "index.html"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html"),
    ]
    for p in candidatos:
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                return f.read()
    raise RuntimeError("Arquivo templates/index.html nao encontrado. Candidatos testados: %s" % candidatos)


ARQUIVO = os.path.join(_caminho_dados(), "campeonato.json")
ARQUIVO_TMP = ARQUIVO + ".tmp"


def _carregar_json_de(caminho):
    """Carrega um JSON de `caminho`. Retorna (dict, erro_ou_None)."""
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            dados = json.load(f)
        if not isinstance(dados, dict):
            return None, "conteudo nao e um objeto"
        return dados, None
    except Exception as e:
        return None, "%s: %s" % (type(e).__name__, e)


def _backup_rotativo(origem):
    """Mantém até 5 backups datados de `origem` e retorna o caminho criado.

    O mais recente vai para campeonato.bak.0.json; os demais são rotacionados
    (bak.0 -> bak.1 -> ...), removendo o mais antigo.
    """
    if not os.path.exists(origem):
        return None
    base = os.path.dirname(origem) or "."
    nome = os.path.basename(origem)
    max_baks = 5
    novo = os.path.join(base, nome.replace(".json", ".bak.0.json"))
    # rotaciona os antigos antes de criar o novo (0 -> 1, 1 -> 2, ...)
    for i in range(max_baks - 1, -1, -1):
        atual = os.path.join(base, nome.replace(".json", ".bak.%d.json" % i))
        if not os.path.exists(atual):
            continue
        if i == max_baks - 1:
            _remove_quieto(atual)
        else:
            _rename_quieto(atual, os.path.join(base, nome.replace(".json", ".bak.%d.json" % (i + 1))))
    _copy_quieto(origem, novo)
    return novo


def _remove_quieto(p):
    try:
        os.remove(p)
    except OSError:
        pass


def _rename_quieto(o, n):
    try:
        os.replace(o, n)
    except OSError:
        pass


def _copy_quieto(o, n):
    try:
        import shutil
        shutil.copyfile(o, n)
    except OSError:
        pass


def salvar():
    """Grava o estado com escrita atomica (arquivo temporario + rename).

    Antes de sobrescrever, o arquivo atual e copiado para um backup datado.
    Se a gravacao falhar, o campeonato.json original fica intacto.
    """
    try:
        blob = json.dumps(estado, ensure_ascii=False, indent=2)
    except Exception as e:
        _log("ERRO serializando estado: %r" % (e,))
        return False
    try:
        with open(ARQUIVO_TMP, "w", encoding="utf-8") as f:
            f.write(blob)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        _log("ERRO gravando temporario %s: %r" % (ARQUIVO_TMP, e))
        _remove_quieto(ARQUIVO_TMP)
        return False
    _backup_rotativo(ARQUIVO)
    try:
        os.replace(ARQUIVO_TMP, ARQUIVO)
    except Exception as e:
        _log("ERRO movendo para %s: %r" % (ARQUIVO, e))
        _remove_quieto(ARQUIVO_TMP)
        return False
    return True

estado = {
    "cabecas": [],
    "jogadores": [],
    "modo": "absoluto",
    "formato_grupos": 3,
    "formato_mata": 3,
    "formato_final": 3,
    "avancar": 2,
    "distribuicao": "serpentina",
    "ranking": [],
    "grupos": [],
    "partidas": [],
    "chave": [],
    "chave_prata": [],
    "dupla": None,
    "baterias": {},
    "em_andamento": None,
    "iniciado": False,
}

NO_MATCH_WARNING = ""


def _normalizar_grupo(g):
    """Garante que um grupo tenha 'jogadores' como lista (arquivos antigos guardavam string)."""
    if not isinstance(g, dict):
        return {"nome": "Grupo", "jogadores": []}
    jog = g.get("jogadores") or []
    if isinstance(jog, str):
        jog = [j for j in jog.split() if j.strip()]
    elif not isinstance(jog, list):
        jog = []
    d = {"nome": g.get("nome") or "Grupo", "jogadores": jog}
    # remove classificacao persistida antiga (sempre calculada na leitura)
    d.pop("classificacao", None)
    return d


def carregar():
    global estado
    novo = None
    origem = None
    dados, erro = _carregar_json_de(ARQUIVO)
    if dados is not None:
        novo, origem = dados, ARQUIVO
    else:
        # campeonato.json corrompido ou ilegivel: tenta recuperar dos backups
        _log("FALHA ao ler %s: %s" % (ARQUIVO, erro))
        base = os.path.dirname(ARQUIVO) or "."
        nome = os.path.basename(ARQUIVO).replace(".json", "")
        for i in range(5):
            bak = os.path.join(base, "%s.bak.%d.json" % (nome, i))
            bdados, berro = _carregar_json_de(bak)
            if bdados is not None:
                novo, origem = bdados, bak
                _log("RECUPERADO de %s" % bak)
                break
    if novo is not None:
        estado = novo
        # se recuperou de um backup, promove para o arquivo principal intacto
        if origem != ARQUIVO:
            try:
                with open(ARQUIVO, "w", encoding="utf-8") as f:
                    json.dump(estado, f, ensure_ascii=False, indent=2)
            except Exception as e:
                _log("ERRO promovendo backup para %s: %r" % (ARQUIVO, e))
    for k in ("cabecas", "avancar", "grupos", "formato_grupos", "formato_mata", "formato_final", "chave", "chave_prata"):
        if k not in estado:
            estado[k] = 2 if k == "avancar" else (3 if k in ("formato_grupos", "formato_mata", "formato_final") else [])
    for k in ("formato_grupos", "formato_mata", "formato_final"):
        if estado.get(k) not in (1, 3, 5, 7):
            estado[k] = 3
    for k in ("jogadores", "partidas"):
        if k not in estado:
            estado[k] = []
    if "modo" not in estado:
        estado["modo"] = "absoluto"
    if "distribuicao" not in estado:
        estado["distribuicao"] = "serpentina"
    if "ranking" not in estado:
        estado["ranking"] = []
    if "grupos" in estado and isinstance(estado["grupos"], list):
        estado["grupos"] = [_normalizar_grupo(g) for g in estado["grupos"]]
    if "dupla" not in estado:
        estado["dupla"] = None
    if "baterias" not in estado:
        estado["baterias"] = {}
    if "em_andamento" not in estado:
        estado["em_andamento"] = None
    if "iniciado" not in estado:
        estado["iniciado"] = False
    if "mesas" not in estado:
        estado["mesas"] = {}
    if "pref_grupo" not in estado:
        estado["pref_grupo"] = {}
    if "modo_chamada" not in estado:
        estado["modo_chamada"] = "auto"
    if "mesa_grupo" not in estado:
        estado["mesa_grupo"] = {}


def limpar_nome(n):
    return n.strip().upper()


def bloquear_se_iniciado():
    if estado.get("iniciado"):
        return jsonify({"ok": False, "erro": "Campeonato em andamento. Use CANCELAR CAMPEONATO para liberar as configuracoes."}), 400
    return None


def par_chave(a, b):
    return tuple(sorted([a, b]))


def campeao_atual():
    # Campeao do modo "absoluto" (cabeça de chave): final da dupla
    if estado.get("modo") == "absoluto":
        return (estado.get("dupla") or {}).get("champ")
    # Modo grupos/chave: vencedor do unico jogo pendente da ultima rodada
    ch = estado.get("chave") or []
    if not ch:
        return None
    ultima = ch[-1] or []
    vals = [m for m in ultima if m.get("vencedor")]
    if len(vals) == 1:
        return vals[0]["vencedor"]
    return None


def vice_campeao_atual():
    # Perdedor do jogo que definiu o campeao.
    camp = campeao_atual()
    if not camp:
        return None
    if estado.get("modo") == "absoluto":
        dupla = estado.get("dupla") or {}
        for k in ("final2", "final"):
            m = dupla.get(k)
            if m and m.get("vencedor"):
                return m["B"] if m["A"] == camp else m["A"]
        return None
    ch = estado.get("chave") or []
    if not ch:
        return None
    ultima = ch[-1] or []
    for m in ultima:
        if m.get("vencedor") == camp:
            per = m.get("B")
            if per == camp:
                per = m.get("A")
            return per
    return None


def campeao_prata_atual():
    """Campeao da DIVISAO PRATA (modo misto)."""
    if estado.get("modo") != "misto":
        return None
    ch = estado.get("chave_prata") or []
    if not ch:
        return None
    ultima = ch[-1] or []
    vals = [m for m in ultima if m.get("vencedor")]
    if len(vals) == 1:
        return vals[0]["vencedor"]
    return None


def final_pendente():
    # Par da grande final ainda sem vencedor (None se nao houver)
    if estado["modo"] == "absoluto":
        dupla = estado.get("dupla") or {}
        for k in ("final", "final2"):
            m = dupla.get(k)
            if m and m.get("A") and m.get("B") and not m.get("vencedor"):
                return par_chave(m["A"], m["B"])
        return None
    ch = estado.get("chave") or []
    if not ch:
        return None
    ultima = ch[-1] or []
    cand = [m for m in ultima
            if m.get("A") and m.get("B") and not m.get("vencedor")]
    if len(cand) == 1:
        return par_chave(cand[0]["A"], cand[0]["B"])
    return None


def texto_campeao_atual():
    """Texto do campeao (+ vice) como exibido nas placas, ou None."""
    cam = campeao_atual()
    if not cam:
        return None
    vice = vice_campeao_atual()
    return cam + "  ·  " + vice if vice else cam


def notificar_campeao():
    global _campeao_push_ts
    if estado["modo"] == "misto":
        # Modo misto tem 2 campeoes (ouro e prata) e pode haver jogos em
        # andamento nas outras chaves; nao emite mensagem de campeao.
        return
    texto = texto_campeao_atual()
    if not texto:
        return
    agora = time.time()
    # Campeao novo (ou mudou o vice): dispara na hora, sem esperar a julefa de 20s.
    # Nas reincidencias (mesmo texto) mantem o intervalo de 20s para o caso de um
    # celular desconectar/recarregar depois e ainda ver a saudacao.
    if texto != estado.get("campeao_avisado") or agora - _campeao_push_ts >= 20:
        if texto != estado.get("campeao_avisado"):
            estado["campeao_avisado"] = texto
            salvar()
        _campeao_push_ts = agora
        for ip in list(_rede):
            try:
                body = urllib.parse.urlencode({"acao": "campeao", "nome": texto}).encode()
                urllib.request.urlopen("http://%s/api/ordem" % ip, data=body, timeout=2)
            except Exception:
                pass


def todos_jogadores():
    return list(estado["jogadores"])


# ---------- Classificacao / confrontos ----------

def _ordem_sorteio(nome):
    return sum(ord(c) * (i + 31) for i, c in enumerate(nome)) % 1000003


def _desempate_cbtm(grupo, partidas):
    # grupo: nomes empatados em pontos; partidas: jogos somente entre eles
    # 2 atletas -> confronto direto
    if len(grupo) == 2:
        a, b = grupo
        for m in partidas:
            if {m["A"], m["B"]} == {a, b}:
                vencedor = a if m["setsA"] > m["setsB"] else b
                return [vencedor, b if vencedor == a else a]
    # 3+ atletas -> coef. partidas, coef. sets, coef. pontos (neste grupo)
    coef = {}
    for p in grupo:
        pro_part = contra_part = pro_set = contra_set = pro_pontos = contra_pontos = 0
        for m in partidas:
            if m["A"] == p:
                if m["setsA"] > m["setsB"]:
                    pro_part += 1
                else:
                    contra_part += 1
                pro_set += m["setsA"]
                contra_set += m["setsB"]
            elif m["B"] == p:
                if m["setsB"] > m["setsA"]:
                    pro_part += 1
                else:
                    contra_part += 1
                pro_set += m["setsB"]
                contra_set += m["setsA"]
            for pa, pb in m.get("sets", []):
                if m["A"] == p:
                    pro_pontos += pa
                    contra_pontos += pb
                elif m["B"] == p:
                    pro_pontos += pb
                    contra_pontos += pa
        cpart = pro_part / (pro_part + contra_part) if (pro_part + contra_part) else 0.0
        cset = pro_set / (pro_set + contra_set) if (pro_set + contra_set) else 0.0
        cpont = pro_pontos / (pro_pontos + contra_pontos) if (pro_pontos + contra_pontos) else 0.0
        coef[p] = (cpart, cset, cpont)
    return sorted(grupo, key=lambda p: (-coef[p][0], -coef[p][1], -coef[p][2], _ordem_sorteio(p)))


def classificacao(jogadores):
    tab = {}
    for p in jogadores:
        tab[p] = {"nome": p, "J": 0, "V": 0, "D": 0, "SetP": 0, "SetC": 0, "PontoP": 0, "PontoC": 0, "Pont": 0}
    ms = []
    for m in estado["partidas"]:
        a, b, sA, sB = m["A"], m["B"], m["setsA"], m["setsB"]
        if a not in tab or b not in tab:
            continue
        ms.append(m)
        tab[a]["J"] += 1
        tab[b]["J"] += 1
        tab[a]["SetP"] += sA
        tab[a]["SetC"] += sB
        tab[b]["SetP"] += sB
        tab[b]["SetC"] += sA
        for pa, pb in m.get("sets", []):
            tab[a]["PontoP"] += pa
            tab[a]["PontoC"] += pb
            tab[b]["PontoP"] += pb
            tab[b]["PontoC"] += pa
        if sA > sB:
            tab[a]["V"] += 1
            tab[b]["D"] += 1
            tab[a]["Pont"] += 3
        else:
            tab[b]["V"] += 1
            tab[a]["D"] += 1
            tab[b]["Pont"] += 3
    por_ponto = {}
    for p in jogadores:
        por_ponto.setdefault(tab[p]["Pont"], []).append(p)
    ordem = []
    for pt in sorted(por_ponto.keys(), reverse=True):
        grupo = por_ponto[pt]
        if len(grupo) == 1:
            ordem.append(grupo[0])
            continue
        partidas_grupo = [m for m in ms if m["A"] in grupo and m["B"] in grupo]
        if not partidas_grupo:
            # Sem partidas jogadas (ex.: classificacao inicial / cabeca de chave),
            # a ordem segue a sequencia do grupo (serpentina/ranking).
            ordem.extend([p for p in jogadores if p in grupo])
            continue
        for p in _desempate_cbtm(grupo, partidas_grupo):
            ordem.append(p)
    return [tab[p] for p in ordem]


def confrontos(jogadores):
    return [{"A": jogadores[i], "B": jogadores[j], "jogada": False}
            for i in range(len(jogadores)) for j in range(i + 1, len(jogadores))]


# ---------- Grupos ----------

def criar_grupos(num):
    if num < 2:
        return
    ordem = list(estado["jogadores"])
    if estado["distribuicao"] == "sorteio":
        random.shuffle(ordem)
    else:
        # serpentina: usar ranking quando disponivel (os primeiros sao os melhores)
        ranking = [r for r in estado["ranking"] if r in ordem]
        if ranking:
            resto = [j for j in ordem if j not in ranking]
            ordem = ranking + resto
    grupos = [{"nome": "Grupo %d" % (i + 1), "jogadores": []} for i in range(num)]
    idx = 0
    backward = False
    for j in ordem:
        grupos[idx]["jogadores"].append(j)
        if not backward:
            idx += 1
            if idx >= num:
                idx = num - 1
                backward = True
        else:
            idx -= 1
            if idx < 0:
                idx = 0
                backward = False
    estado["grupos"] = grupos
    estado["partidas"] = []
    estado["chave"] = []
    salvar()


def grupo_de(jog):
    for g in estado["grupos"]:
        if jog in g["jogadores"]:
            return g
    return None


def mover_jogador(jog, novo_idx):
    for g in estado["grupos"]:
        if jog in g["jogadores"]:
            g["jogadores"].remove(jog)
    if 0 <= novo_idx < len(estado["grupos"]):
        estado["grupos"][novo_idx]["jogadores"].append(jog)
    salvar()


def reordenar_jogador(jog, grupo_idx, posicao):
    """Move o jogador para a posicao `posicao` do grupo `grupo_idx`. Se grupo_idx
    for -1, permanece no grupo atual. `posicao` e o indice do slot no array ANTES
    da remocao; quando o jogador vem de uma posicao anterior no mesmo grupo,
    ajusta para compensar o deslocamento apos a remocao."""
    orig_idx = -1
    grupo_origem = None
    for g in estado["grupos"]:
        if jog in g["jogadores"]:
            orig_idx = g["jogadores"].index(jog)
            grupo_origem = estado["grupos"].index(g)
            g["jogadores"].remove(jog)
            if grupo_idx < 0:
                grupo_idx = grupo_origem
            break
    if not (0 <= grupo_idx < len(estado["grupos"])):
        return
    jogadores = estado["grupos"][grupo_idx]["jogadores"]
    pos = int(posicao)
    if grupo_origem == grupo_idx and orig_idx >= 0 and orig_idx < pos:
        pos -= 1
    pos = max(0, min(pos, len(jogadores)))
    jogadores.insert(pos, jog)
    salvar()


# ---------- Chave (mata-mata) ----------

NOME_RODADA = ["Final", "Semifinal", "Quartas de Final", "Oitavas de Final"]


def nome_rodada(idx, total_rodadas):
    n = total_rodadas - idx
    if n - 1 < len(NOME_RODADA):
        return NOME_RODADA[n - 1]
    return "Rodada %d" % n


def seed_positions(n):
    if n <= 1:
        return [0]
    prev = seed_positions(n // 2)
    res = []
    for p in prev:
        res.append(p)
        res.append(n - 1 - p)
    return res


def montar_chave(ordem):
    n = len(ordem)
    if n < 2:
        return []
    total = 1
    while total < n:
        total *= 2
    slots = [None] * total
    sp = seed_positions(total)
    placed = set()
    for i, p in enumerate(ordem):
        if i < len(sp):
            slots[sp[i]] = p
            placed.add(p)
    rest = [p for p in ordem if p not in placed]
    for i in range(total):
        if slots[i] is None and rest:
            slots[i] = rest.pop(0)
    rodadas = []
    r0 = []
    for i in range(0, total, 2):
        a, b = slots[i], slots[i + 1]
        if a is None and b is None:
            continue
        m = {"A": a, "B": b, "setsA": 0, "setsB": 0,
             "vencedor": None, "jogada": False, "pos": i // 2}
        if a is None and b is not None:
            m["A"], m["B"] = b, None
        if m["B"] is None and m["A"] is not None:
            m["vencedor"] = m["A"]
            m["jogada"] = True
        r0.append(m)
    rodadas.append(r0)
    while len(rodadas) < int(math.log2(total)):
        rodadas.append([])
    return rodadas


def gerar_chave():
    ordem = list(estado["jogadores"])
    if estado["distribuicao"] == "sorteio":
        random.shuffle(ordem)
    else:
        ranking = [r for r in estado["ranking"] if r in ordem]
        if ranking:
            resto = [j for j in ordem if j not in ranking]
            ordem = ranking + resto
    return montar_chave(ordem)


def qualificados_dos_grupos():
    n_avancar = max(1, int(estado.get("avancar") or 2))
    tiers = []
    for g in estado["grupos"]:
        cl = classificacao(g["jogadores"])
        for i in range(min(n_avancar, len(cl))):
            if len(tiers) <= i:
                tiers.append([])
            tiers[i].append(cl[i]["nome"])
    qualificados = []
    for i, t in enumerate(tiers):
        if i % 2 == 1:
            t = list(reversed(t))
        qualificados.extend(t)
    return qualificados


def nao_classificados_dos_grupos():
    """Jogadores que NAO avancaram (a partir da posicao `avancar`)."""
    n_avancar = max(1, int(estado.get("avancar") or 2))
    tiers = []
    for g in estado["grupos"]:
        cl = classificacao(g["jogadores"])
        for i in range(n_avancar, len(cl)):
            if len(tiers) <= i - n_avancar:
                tiers.append([])
            tiers[i - n_avancar].append(cl[i]["nome"])
    nao = []
    for i, t in enumerate(tiers):
        if i % 2 == 1:
            t = list(reversed(t))
        nao.extend(t)
    return nao


def gerar_chave_grupos():
    qual = qualificados_dos_grupos()
    chave = montar_chave(qual)
    if estado["modo"] == "misto":
        estado["chave_prata"] = montar_chave(nao_classificados_dos_grupos())
    else:
        estado["chave_prata"] = []
    return chave


def chaves_ativas():
    """Lista de chaves de eliminacao em jogo (ouro + prata no modo misto)."""
    chaves = []
    if estado.get("chave"):
        chaves.append(estado["chave"])
    if estado.get("modo") == "misto" and estado.get("chave_prata"):
        chaves.append(estado["chave_prata"])
    return chaves


def avancar_chave():
    for chave in chaves_ativas():
        _avancar_uma_chave(chave)


def _avancar_uma_chave(chave):
    for r in range(len(chave) - 1):
        rodada = chave[r]
        if rodada and all(m["vencedor"] for m in rodada):
            if chave[r + 1]:
                continue
            vencedores = [m["vencedor"] for m in rodada]
            proxima = []
            i = 0
            while i < len(vencedores):
                if i + 1 < len(vencedores):
                    proxima.append({"A": vencedores[i], "B": vencedores[i + 1],
                                    "setsA": 0, "setsB": 0, "vencedor": None, "jogada": False,
                                    "pos": i // 2})
                    i += 2
                else:
                    proxima.append({"A": vencedores[i], "B": None,
                                    "setsA": 0, "setsB": 0,
                                    "vencedor": vencedores[i], "jogada": True,
                                    "pos": i // 2})
                    i += 1
            chave[r + 1] = proxima


def registrar_na_chave(a, b, sA, sB):
    for chave in chaves_ativas():
        for r in range(len(chave)):
            for m in chave[r]:
                if m["vencedor"]:
                    continue
                pares = (m["A"] == a and m["B"] == b) or (m["A"] == b and m["B"] == a)
                if pares:
                    m["setsA"], m["setsB"] = sA, sB
                    m["vencedor"] = a if sA > sB else b
                    m["jogada"] = True
                    avancar_chave()
                    return True
    _log("NAO_ACHOU_CHAVE %s x %s (%dx%d) modo=%s elim=%s" % (
        a, b, sA, sB, estado.get("modo"), em_eliminacao()))
    return False


def pertence_chave(jog):
    for chave in chaves_ativas():
        for r in chave:
            for m in r:
                if m["vencedor"]:
                    continue
                if m["A"] == jog or m["B"] == jog:
                    return True
    return False


# ---------- Dupla eliminacao ----------
# Chave de vencedores (como eliminacao simples) + chave de perdedores.
# Quem perde 1x cai para a chave de perdedores; 2 derrotas eliminam.
# Grande final: campeao de vencedores x campeao de perdedores.
# Se o campeao de perdedores vencer a grande final, ha jogo decisivo.

def _wb_roteado(r):
    rod = estado["dupla"]["vencedores"][r]
    return bool(rod) and all(m.get("_roteado") for m in rod)


def _lb_pronta(i):
    # lb[i] terminou de produzir vencedores: fontes esgotadas E todos os jogos
    # da rodada ja foram processados (recursivo, evita rotulo prematuro)
    rod = estado["dupla"]["perdedores"][i]
    return _lb_fonte_exausta(i) and all(m.get("_roteado") for m in rod["jogos"])


def _lb_fonte_exausta(i):
    # True quando todas as fontes da rodada i dos perdedores ja chegaram
    dupla = estado["dupla"]
    if i == 0:
        return _wb_roteado(0)
    if i % 2 == 1:
        return _lb_pronta(i - 1) and _wb_roteado((i + 1) // 2)
    return _lb_pronta(i - 1)


def montar_dupla(ordem=None):
    if ordem is None:
        ordem = list(estado["jogadores"])
        if estado["distribuicao"] == "sorteio":
            random.shuffle(ordem)
        else:
            ranking = [r for r in estado["ranking"] if r in ordem]
            if ranking:
                resto = [j for j in ordem if j not in ranking]
                ordem = ranking + resto
    vazio = {"vencedores": [], "perdedores": [], "final": None, "final2": None,
             "champ": None, "champ_wb": None, "champ_lb": None}
    if len(ordem) < 2:
        estado["dupla"] = vazio
        salvar()
        return estado["dupla"]
    total = 1
    while total < len(ordem):
        total *= 2
    W = int(math.log2(total))
    slots = [None] * total
    sp = seed_positions(total)
    placed = set()
    for i, p in enumerate(ordem):
        if i < len(sp):
            slots[sp[i]] = p
            placed.add(p)
    rest = [p for p in ordem if p not in placed]
    for i in range(total):
        if slots[i] is None and rest:
            slots[i] = rest.pop(0)
    r0 = []
    for i in range(0, total, 2):
        a, b = slots[i], slots[i + 1]
        if a is None and b is None:
            continue
        m = {"A": a, "B": b, "setsA": 0, "setsB": 0, "vencedor": None, "jogada": False,
             "pos": i // 2}
        if a is None:
            m["A"], m["B"] = b, None
        if m["B"] is None:
            m["vencedor"] = m["A"]
            m["jogada"] = True
        r0.append(m)
    wb = [r0] + [[] for _ in range(max(0, W - 1))]
    lb = []
    for i in range(max(0, 2 * W - 2)):
        lb.append({"tipo": "drop" if i == 0 else ("merge" if i % 2 == 1 else "mini"),
                   "jogos": [], "fila": []})
    estado["dupla"] = {
        "vencedores": wb,
        "perdedores": lb,
        "final": None,
        "final2": None,
        "champ": None,
        "champ_wb": None,
        "champ_lb": None,
    }
    salvar()
    return estado["dupla"]


def dupla_avancar():
    dupla = estado["dupla"]
    if not dupla:
        return
    # 1. montar proximas rodadas da chave de vencedores
    for r in range(len(dupla["vencedores"]) - 1):
        rod = dupla["vencedores"][r]
        if rod and all(m["vencedor"] for m in rod) and not dupla["vencedores"][r + 1]:
            vencedores = [m["vencedor"] for m in rod]
            prox = []
            i = 0
            while i < len(vencedores):
                if i + 1 < len(vencedores):
                    prox.append({"A": vencedores[i], "B": vencedores[i + 1],
                                 "setsA": 0, "setsB": 0, "vencedor": None, "jogada": False,
                                 "pos": i // 2})
                    i += 2
                else:
                    prox.append({"A": vencedores[i], "B": None, "setsA": 0, "setsB": 0,
                                 "vencedor": vencedores[i], "jogada": True,
                                 "pos": i // 2})
                    i += 1
            dupla["vencedores"][r + 1] = prox
    # 2. propagar resultados (estabilizar) ate nao haver mais mudancas
    while True:
        mudou = False
        for r, rod in enumerate(dupla["vencedores"]):
            for m in rod:
                if m["jogada"] and not m.get("_roteado"):
                    m["_roteado"] = True
                    mudou = True
                    if m["A"] and m["B"] and m["vencedor"]:
                        perdedor = m["B"] if m["vencedor"] == m["A"] else m["A"]
                        if dupla["perdedores"]:
                            if r == 0:
                                dupla["perdedores"][0]["fila"].append(perdedor)
                            elif 2 * r - 1 < len(dupla["perdedores"]):
                                dupla["perdedores"][2 * r - 1]["fila"].append(perdedor)
        for i, rod in enumerate(dupla["perdedores"]):
            for m in rod["jogos"]:
                if m["jogada"] and not m.get("_roteado"):
                    m["_roteado"] = True
                    mudou = True
                    if m["vencedor"]:
                        if i + 1 < len(dupla["perdedores"]):
                            dupla["perdedores"][i + 1]["fila"].append(m["vencedor"])
                        else:
                            dupla["champ_lb"] = m["vencedor"]
        for rod in dupla["perdedores"]:
            while len(rod["fila"]) >= 2:
                a = rod["fila"].pop(0)
                b = rod["fila"].pop(0)
                rod["jogos"].append({"A": a, "B": b, "setsA": 0, "setsB": 0,
                                     "vencedor": None, "jogada": False,
                                     "pos": len(rod["jogos"])})
                mudou = True
        for i, rod in enumerate(dupla["perdedores"]):
            if len(rod["fila"]) == 1 and _lb_fonte_exausta(i):
                p = rod["fila"].pop(0)
                if i + 1 < len(dupla["perdedores"]):
                    dupla["perdedores"][i + 1]["fila"].append(p)
                else:
                    dupla["champ_lb"] = p
                mudou = True
        if not mudou:
            break
    # 3. campeoes e grande final
    wb = dupla["vencedores"]
    lb = dupla["perdedores"]
    if wb and wb[-1] and all(m["vencedor"] for m in wb[-1]) and len(wb[-1]) == 1:
        dupla["champ_wb"] = wb[-1][0]["vencedor"]
    if not lb and dupla.get("champ_wb") and not dupla.get("champ"):
        dupla["champ"] = dupla["champ_wb"]
    if dupla.get("champ_wb") and dupla.get("champ_lb") and dupla.get("final") is None:
        dupla["final"] = {"A": dupla["champ_wb"], "B": dupla["champ_lb"],
                          "setsA": 0, "setsB": 0, "vencedor": None, "jogada": False}
    f = dupla.get("final")
    if f and f.get("vencedor"):
        if f["vencedor"] == dupla.get("champ_wb"):
            dupla["champ"] = dupla["champ_wb"]
        elif dupla.get("final2") is None:
            dupla["final2"] = {"A": dupla["champ_wb"], "B": dupla["champ_lb"],
                               "setsA": 0, "setsB": 0, "vencedor": None, "jogada": False}
    f2 = dupla.get("final2")
    if f2 and f2.get("vencedor"):
        dupla["champ"] = f2["vencedor"]


def registrar_na_dupla(a, b, sA, sB):
    dupla = estado["dupla"]
    if not dupla:
        return False
    candidatos = []
    for rod in dupla["vencedores"]:
        candidatos.extend(rod)
    for rod in dupla["perdedores"]:
        candidatos.extend(rod["jogos"])
    if dupla.get("final"):
        candidatos.append(dupla["final"])
    if dupla.get("final2"):
        candidatos.append(dupla["final2"])
    for m in candidatos:
        if m.get("vencedor"):
            continue
        if (m["A"] == a and m["B"] == b) or (m["A"] == b and m["B"] == a):
            m["setsA"], m["setsB"] = sA, sB
            m["vencedor"] = a if sA > sB else b
            m["jogada"] = True
            dupla_avancar()
            return True
    return False


def dupla_pendentes():
    pend = []
    seen = set()
    dupla = estado.get("dupla") or {}
    for rod in dupla.get("vencedores", []):
        for m in rod:
            if not m.get("vencedor") and m.get("A") and m.get("B"):
                for p in (m["A"], m["B"]):
                    if p not in seen:
                        seen.add(p)
                        pend.append(p)
    for rod in dupla.get("perdedores", []):
        for m in rod.get("jogos", []):
            if not m.get("vencedor") and m.get("A") and m.get("B"):
                for p in (m["A"], m["B"]):
                    if p not in seen:
                        seen.add(p)
                        pend.append(p)
    for k in ("final", "final2"):
        m = dupla.get(k)
        if m and not m.get("vencedor") and m.get("A") and m.get("B"):
            for p in (m["A"], m["B"]):
                if p not in seen:
                    seen.add(p)
                    pend.append(p)
    return pend


def em_eliminacao():
    return estado["modo"] in ("grupos", "misto") and bool(estado["chave"])


def eh_partida_final(a, b):
    """True se o jogo a x b e a grande final / ultimo jogo que decide o campeao."""
    par = par_chave(a, b)
    if estado["modo"] == "absoluto":
        dupla = estado.get("dupla") or {}
        for k in ("final", "final2"):
            m = dupla.get(k)
            if m and m.get("A") and m.get("B") and par_chave(m["A"], m["B"]) == par:
                return True
        return False
    if estado["modo"] in ("chave", "grupos", "misto"):
        for chave in chaves_ativas():
            if not chave:
                continue
            ultima = chave[-1] or []
            for m in ultima:
                if not m.get("vencedor") and m.get("A") and m.get("B") and par_chave(m["A"], m["B"]) == par:
                    return True
        return False
    return False


def formato_da_partida(a, b):
    """Formato de sets aplicavel a partida a x b conforme a fase.
    Sem opcao 'livre': todo formato e um valor 1/3/5/7 (melhor de N)."""
    if eh_partida_final(a, b):
        f = int(estado.get("formato_final") or 3)
    elif estado["modo"] in ("chave", "absoluto") or em_eliminacao():
        f = int(estado.get("formato_mata") or 3)
    else:
        f = int(estado.get("formato_grupos") or 3)
    if f not in (1, 3, 5, 7):
        f = 3
    return f


def pares_pendentes():
    pares = []
    if estado["modo"] == "chave" or em_eliminacao():
        for chave in chaves_ativas():
            for rod in chave:
                for m in rod:
                    if not m.get("vencedor") and m.get("A") and m.get("B"):
                        pares.append([m["A"], m["B"]])
    elif estado["modo"] == "absoluto":
        dupla = estado.get("dupla") or {}
        for rod in dupla.get("vencedores", []):
            for m in rod:
                if not m.get("vencedor") and m.get("A") and m.get("B"):
                    pares.append([m["A"], m["B"]])
        for rod in dupla.get("perdedores", []):
            for m in rod.get("jogos", []):
                if not m.get("vencedor") and m.get("A") and m.get("B"):
                    pares.append([m["A"], m["B"]])
        for k in ("final", "final2"):
            m = dupla.get(k)
            if m and not m.get("vencedor") and m.get("A") and m.get("B"):
                pares.append([m["A"], m["B"]])
    elif estado["modo"] in ("grupos", "misto"):
        jogadas = {par_chave(m["A"], m["B"]) for m in estado["partidas"]}
        for g in estado.get("grupos", []):
            js = g["jogadores"]
            for i in range(len(js)):
                for j in range(i + 1, len(js)):
                    if par_chave(js[i], js[j]) not in jogadas:
                        pares.append([js[i], js[j]])
    return pares


def grupos_completos():
    for g in estado.get("grupos", []):
        jog = g["jogadores"]
        jogadas = {par_chave(m["A"], m["B"]) for m in estado["partidas"]
                   if m["A"] in jog and m["B"] in jog}
        for i in range(len(jog)):
            for j in range(i + 1, len(jog)):
                if par_chave(jog[i], jog[j]) not in jogadas:
                    return False
    return True


def grupo_do_par(a, b):
    if estado["modo"] in ("grupos", "misto"):
        for idx, g in enumerate(estado.get("grupos", [])):
            if a in g["jogadores"] and b in g["jogadores"]:
                return idx
        return -1
    return 0


def pares_por_grupo():
    mp = {}
    for a, b in pares_pendentes():
        mp.setdefault(grupo_do_par(a, b), []).append((a, b))
    return mp


def so_mesa_tem(a, b, mesa):
    """True se o par esta em andamento em outra mesa (diferente de `mesa`)."""
    pk = par_chave(a, b)
    for m, x in estado["mesas"].items():
        if m != mesa and par_chave(x.get("A", ""), x.get("B", "")) == pk:
            return True
    return False


def proxima_partida(mesa):
    if not estado.get("iniciado"):
        estado["em_andamento"] = None
        return None
    cand = pares_pendentes()
    if not cand:
        estado["em_andamento"] = None
        return None
    estado.setdefault("mesas", {})

    ativo = estado["mesas"].get(mesa)
    if ativo and ativo.get("A") and any(
            par_chave(a, b) == par_chave(ativo["A"], ativo["B"]) for a, b in cand):
        estado["em_andamento"] = {"A": ativo["A"], "B": ativo["B"], "mesa": mesa,
                                  "desde": datetime.datetime.now().strftime("%H:%M")}
        return ativo["A"], ativo["B"]
    if ativo:
        estado["mesas"].pop(mesa, None)

    # Jogadores ocupados em OUTRAS mesas (jogo ainda nao registrado) nao podem ser chamados.
    ocupados = set()
    grupos_em_andamento = set()
    for m, g_jogo in list(estado["mesas"].items()):
        if m == mesa or not g_jogo.get("A"):
            continue
        ocupados.add(g_jogo["A"])
        ocupados.add(g_jogo["B"])
        if g_jogo.get("grupo") is not None:
            grupos_em_andamento.add(g_jogo["grupo"])

    gp = pares_por_grupo()

    # Modo "manual": nenhuma mesa recebe chamada automatica, todas ficam
    # ociosas aguardando o administrador chamar pelo painel.
    if estado.get("modo_chamada") == "manual":
        return None

    # Modo "grupos": cada mesa so recebe jogos do grupo que o administrador
    # atribuiu (estado["mesa_grupo"][mesa]). Mesa sem grupo nao recebe chamada
    # automatica (fica ociosa para chamada manual).
    if estado.get("modo_chamada") == "grupos":
        alvo = estado.get("mesa_grupo", {}).get(mesa)
        if alvo is None or alvo not in gp or not gp[alvo]:
            return None
        for a, b in gp[alvo]:
            if a in ocupados or b in ocupados:
                continue
            if not so_mesa_tem(a, b, mesa):
                estado["mesas"][mesa] = {"A": a, "B": b, "grupo": alvo}
                estado["em_andamento"] = {"A": a, "B": b, "mesa": mesa,
                                          "desde": datetime.datetime.now().strftime("%H:%M")}
                salvar()
                return a, b
        return None

    def grupo_tem_livre(g):
        return any(not so_mesa_tem(a, b, mesa) and a not in ocupados and b not in ocupados
                   for a, b in gp[g])

    # Prioridade: grupos SEM jogo em andamento em outra mesa, pra cada mesa pegar um grupo diferente.
    grupos_ok = [g for g in gp if g not in grupos_em_andamento and grupo_tem_livre(g)]
    if not grupos_ok:
        grupos_ok = [g for g in gp if grupo_tem_livre(g)]
    if not grupos_ok:
        return None

    # Rodízio: escolhe o grupo com MENOS jogos ja registrados (intercalando um
    # jogo de cada grupo por rodada), para que nenhum grupo termine tudo no inicio
    # mesmo com grupos de tamanhos diferentes.
    def reg_no(g):
        if estado["modo"] in ("grupos", "misto") and 0 <= g < len(estado["grupos"]):
            jog = set(estado["grupos"][g]["jogadores"])
            return sum(1 for m in estado["partidas"]
                       if m.get("A") in jog and m.get("B") in jog)
        return sum(1 for m in estado["partidas"]
                   if grupo_do_par(m.get("A", ""), m.get("B", "")) == g)
    g = min(grupos_ok, key=lambda gg: (reg_no(gg), gg))

    designada = limpar_nome(estado.get("mesa_final") or "")
    final_pk = final_pendente()
    for a, b in gp[g]:
        if final_pk and par_chave(a, b) == final_pk:
            # Final pode ser chamada em qualquer mesa livre no modo AUTO
            # (mesa_final vazio). So fica restrita quando houver uma mesa
            # escolhida explicitamente.
            if designada and designada != mesa:
                continue
        if a in ocupados or b in ocupados:
            continue
        if not so_mesa_tem(a, b, mesa):
            estado["mesas"][mesa] = {"A": a, "B": b, "grupo": g}
            estado["em_andamento"] = {"A": a, "B": b, "mesa": mesa,
                                      "desde": datetime.datetime.now().strftime("%H:%M")}
            salvar()
            return a, b
    return None


def pares_por_jogador():
    mapa = {}
    for a, b in pares_pendentes():
        mapa.setdefault(a, set()).add(b)
        mapa.setdefault(b, set()).add(a)
    return mapa


# ---------- Registro de partida ----------

def parse_sets(texto):
    # texto: "11x9, 11x8, 9x11, 11x6" -> [[11,9],[11,8],[9,11],[11,6]]
    sets = []
    for parte in texto.replace(";", ",").split(","):
        parte = parte.strip().lower()
        if not parte:
            continue
        p = parte.replace("×", " ").replace("x", " ").replace("-", " ").replace(":", " ").split()
        if len(p) != 2:
            return None, "Set invalido: '%s'." % parte
        try:
            pa, pb = int(p[0]), int(p[1])
        except ValueError:
            return None, "Set invalido: '%s'." % parte
        sets.append([pa, pb])
    if not sets:
        return None, "Informe o placar dos sets (ex: 11x9, 11x8)."
    return sets, None


def _set_unico_ok(pa, pb):
    # placar individual valido: vencedor claro, 11+ pontos e diferenca de 2
    if pa == pb or pa < 0 or pb < 0:
        return False
    if pa > 99 or pb > 99:
        return False
    hi, lo = max(pa, pb), min(pa, pb)
    return hi >= 11 and (lo <= 9 or lo == hi - 2)


def _sets_ok(sets):
    # cada set precisa de vencedor (pontos diferentes) e nao pode haver empate em sets
    if not sets:
        return False
    a = b = 0
    for pa, pb in sets:
        if not _set_unico_ok(pa, pb):
            return False
        if pa > pb:
            a += 1
        else:
            b += 1
    return a != b


def eh_jogo_grupos(a, b):
    """True se o par a x b pertence a fase de grupos (mesmo grupo), mesmo apos a chave ter sido gerada.
    Uma partida PENDENTE da chave de eliminacao nunca e jogo de grupos: mesmo quando os dois
    jogadores vieram do mesmo grupo (ex.: a grande final entre o 1o e o 2o do mesmo grupo),
    o resultado vale pela chave e usa o formato da fase (mata/final), nao o formato dos grupos."""
    if estado["modo"] not in ("grupos", "misto"):
        return False
    if em_eliminacao():
        for chave in chaves_ativas():
            for rod in chave:
                for m in rod:
                    if m.get("vencedor"):
                        continue
                    if m.get("A") and m.get("B") and par_chave(m["A"], m["B"]) == par_chave(a, b):
                        return False
    ga = grupo_de(a)
    gb = grupo_de(b)
    return ga is not None and ga is gb


def registrar_partida(a, b, sA, sB, sets=None, mesa=""):
    global NO_MATCH_WARNING
    a, b = limpar_nome(a), limpar_nome(b)
    eh_grupo = eh_jogo_grupos(a, b)
    fmt = estado["formato_grupos"] if eh_grupo else formato_da_partida(a, b)
    if sets:
        sA = int(sA) if str(sA).isdigit() else 0
        sB = int(sB) if str(sB).isdigit() else 0
        if _sets_ok(sets):
            sA = sum(1 for pa, pb in sets if pa > pb)
            sB = len(sets) - sA
        elif all(_set_unico_ok(pa, pb) for pa, pb in sets) and sA != sB and sA + sB == len(sets):
            # Historico de sets inconsistente (ex.: troca de lado no meio da
            # partida). Os contadores oficiais da placa definem o resultado.
            _log("TRUST counters %s x %s (%dx%d) sets=%r" % (a, b, sA, sB, sets))
            sets = None
        else:
            _log("REJEITA_sets %s x %s sets=%r (counters %d,%d)" % (a, b, sets, sA, sB))
            return False, "Partida invalida: cada set deve terminar com 11+ pontos e 2 de diferenca (ex.: 11x9, 12x10)."
    else:
        sA, sB = int(sA), int(sB)
    if fmt > 0 and max(sA, sB) != (fmt + 1) // 2:
        _log("REJEITA_fmt %s x %s (%d,%d) fmt=%d" % (a, b, sA, sB, fmt))
        return False, "Partida invalida: vencedor precisa de %d sets neste formato." % ((fmt + 1) // 2)
    if a == b or sA == sB:
        return False, "Partida invalida: mesmo jogador ou empate."
    par = par_chave(a, b)
    if estado["modo"] in ("grupos", "misto") and (eh_grupo or not em_eliminacao()):
        for m in estado["partidas"]:
            if par_chave(m["A"], m["B"]) == par:
                return False, "Partida entre %s e %s ja registrada." % (a, b)
    elif estado["modo"] in ("chave", "absoluto") or em_eliminacao():
        pend = {par_chave(x, y) for x, y in pares_pendentes()}
        if par not in pend:
            for m in estado["partidas"]:
                if par_chave(m["A"], m["B"]) == par:
                    return False, "Partida entre %s e %s ja registrada." % (a, b)
    if estado["modo"] in ("grupos", "misto") and (eh_grupo or not em_eliminacao()):
        ga = grupo_de(a)
        gb = grupo_de(b)
        if ga is None or gb is None or ga is not gb:
            _log("REJEITA_regrupo %s x %s (%dx%d)" % (a, b, sA, sB))
            return False, "Partida invalida: %s e %s devem estar no mesmo grupo." % (a, b)
    if (estado["modo"] in ("chave", "absoluto") or em_eliminacao()) and not eh_grupo:
        if b not in pares_por_jogador().get(a, set()):
            _log("REJEITA_chave %s x %s (%dx%d) modo=%s elim=%s mapa=%r" % (
                a, b, sA, sB, estado.get("modo"), em_eliminacao(),
                {k: sorted(v) for k, v in pares_por_jogador().items()}))
            return False, "Partida %s x %s nao existe na chave atual." % (a, b)
    partida = {"A": a, "B": b, "setsA": sA, "setsB": sB,
               "sets": sets or [],
               "vencedor": a if sA > sB else b,
               "formato": ("melhor de %d" % fmt),
               "data": datetime.datetime.now().strftime("%d/%m %H:%M")}
    estado["partidas"].append(partida)
    em = estado.get("em_andamento")
    if em and par_chave(em.get("A", ""), em.get("B", "")) == par_chave(a, b) and (not mesa or em.get("mesa") == mesa):
        estado["em_andamento"] = None
    mesas_afetadas = []
    for m in list(estado.get("mesas", {}).keys()):
        x = estado["mesas"][m]
        if par_chave(x.get("A", ""), x.get("B", "")) == par_chave(a, b):
            mesas_afetadas.append(m)
            estado["mesas"].pop(m, None)

    if (estado["modo"] == "chave" or em_eliminacao()) and not eh_grupo:
        if not registrar_na_chave(a, b, sA, sB):
            NO_MATCH_WARNING = "Resultado de %s x %s nao pertence a chave atual." % (a, b)
    elif estado["modo"] == "absoluto" and not eh_grupo:
        if not registrar_na_dupla(a, b, sA, sB):
            NO_MATCH_WARNING = "Resultado de %s x %s nao pertence a chave da dupla eliminacao." % (a, b)
    if estado["modo"] in ("grupos", "misto") and not em_eliminacao() and grupos_completos():
        estado["chave"] = gerar_chave_grupos()
        print("Grupos completos - fase eliminatória gerada automaticamente.", flush=True)
    salvar()
    notificar_campeao()
    if not mesa and estado.get("iniciado"):
        # Registro manual (web, sem mesa): a placa que exibia o jogo registrado
        # ainda acha que ele esta em andamento. Avancar para a proxima partida.
        for m in mesas_afetadas:
            _avancar_mesa(m)
    return True, ""


def _jogo_de_grupos(m):
    """True se a partida registrada pertence à fase de grupos (ambos no mesmo grupo)."""
    if estado.get("modo") not in ("grupos", "misto"):
        return False
    ga = grupo_de(m.get("A", ""))
    gb = grupo_de(m.get("B", ""))
    return ga is not None and ga is gb


def _reconstruir_chaves():
    """Reconstrói as chaves do zero conforme o modo (ouro/prata ou dupla)."""
    if estado.get("modo") == "chave":
        estado["chave"] = gerar_chave()
    elif estado.get("modo") == "absoluto":
        montar_dupla()
    else:
        estado["chave"] = gerar_chave_grupos()


def _reaplicar_eliminatoria():
    """Reaplica nas chaves as eliminatórias já registradas (ainda pendentes)."""
    modo = estado.get("modo")
    for m in list(estado.get("partidas", [])):
        if _jogo_de_grupos(m):
            continue
        a, b, sA, sB = m["A"], m["B"], m["setsA"], m["setsB"]
        if modo == "absoluto":
            registrar_na_dupla(a, b, sA, sB)
        elif modo in ("chave", "grupos", "misto"):
            registrar_na_chave(a, b, sA, sB)


def remover_partida(a, b, data=""):
    """Remove um registro de partida e rederiva a fase (grupos/chave/dupla)."""
    a, b = limpar_nome(a), limpar_nome(b)
    par = par_chave(a, b)
    idx = -1
    for i, m in enumerate(estado.get("partidas", [])):
        if par_chave(m["A"], m["B"]) == par:
            if data and m.get("data") != data:
                continue
            idx = i
            break
    if idx < 0:
        return False, "Registro de %s x %s nao encontrado." % (a, b)
    estado["partidas"].pop(idx)
    # Se já havia eliminatória, reconstrói a fase e reaplica as já registradas
    if estado.get("modo") in ("chave", "absoluto") or em_eliminacao():
        _reconstruir_chaves()
        _reaplicar_eliminatoria()
    elif estado.get("modo") in ("grupos", "misto") and grupos_completos():
        estado["chave"] = gerar_chave_grupos()
    salvar()
    return True, ""


def editar_resultado(a, b, sA, sB, sets=None, data=""):
    """Substitui o resultado de uma partida já registrada (mesmos jogadores)."""
    global estado
    snapshot = copy.deepcopy(estado)
    ok, err = remover_partida(a, b, data=data)
    if not ok:
        return False, err
    ok, err = registrar_partida(a, b, sA, sB, sets=sets)
    if not ok:
        estado = snapshot
        salvar()
        return False, err
    return True, ""


# ---------- Rotas ----------

@app.route("/api/partida", methods=["POST"])
def api_partida():
    d = request.get_json(silent=True) or request.form.to_dict()
    a = d.get("jogadorA") or d.get("A")
    b = d.get("jogadorB") or d.get("B")
    sA = d.get("setsA")
    sB = d.get("setsB")
    sets = d.get("sets")
    if isinstance(sets, str):
        sets, err = parse_sets(sets)
        if err:
            return jsonify({"ok": False, "erro": err}), 400
    if not (a and b and (sets is not None or (sA is not None and sB is not None))):
        return jsonify({"ok": False, "erro": "Faltam campos."}), 400
    mesa_aut = limpar_nome(d.get("mesa", ""))
    pk = par_chave(a, b)
    for m, x in estado.get("mesas", {}).items():
        if par_chave(x.get("A", ""), x.get("B", "")) == pk:
            if mesa_aut and mesa_aut == m:
                break
            if mesa_aut and mesa_aut != m:
                return jsonify({"ok": False,
                                "erro": "Partida %s x %s esta em andamento na mesa %s." % (a, b, m)}), 400
    ok, err = registrar_partida(a, b, sA, sB, sets=sets, mesa=mesa_aut)
    if not ok:
        return jsonify({"ok": False, "erro": err}), 400
    return jsonify({"ok": True})


@app.route("/api/partida/editar", methods=["POST"])
def api_editar_partida():
    d = request.get_json(silent=True) or request.form.to_dict()
    a = d.get("jogadorA") or d.get("A")
    b = d.get("jogadorB") or d.get("B")
    data = d.get("data", "")
    sA = d.get("setsA")
    sB = d.get("setsB")
    sets = d.get("sets")
    if isinstance(sets, str):
        sets, err = parse_sets(sets)
        if err:
            return jsonify({"ok": False, "erro": err}), 400
    if not (a and b and (sets is not None or (sA is not None and sB is not None))):
        return jsonify({"ok": False, "erro": "Faltam campos."}), 400
    ok, err = editar_resultado(a, b, sA, sB, sets=sets, data=data)
    if not ok:
        return jsonify({"ok": False, "erro": err}), 400
    return jsonify({"ok": True})


@app.route("/api/partida/desfazer", methods=["POST"])
def api_desfazer_partida():
    d = request.get_json(silent=True) or request.form.to_dict()
    a = d.get("jogadorA") or d.get("A")
    b = d.get("jogadorB") or d.get("B")
    data = d.get("data", "")
    if not (a and b):
        return jsonify({"ok": False, "erro": "Faltam jogadores."}), 400
    ok, err = remover_partida(a, b, data=data)
    if not ok:
        return jsonify({"ok": False, "erro": err}), 400
    return jsonify({"ok": True})


@app.route("/api/jogadores", methods=["POST"])
def api_jogadores():
    b = bloquear_se_iniciado()
    if b: return b
    d = request.get_json(silent=True) or request.form.to_dict()
    import re as _re
    jog = [limpar_nome(x) for x in _re.split(r"[\n,;\t]+", d.get("jogadores", "")) if limpar_nome(x)]
    estado["cabecas"] = []
    estado["jogadores"] = jog
    if "ranking" in d:
        rk = [limpar_nome(x) for x in _re.split(r"[\n,;\t]+", d.get("ranking") or "") if limpar_nome(x)]
        estado["ranking"] = rk
    estado["partidas"] = []
    estado["chave"] = []
    estado["chave_prata"] = []
    estado["grupos"] = []
    estado["dupla"] = None
    estado["em_andamento"] = None
    estado["iniciado"] = False
    salvar()
    return jsonify({"ok": True, "jogadores": jog})


@app.route("/api/incluir", methods=["POST"])
def api_incluir():
    b = bloquear_se_iniciado()
    if b: return b
    import re as _re
    d = request.get_json(silent=True) or request.form.to_dict()
    nome = limpar_nome(d.get("jogador", ""))
    if not nome:
        return jsonify({"ok": False, "erro": "Informe o nome do jogador."}), 400
    if nome in estado["jogadores"]:
        return jsonify({"ok": False, "erro": "Jogador ja cadastrado."}), 400
    estado["jogadores"].append(nome)
    estado["partidas"] = []
    estado["chave"] = []
    estado["chave_prata"] = []
    estado["grupos"] = []
    estado["dupla"] = None
    estado["em_andamento"] = None
    salvar()
    return jsonify({"ok": True, "jogadores": estado["jogadores"]})


@app.route("/api/modo", methods=["POST"])
def api_modo():
    b = bloquear_se_iniciado()
    if b: return b
    global NO_MATCH_WARNING
    d = request.get_json(silent=True) or request.form.to_dict()
    modo = d.get("modo", "absoluto")
    if modo not in ("absoluto", "grupos", "chave", "misto"):
        return jsonify({"ok": False, "erro": "Modo invalido."}), 400
    if modo == estado["modo"]:
        return jsonify({"ok": True})
    estado["modo"] = modo
    estado["partidas"] = []
    estado["chave"] = []
    estado["chave_prata"] = []
    estado["grupos"] = []
    estado["dupla"] = None
    NO_MATCH_WARNING = ""
    if modo == "chave":
        estado["chave"] = gerar_chave()
    elif modo == "absoluto":
        montar_dupla()
    salvar()
    return jsonify({"ok": True})


@app.route("/api/grupos", methods=["POST"])
def api_grupos():
    b = bloquear_se_iniciado()
    if b: return b
    if not estado.get("jogadores"):
        return jsonify({"ok": False, "erro": "Cadastre os jogadores primeiro."}), 400
    d = request.get_json(silent=True) or request.form.to_dict()
    try:
        num = int(d.get("numero", 0))
    except (TypeError, ValueError):
        num = 0
    if num < 2:
        return jsonify({"ok": False, "erro": "Numero de grupos invalido."}), 400
    dist = d.get("distribuicao")
    if dist in ("serpentina", "sorteio"):
        estado["distribuicao"] = dist
    if "ranking" in d and d["ranking"]:
        ranking = [limpar_nome(x) for x in d["ranking"].split(",") if limpar_nome(x)]
        if ranking:
            estado["ranking"] = ranking
    criar_grupos(num)
    return jsonify({"ok": True})


@app.route("/api/mover", methods=["POST"])
def api_mover():
    b = bloquear_se_iniciado()
    if b: return b
    d = request.get_json(silent=True) or request.form.to_dict()
    jog = limpar_nome(d.get("jogador", ""))
    try:
        novo = int(d.get("grupo", -1))
    except (TypeError, ValueError):
        novo = -1
    if not jog:
        return jsonify({"ok": False, "erro": "Faltam campos."}), 400
    mover_jogador(jog, novo)
    return jsonify({"ok": True})


@app.route("/api/reordenar", methods=["POST"])
def api_reordenar():
    b = bloquear_se_iniciado()
    if b: return b
    d = request.get_json(silent=True) or request.form.to_dict()
    jog = limpar_nome(d.get("jogador", ""))
    try:
        grupo = int(d.get("grupo", -1))
        pos = int(d.get("pos", 0))
    except (TypeError, ValueError):
        grupo, pos = -1, 0
    if not jog:
        return jsonify({"ok": False, "erro": "Faltam campos."}), 400
    reordenar_jogador(jog, grupo, pos)
    return jsonify({"ok": True})


@app.route("/api/distribuicao", methods=["POST"])
def api_distribuicao():
    b = bloquear_se_iniciado()
    if b: return b
    d = request.get_json(silent=True) or request.form.to_dict()
    dist = d.get("distribuicao")
    if dist not in ("serpentina", "sorteio"):
        return jsonify({"ok": False, "erro": "Distribuicao invalida."}), 400
    estado["distribuicao"] = dist
    salvar()
    return jsonify({"ok": True})


@app.route("/api/chamada", methods=["POST"])
def api_modo_chamada():
    b = bloquear_se_iniciado()
    if b: return b
    d = request.get_json(silent=True) or request.form.to_dict()
    modo = d.get("modo", "auto")
    if modo not in ("auto", "grupos", "manual"):
        return jsonify({"ok": False, "erro": "Modo de chamada invalido."}), 400
    estado["modo_chamada"] = modo
    mg = d.get("mesa_grupo")
    if isinstance(mg, dict):
        estado["mesa_grupo"] = {limpar_nome(k): v for k, v in mg.items()}
    else:
        estado["mesa_grupo"] = {}
    salvar()
    return jsonify({"ok": True})


@app.route("/api/chamar", methods=["POST"])
def api_chamar_manual():
    d = request.get_json(silent=True) or request.form.to_dict()
    mesa = limpar_nome(d.get("mesa", ""))
    a = limpar_nome(d.get("A", ""))
    b = limpar_nome(d.get("B", ""))
    if not mesa or not a or not b:
        return jsonify({"ok": False, "erro": "Informe mesa e confronto."}), 400
    if not estado.get("iniciado"):
        return jsonify({"ok": False, "erro": "Inicie o campeonato primeiro."}), 400
    if a == b:
        return jsonify({"ok": False, "erro": "Confronto invalido."}), 400
    info = estado.get("baterias", {}).get(mesa) or {}
    ip = info.get("ip") or ""
    if not ip:
        return jsonify({"ok": False, "erro": "Mesa %s esta offline." % mesa}), 400
    if estado.get("mesas", {}).get(mesa):
        return jsonify({"ok": False, "erro": "Mesa %s ja tem partida em andamento." % mesa}), 400
    cand = pares_pendentes()
    if par_chave(a, b) not in {par_chave(x, y) for x, y in cand}:
        return jsonify({"ok": False, "erro": "Confronto %s x %s nao esta pendente." % (a, b)}), 400
    for m, g_jogo in list(estado.get("mesas", {}).items()):
        if m != mesa and par_chave(g_jogo.get("A", ""), g_jogo.get("B", "")) == par_chave(a, b):
            return jsonify({"ok": False, "erro": "Confronto %s x %s ja esta em outra mesa." % (a, b)}), 400
        if g_jogo.get("A") in (a, b) or g_jogo.get("B") in (a, b):
            return jsonify({"ok": False, "erro": "%s ja esta jogando na mesa %s." % (a if a in (g_jogo.get("A"), g_jogo.get("B")) else b, m)}), 400
    if not enviar_chamada(ip, a, b, mesa):
        return jsonify({"ok": False, "erro": "Placa da mesa %s nao aceitou a chamada." % mesa}), 400
    g = grupo_do_par(a, b)
    estado.setdefault("mesas", {})[mesa] = {"A": a, "B": b, "grupo": g}
    estado["em_andamento"] = {"A": a, "B": b, "mesa": mesa,
                              "desde": datetime.datetime.now().strftime("%H:%M")}
    salvar()
    print("Chamada manual para %s: %s x %s" % (mesa, a, b), flush=True)
    return jsonify({"ok": True})


@app.route("/api/cancelar_chamada", methods=["POST"])
def api_cancelar_chamada():
    d = request.get_json(silent=True) or request.form.to_dict()
    mesa = limpar_nome(d.get("mesa", ""))
    if not mesa:
        return jsonify({"ok": False, "erro": "Informe a mesa."}), 400
    cancelar_chamado(mesa)
    salvar()
    return jsonify({"ok": True})


@app.route("/api/avancar", methods=["POST"])
def api_avancar():
    b = bloquear_se_iniciado()
    if b: return b
    d = request.get_json(silent=True) or request.form.to_dict()
    try:
        n = int(d.get("avancar", 2))
    except (TypeError, ValueError):
        n = 2
    if n < 1:
        n = 1
    estado["avancar"] = n
    salvar()
    return jsonify({"ok": True})


@app.route("/api/gerar_chave", methods=["POST"])
def api_gerar_chave():
    if estado["modo"] in ("grupos", "misto"):
        if estado.get("chave"):
            return jsonify({"ok": False, "erro": "A fase eliminatória já foi gerada. Use CANCELAR CAMPEONATO para recomeçar."}), 400
        if not grupos_completos():
            return jsonify({"ok": False, "erro": "Ainda há jogos de grupo pendentes."}), 400
        estado["chave"] = gerar_chave_grupos()
    elif estado["modo"] == "absoluto":
        b = bloquear_se_iniciado()
        if b: return b
        montar_dupla(ordem=qualificados_dos_grupos() if estado["grupos"] else None)
    else:
        b = bloquear_se_iniciado()
        if b: return b
        estado["chave"] = gerar_chave_grupos() if estado["grupos"] else gerar_chave()
    salvar()
    return jsonify({"ok": True})


@app.route("/api/zerar", methods=["POST"])
def api_zerar():
    global NO_MATCH_WARNING
    estado["jogadores"] = []
    estado["ranking"] = []
    estado["cabecas"] = []
    estado["partidas"] = []
    estado["chave"] = []
    estado["chave_prata"] = []
    estado["grupos"] = []
    estado["dupla"] = None
    estado["mesas"] = {}
    estado["em_andamento"] = None
    estado["iniciado"] = False
    NO_MATCH_WARNING = ""
    comandar_placas("avulso")
    for ip in list(_rede):
        try:
            urllib.request.urlopen("http://%s/api/limpar_campeao" % ip, data=b"", method="POST", timeout=2)
        except Exception:
            pass
    salvar()
    return jsonify({"ok": True})


@app.route("/api/bateria", methods=["POST"])
def api_bateria():
    d = request.get_json(silent=True) or request.form.to_dict()
    mesa = limpar_nome(d.get("mesa") or "?")
    try:
        b = int(d.get("b", -1))
    except (TypeError, ValueError):
        b = -1
    v = d.get("v") or ""
    agora = datetime.datetime.now()
    with _bateria_lock:
        estado["baterias"][mesa] = {
            "b": b,
            "v": v,
            "hora": agora.strftime("%H:%M"),
            "ts": agora.timestamp(),
            "ip": d.get("ip", ""),
        }
    salvar()
    return jsonify({"ok": True})


@app.route("/api/mes_ip", methods=["GET"])
def api_mes_ip():
    mesa = limpar_nome(request.args.get("mesa", ""))
    info = estado.get("baterias", {}).get(mesa)
    if not info or not info.get("ip"):
        return jsonify({"ip": "", "ok": False})
    return jsonify({"ip": info["ip"], "ok": True})


@app.route("/api/proxima_partida", methods=["GET"])
def api_proxima_partida():
    mesa = limpar_nome(request.args.get("mesa", ""))
    r = proxima_partida(mesa)
    if not r:
        return jsonify({"A": "", "B": ""})
    return jsonify({"A": r[0], "B": r[1]})


@app.route("/api/mesa_final", methods=["POST"])
def api_set_mesa_final():
    d = request.get_json(silent=True) or request.form.to_dict()
    nova = limpar_nome(d.get("mesa") or "")
    estado["mesa_final"] = nova or None

    fpk = final_pendente() if nova else None
    if fpk:
        # Se a final ja esta atribuida a outra mesa, cancela-la para a
        # varredura chamar na mesa escolhida.
        for m, g in list(estado.get("mesas", {}).items()):
            if par_chave(g.get("A", ""), g.get("B", "")) == fpk and m != nova:
                print("Mesa final trocada: cancelando %s" % m, flush=True)
                cancelar_chamado(m)
                break
        salvar()
    else:
        salvar()
    return jsonify({"ok": True, "mesa_final": estado["mesa_final"]})


@app.route("/api/renomear", methods=["POST"])
def api_renomear_mesa():
    d = request.get_json(silent=True) or request.form.to_dict()
    mesa = limpar_nome(d.get("mesa") or "")
    nome = (d.get("nome") or "").strip()
    if not mesa or not nome:
        return jsonify({"ok": False, "erro": "faltou_mesa_ou_nome"})
    info = estado.get("baterias", {}).get(mesa) or {}
    ip = info.get("ip") or ""
    if not ip:
        return jsonify({"ok": False, "erro": "mesa_nao_online"})
    try:
        u = "http://%s/api/nome?nome=%s" % (ip, urllib.parse.quote(nome))
        with urllib.request.urlopen(urllib.request.Request(u, data=b"", method="POST"), timeout=6) as r:
            corpo = r.read().decode("utf-8", "ignore")
        ok = '"ok":true' in corpo or '"ok": true' in corpo
        return jsonify({"ok": ok, "mesa": nome, "ip": ip})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)})


@app.route("/api/estado", methods=["GET"])
def api_estado():
    agora = datetime.datetime.now().timestamp()
    _todas_b = estado.get("baterias", {})
    mesas_frescas = {k: v for k, v in _todas_b.items()
                     if v.get("ts") and agora - v["ts"] <= 120}
    if len(mesas_frescas) != len(_todas_b):
        estado["baterias"] = mesas_frescas
        salvar()
    rodadas = []
    for i, r in enumerate(estado["chave"]):
        rodadas.append({"nome": nome_rodada(i, len(estado["chave"])), "jogos": r})
    rodadas_prata = []
    if estado["modo"] == "misto":
        for i, r in enumerate(estado.get("chave_prata") or []):
            rodadas_prata.append({"nome": nome_rodada(i, len(estado.get("chave_prata"))), "jogos": r})
    if estado["grupos"]:
        grupos = [{"nome": g["nome"], "jogadores": list(g["jogadores"]),
                   "classificacao": classificacao(g["jogadores"])}
                  for g in (_normalizar_grupo(x) for x in estado["grupos"])]
    else:
        grupos = []
    return jsonify({
        "cabecas": estado["cabecas"],
        "jogadores": estado["jogadores"],
        "modo": estado["modo"],
        "avancar": estado["avancar"],
        "distribuicao": estado["distribuicao"],
        "ranking": estado["ranking"],
        "grupos": grupos,
        "formato_grupos": estado["formato_grupos"],
        "formato_mata": estado["formato_mata"],
        "formato_final": estado["formato_final"],
        "partidas": list(reversed(estado["partidas"])),
        "classificacao": classificacao(todos_jogadores()),
        "confrontos": confrontos(todos_jogadores()),
        "rodadas": rodadas,
        "rodadas_prata": rodadas_prata,
        "dupla": estado.get("dupla"),
        "dupla_pendentes": dupla_pendentes() if estado["modo"] == "absoluto" else [],
        "pares_pendentes": pares_pendentes(),
        "grupos_completos": grupos_completos(),
        "baterias": mesas_frescas,
        "online": list(mesas_frescas.keys()),
        "em_andamento": estado.get("em_andamento"),
        "em_mesas": [[m] + (list(x.values()) if isinstance(x, dict) else [x])
                     for m, x in estado.get("mesas", {}).items()]
            if isinstance(estado.get("mesas"), dict) else [],
        "iniciado": bool(estado.get("iniciado")),
        "aviso": NO_MATCH_WARNING,
        "campeao": campeao_atual(),
        "vice_campeao": vice_campeao_atual(),
        "campeao_prata": campeao_prata_atual(),
        "mesa_final": estado.get("mesa_final"),
        "modo_chamada": estado.get("modo_chamada", "auto"),
        "mesa_grupo": estado.get("mesa_grupo", {}),
    })


@app.route("/api/iniciar", methods=["POST"])
def api_iniciar():
    if not estado.get("jogadores"):
        return jsonify({"ok": False, "erro": "Cadastre os jogadores primeiro."}), 400
    if estado.get("modo") == "chave" and not estado.get("chave"):
        estado["chave"] = gerar_chave_grupos() if estado.get("grupos") else gerar_chave()
    elif estado.get("modo") == "absoluto" and not estado.get("dupla"):
        montar_dupla(ordem=qualificados_dos_grupos() if estado.get("grupos") else None)
    estado["iniciado"] = True
    estado["em_andamento"] = None
    estado["campeao_avisado"] = None
    comandar_placas("campeonato")
    salvar()
    return jsonify({"ok": True})


@app.route("/api/formato", methods=["POST"])
def api_formato():
    b = bloquear_se_iniciado()
    if b: return b
    d = request.get_json(silent=True) or request.form.to_dict()
    try:
        f = int(d.get("formato", 0))
    except (TypeError, ValueError):
        f = 0
    if f not in (1, 3, 5, 7):
        return jsonify({"ok": False, "erro": "Formato invalido. Use 1, 3, 5 ou 7 (melhor de N)."}), 400
    fase = d.get("fase", "grupos")
    if fase == "mata":
        estado["formato_mata"] = f
    elif fase == "final":
        estado["formato_final"] = f
    else:
        estado["formato_grupos"] = f
    salvar()
    return jsonify({"ok": True})


@app.route("/")
def pagina():
    resp = make_response(_ler_html())
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/xlsx.min.js")
def servir_xlsx():
    caminho = os.path.join(_caminho_bundle(), "xlsx.min.js")
    if not os.path.exists(caminho):
        return "Arquivo xlsx.min.js ausente.", 404
    return Response(open(caminho, "rb").read(), mimetype="application/javascript")


if __name__ == "__main__":
    carregar()
    iniciar_descoberta()
    threading.Thread(target=varredura_periodica, daemon=True).start()
    if getattr(sys, "frozen", False):
        def _abrir_painel():
            time.sleep(4)
            try:
                import webbrowser
                webbrowser.open("http://127.0.0.1:5000")
            except Exception:
                pass
        threading.Thread(target=_abrir_painel, daemon=True).start()
    print("Servidor do campeonato: http://localhost:5000")
    print("Descoberta automatica das placas: UDP porta 7777")
    print("Varredura/leitura por puxada (outbound) ligada.")
    print("O ESP32 deve apontar para este IP na porta 5000.")
    app.run(host="0.0.0.0", port=5000, debug=False)
