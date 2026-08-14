# Renomeador em lote de mesas (ESP32 em modo AP)
# Conecta em cada AP "Placar-XXXX" e manda /api/nome com o proximo nome da lista.
# Uso:
#   powershell -ExecutionPolicy Bypass -File renomear_mesas.ps1                -> Mesa 01, Mesa 02, ...
#   powershell -ExecutionPolicy Bypass -File renomear_mesas.ps1 -Senha "minha" -Nomes "Mesa 1,Mesa 2,Mesa 3"
param(
  [string]$Senha = "12345678Super",
  [string]$Nomes = ""
)

$ErrorActionPreference = "Continue"
$apIP = "192.168.4.1"
$base = "Placar-"

$nomes = @()
if ($Nomes -ne "") {
  $nomes = $Nomes -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
} else {
  for ($i = 1; $i -le 20; $i++) { $nomes += ("Mesa {0:D2}" -f $i) }
}

function Get-PlacarNetworks {
  $out = netsh wlan show networks 2>$null
  $lista = @()
  foreach ($linha in $out) {
    $l = $linha.Trim()
    if ($l -match "^SSID\s*\d+\s*:\s*(.+)$") {
      $ssid = $Matches[1].Trim()
      if ($ssid -like "$base*") { $lista += $ssid }
    }
  }
  return $lista
}

function New-ProfileXml([string]$ssid, [string]$senha) {
  $hex = ($ssid.ToCharArray() | ForEach-Object { '{0:X}' -f [int]$_ }) -join ""
  return @"
<?xml version="1.0"?>
<WLANProfile xmlns="http://www.microsoft.com/networking/WLAN/profile/v1">
  <name>$ssid</name>
  <SSIDConfig><SSID><hex>$hex</hex><name>$ssid</name></SSID></SSIDConfig>
  <connectionType>ESS</connectionType>
  <connectionMode>manual</connectionMode>
  <MSM><security><authEncryption><authentication>WPA2PSK</authentication><encryption>AES</encryption><useOneX>false</useOneX></authEncryption>
  <sharedKey><keyType>passPhrase</keyType><protected>false</protected><keyMaterial>$senha</keyMaterial></sharedKey></security></MSM>
</WLANProfile>
"@
}

function Wait-ApOnline {
  $deadline = (Get-Date).AddSeconds(25)
  while ((Get-Date) -lt $deadline) {
    try {
      $tcp = New-Object Net.Sockets.TcpClient
      $tcp.Connect($apIP, 80)
      $tcp.Close()
      return $true
    } catch { Start-Sleep -Milliseconds 500 }
  }
  return $false
}

function Send-Rename([string]$nomeNovo) {
  try {
    $u = "http://$apIP/api/nome?nome=" + [uri]::EscapeDataString($nomeNovo)
    $r = Invoke-WebRequest -UseBasicParsing -Method Post -Uri $u -TimeoutSec 10
    return $r.StatusCode -eq 200
  } catch { return $false }
}

Write-Host "== Renomeador de mesas (AP) =="
$idx = 0
while ($idx -lt $nomes.Count) {
  $encontradas = @(Get-PlacarNetworks)
  if ($encontradas.Count -eq 0) { Write-Host "Nenhuma rede Placar-* encontrada. Encerrando." ; break }
  $ssid = $encontradas[0]
  $nomeNovo = $nomes[$idx]
  Write-Host "`n[$($idx+1)/$($nomes.Count)] Conectando em '$ssid'..."
  $perfil = New-ProfileXml $ssid $Senha
  $perfil | Out-File -Encoding utf8 -FilePath "$env:TEMP\perfil_wlan.xml"
  netsh wlan delete profile name="$ssid" 2>$null | Out-Null
  netsh wlan add profile filename="$env:TEMP\perfil_wlan.xml" | Out-Null
  netsh wlan connect name="$ssid" ssid="$ssid" | Out-Null
  if (-not (Wait-ApOnline)) { Write-Host "  Falha ao conectar no AP. Tente novamente." ; break }
  Start-Sleep -Milliseconds 800
  if (Send-Rename $nomeNovo) { Write-Host "  Renomeado para '$nomeNovo'." }
  else { Write-Host "  Erro ao renomear '$ssid'." }
  Start-Sleep -Seconds 2
  netsh wlan disconnect 2>$null | Out-Null
  netsh wlan delete profile name="$ssid" 2>$null | Out-Null
  $idx++
}
Remove-Item "$env:TEMP\perfil_wlan.xml" -ErrorAction SilentlyContinue
Write-Host "`nConcluido: $idx mesa(s) processada(s)."
