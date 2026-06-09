param(
    [Parameter(Mandatory = $true)]
    [string]$To,

    [string]$Subject = "A proposal for official collaboration - StarCraft TMG",
    [string]$SmtpHost = "tmg-stats.org",
    [int]$SmtpPort = 587,
    [string]$Username = "customersupport@tmg-stats.org",
    [string]$Sender = "customersupport@tmg-stats.org",
    [string]$SenderName = "TMG Stats"
)

$ErrorActionPreference = "Stop"

$toolsDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$plainPath = Join-Path $toolsDir "archon_studio_email.txt"
$htmlPath = Join-Path $toolsDir "archon_studio_email.html"

$plainBody = Get-Content -LiteralPath $plainPath -Raw -Encoding UTF8
$htmlBody = Get-Content -LiteralPath $htmlPath -Raw -Encoding UTF8

$password = $env:TMG_SMTP_PASSWORD
if ([string]::IsNullOrWhiteSpace($password)) {
    $securePassword = Read-Host "SMTP password for $Username" -AsSecureString
    $credential = [System.Net.NetworkCredential]::new($Username, $securePassword)
} else {
    $credential = [System.Net.NetworkCredential]::new($Username, $password)
}

$message = [System.Net.Mail.MailMessage]::new()
$smtp = [System.Net.Mail.SmtpClient]::new($SmtpHost, $SmtpPort)

try {
    $message.From = [System.Net.Mail.MailAddress]::new($Sender, $SenderName)
    $message.ReplyToList.Add($Sender)
    $message.To.Add($To)
    $message.Subject = $Subject
    $message.SubjectEncoding = [System.Text.Encoding]::UTF8
    $message.HeadersEncoding = [System.Text.Encoding]::UTF8

    $plainView = [System.Net.Mail.AlternateView]::CreateAlternateViewFromString(
        $plainBody,
        [System.Text.Encoding]::UTF8,
        "text/plain"
    )
    $htmlView = [System.Net.Mail.AlternateView]::CreateAlternateViewFromString(
        $htmlBody,
        [System.Text.Encoding]::UTF8,
        "text/html"
    )
    $message.AlternateViews.Add($plainView)
    $message.AlternateViews.Add($htmlView)

    $smtp.EnableSsl = $true
    $smtp.Credentials = $credential
    $smtp.Send($message)

    Write-Host "Sent HTML email to $To from $Sender."
} finally {
    $smtp.Dispose()
    $message.Dispose()
}
