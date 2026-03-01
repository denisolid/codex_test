param(
  [switch]$Staged,
  [ValidateSet("git", "dir")]
  [string]$Mode = "git"
)

$ErrorActionPreference = "Stop"

function Resolve-GitleaksBinary {
  try {
    $cmd = Get-Command gitleaks -ErrorAction Stop
    if ($cmd -and $cmd.Source) {
      return $cmd.Source
    }
  } catch {
    # Fallback below.
  }

  $packagesRoot = Join-Path $env:LOCALAPPDATA "Microsoft\\WinGet\\Packages"
  if (Test-Path $packagesRoot) {
    $candidate = Get-ChildItem $packagesRoot -Directory -Filter "Gitleaks.Gitleaks_*" `
      | Sort-Object LastWriteTime -Descending `
      | Select-Object -First 1
    if ($candidate) {
      $exePath = Join-Path $candidate.FullName "gitleaks.exe"
      if (Test-Path $exePath) {
        return $exePath
      }
    }
  }

  throw "gitleaks executable not found. Install with: winget install --id Gitleaks.Gitleaks --exact --source winget"
}

$gitleaks = Resolve-GitleaksBinary

$args = @($Mode, ".", "--no-banner", "--redact", "--exit-code", "1")
if ($Mode -eq "git" -and $Staged) {
  $args += "--staged"
}

& $gitleaks @args
exit $LASTEXITCODE
