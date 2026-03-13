$ErrorActionPreference = "Stop"

function Get-PythonCommand {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        return "py -3"
    }

    if (Get-Command python -ErrorAction SilentlyContinue) {
        return "python"
    }

    throw "Python was not found on PATH."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$pythonCmd = Get-PythonCommand
Write-Host "Using Python command: $pythonCmd"

# Check if venv already exists and matches requirements.txt
if (Test-Path ".venv") {
    $venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        $installedPackages = & $venvPython -m pip freeze
        $requiredPackages = Get-Content "requirements.txt" | Where-Object { $_ -and -not $_.StartsWith("#") }
        if ($installedPackages -join "`n" -eq $requiredPackages -join "`n") {
            Write-Host "Virtual environment already exists and matches requirements.txt. Skipping setup."
            Write-Host "Activate with:"
            Write-Host "  .\.venv\Scripts\Activate.ps1"
            return
        }
    }
}

Invoke-Expression "$pythonCmd -m venv .venv"
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"

& $venvPython -m pip install --upgrade pip
& $venvPython -m pip install -r requirements.txt

Write-Host ""
Write-Host "Environment ready."
Write-Host "Activate with:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
