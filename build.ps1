param(
    [switch]$Debug
)
# Builds the native headset binaries, then the APK.
# Release output: app\build\outputs\apk\release\app-release.apk (signed if keystore.properties exists)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $root 'native\build.ps1')
if ($LASTEXITCODE -ne 0) { throw "Native build failed: $LASTEXITCODE" }
$task = 'assembleRelease'
if ($Debug) { $task = 'assembleDebug' }
Push-Location $root
try {
    & (Join-Path $root 'gradlew.bat') $task
    if ($LASTEXITCODE -ne 0) { throw "Gradle $task failed: $LASTEXITCODE" }
} finally {
    Pop-Location
}
