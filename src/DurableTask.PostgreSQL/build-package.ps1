# Build and pack DurableTask.PostgreSQL as NuGet package
# Usage: .\build-package.ps1 [-Version "1.0.0"] [-OutputDir "..\..\artifacts"]

param(
    [string]$Version = "1.0.0-alpha",
    [string]$OutputDir = "..\..\artifacts\packages",
    [switch]$Push,
    [string]$NuGetSource = ""
)

$ErrorActionPreference = "Stop"

Write-Host "Building DurableTask.PostgreSQL v$Version..." -ForegroundColor Cyan

# Clean previous builds
if (Test-Path "bin") {
    Remove-Item -Recurse -Force "bin"
}
if (Test-Path "obj") {
    Remove-Item -Recurse -Force "obj"
}

# Build in Release mode
Write-Host "Compiling project..." -ForegroundColor Yellow
dotnet build -c Release /p:Version=$Version

if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed"
    exit 1
}

# Create output directory
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

# Pack the NuGet package
Write-Host "Creating NuGet package..." -ForegroundColor Yellow
dotnet pack -c Release /p:PackageVersion=$Version --no-build -o $OutputDir

if ($LASTEXITCODE -ne 0) {
    Write-Error "Pack failed"
    exit 1
}

$packagePath = Join-Path $OutputDir "DurableTask.PostgreSQL.$Version.nupkg"

Write-Host "✅ Package created: $packagePath" -ForegroundColor Green

# Optionally push to NuGet source
if ($Push) {
    if ([string]::IsNullOrWhiteSpace($NuGetSource)) {
        Write-Error "NuGet source must be specified with -NuGetSource when using -Push"
        exit 1
    }

    Write-Host "Pushing package to $NuGetSource..." -ForegroundColor Yellow
    dotnet nuget push $packagePath --source $NuGetSource

    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Package pushed successfully" -ForegroundColor Green
    } else {
        Write-Error "Push failed"
        exit 1
    }
}

Write-Host ""
Write-Host "To install this package locally, run:" -ForegroundColor Cyan
Write-Host "  dotnet add package DurableTask.PostgreSQL --version $Version --source $OutputDir" -ForegroundColor White
