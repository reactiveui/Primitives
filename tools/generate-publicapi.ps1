#requires -Version 5.1
<#
.SYNOPSIS
    (Re)generate PublicAPI baseline files for every shipped ReactiveUI.Primitives library,
    across each target framework that builds on this machine.

.DESCRIPTION
    The Microsoft.CodeAnalysis.PublicApiAnalyzers (RS0016 / RS0017 / RS0037) require a
    per-TFM pair of tracking files:

        <Project>/PublicAPI/<tfm>/PublicAPI.Shipped.txt
        <Project>/PublicAPI/<tfm>/PublicAPI.Unshipped.txt

    This script seeds those files and uses `dotnet format analyzers` to populate the
    Unshipped file with the project's current public surface (RS0016), drop stale
    entries (RS0017), and record nullability (RS0037).

    Only projects with MSBuild property TrackPublicApi=true are processed; the
    tests/ and benchmarks/ trees opt out centrally in src/Directory.Build.props.

    Run on Windows to generate the Windows-desktop and (with the relevant workloads)
    Apple/Android target frameworks. Use the bash sibling (generate-publicapi.sh) on
    Linux/macOS. A TFM whose workload/SDK is missing is skipped with a warning rather
    than aborting the whole run.

.PARAMETER Filter
    Optional substring; only projects whose path contains it are processed.

.EXAMPLE
    ./tools/generate-publicapi.ps1
    Generates baselines for all tracked libraries across all buildable TFMs.

.EXAMPLE
    ./tools/generate-publicapi.ps1 -Filter Async
    Only projects whose path contains 'Async'.
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Filter = ''
)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcDir = (Resolve-Path (Join-Path $scriptDir '..' 'src')).Path
Set-Location $srcDir

# MSBuild properties that `dotnet format` cannot accept via -p:; pass through the env.
$env:EnableWindowsTargeting = 'true'
$env:CheckEolTargetFramework = 'false'
if (-not $env:MinVerVersionOverride) { $env:MinVerVersionOverride = '255.255.255-dev' }

$diags = @('RS0016', 'RS0017', 'RS0037')

Write-Host 'PublicAPI baseline generation'
Write-Host "  src        : $srcDir"
Write-Host "  filter     : $(if ($Filter) { $Filter } else { '<none>' })"
Write-Host "  diagnostics: $($diags -join ' ')"
Write-Host "  MinVer     : $($env:MinVerVersionOverride)"
Write-Host ''

function Get-MsBuildProperty {
    param([string]$Project, [string]$Name)
    $value = & dotnet msbuild $Project "-getProperty:$Name" -nologo 2>$null
    if ($LASTEXITCODE -ne 0 -or $null -eq $value) { return '' }
    return ($value | Out-String).Trim()
}

$projects = Get-ChildItem -Path . -Recurse -Filter '*.csproj' |
    Where-Object {
        $p = $_.FullName -replace '\\', '/'
        $p -notmatch '/tests/' -and $p -notmatch '/benchmarks/'
    } |
    Sort-Object FullName

$generated = 0
$failed = 0
$skipped = 0

foreach ($projItem in $projects) {
    $proj = $projItem.FullName
    if ($Filter -and ($proj -notlike "*$Filter*")) { continue }

    $track = Get-MsBuildProperty -Project $proj -Name 'TrackPublicApi'
    if ($track -ne 'true') {
        Write-Host "skip  (TrackPublicApi != true): $proj"
        $skipped++
        continue
    }

    $tfms = Get-MsBuildProperty -Project $proj -Name 'TargetFrameworks'
    if (-not $tfms) { $tfms = Get-MsBuildProperty -Project $proj -Name 'TargetFramework' }
    if (-not $tfms) {
        Write-Host "skip  (no TargetFramework(s)): $proj"
        $skipped++
        continue
    }

    $projDir = Split-Path -Parent $proj
    Write-Host "=== $proj"
    Write-Host "    TFMs: $tfms"

    foreach ($tfm in ($tfms -split ';')) {
        $tfm = $tfm.Trim()
        if (-not $tfm) { continue }

        $apiDir = Join-Path $projDir (Join-Path 'PublicAPI' $tfm)
        New-Item -ItemType Directory -Force -Path $apiDir | Out-Null

        $shipped = Join-Path $apiDir 'PublicAPI.Shipped.txt'
        $unshipped = Join-Path $apiDir 'PublicAPI.Unshipped.txt'
        # Seed Shipped only if absent; always reset Unshipped to the bare header so the
        # regenerated surface is deterministic.
        if (-not (Test-Path $shipped)) { "#nullable enable" | Set-Content -NoNewline:$false -Path $shipped }
        "#nullable enable" | Set-Content -NoNewline:$false -Path $unshipped

        Write-Host "    --> [$tfm]"
        & dotnet format analyzers $proj -f $tfm --diagnostics @diags --severity info -v quiet
        if ($LASTEXITCODE -eq 0) {
            $generated++
        }
        else {
            Write-Host "    WARN: generation failed for [$tfm] (missing workload/SDK for this platform?)"
            $failed++
        }
    }
    Write-Host ''
}

Write-Host "Done. generated: $generated TFM baseline(s), failed: $failed, projects skipped: $skipped"
if ($failed -ne 0) { exit 1 }
