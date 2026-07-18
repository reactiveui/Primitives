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

    This script preserves the existing Shipped baseline, resets only Unshipped, then
    uses `dotnet format analyzers` to add missing public API entries (RS0016), drop
    stale shipped entries (RS0017), and record nullability (RS0037). The resulting
    Shipped and Unshipped files are folded back into Shipped, leaving Unshipped empty.

    Tests, benchmarks, and source generators are skipped structurally.

    Projects run in parallel (PowerShell 7+ runspaces; sequential on 5.1), while the
    TFMs within one project run serially. `dotnet format` can keep AdditionalFiles for
    sibling TFMs memory-mapped, so concurrent TFMs from the same project are unsafe.
    The formatter can also simplify source imports while applying analyzer fixes; the
    script snapshots tracked source content and restores any such incidental edits.
    Override the project concurrency width with -Jobs <n> or $env:JOBS.

    Run on Windows to generate the Windows-desktop and (with the relevant workloads)
    Apple/Android target frameworks. Use the bash sibling (generate-publicapi.sh) on
    Linux/macOS. A TFM whose workload/SDK is missing is reported as failed (its seed
    files are left in place) rather than aborting the whole run.

.PARAMETER Filter
    Optional substring; only projects whose path contains it are processed.

.PARAMETER Jobs
    Maximum number of (project, TFM) pairs to generate concurrently.

.EXAMPLE
    ./tools/generate-publicapi.ps1
    Generates baselines for all tracked libraries across all buildable TFMs.

.EXAMPLE
    ./tools/generate-publicapi.ps1 -Filter Async -Jobs 4
    Only projects whose path contains 'Async', 4 at a time.
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Filter = '',
    [int]$Jobs = 0
)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcDir = (Resolve-Path (Join-Path (Join-Path $scriptDir '..') 'src')).Path
Set-Location $srcDir

# MSBuild properties that `dotnet format` cannot accept via -p:; pass through the env
# (also inherited by the parallel runspaces, which share this process).
$env:EnableWindowsTargeting = 'true'
$env:CheckEolTargetFramework = 'false'
if (-not $env:MinVerVersionOverride) { $env:MinVerVersionOverride = '255.255.255-dev' }
$env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'
$env:DOTNET_NOLOGO = 'true'
$env:DOTNET_SKIP_FIRST_TIME_EXPERIENCE = '1'
# The generator must not inherit an environment/MSBuild opt-out. Force API tracking on
# for normal library projects; Directory.Build.props still disables tests, benchmarks,
# and source generators by project path/name.
$env:TrackPublicApi = 'true'
Remove-Item Env:TRACKPUBLICAPI -ErrorAction SilentlyContinue
Remove-Item Env:trackpublicapi -ErrorAction SilentlyContinue

if ($Jobs -le 0) {
    $Jobs = if ($env:JOBS) { [int]$env:JOBS } else { [Math]::Min([Environment]::ProcessorCount, 8) }
}

$diags = @('RS0016', 'RS0017', 'RS0037')

Write-Host 'PublicAPI baseline generation'
Write-Host "  src        : $srcDir"
Write-Host "  filter     : $(if ($Filter) { $Filter } else { '<none>' })"
Write-Host "  diagnostics: $($diags -join ' ')"
Write-Host "  MinVer     : $($env:MinVerVersionOverride)"
Write-Host "  jobs       : $Jobs"
Write-Host ''

function Get-MsBuildProperty {
    param([string]$Project, [string]$Name)
    $value = & dotnet msbuild $Project "-getProperty:$Name" -nologo 2>$null
    if ($LASTEXITCODE -ne 0 -or $null -eq $value) { return '' }
    $lines = @($value | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($lines.Count -eq 0) { return '' }
    return [string]$lines[$lines.Count - 1].Trim()
}

# Regenerate one (project, TFM) pair and fold the surface into Shipped. Returns a result
# object; also defined inside the parallel block below via its source text.
function Invoke-PublicApiOne {
    param($Item, [string[]]$Diags)
    $proj = $Item.Proj
    $tfm = $Item.Tfm
    $apiNamespace = $Item.ApiNamespace
    $lf = "`n"
    $header = '#nullable enable'
    # Write LF-only so the baselines match the bash sibling's output byte-for-byte.
    $writeLf = { param($p, $lines) [IO.File]::WriteAllText($p, (($lines -join $lf) + $lf)) }
    $ensureApiFile = {
        param($p)
        if (-not (Test-Path $p)) {
            & $writeLf $p @($header)
            return
        }

        $lines = [string[]]@(Get-Content $p)
        if ($lines -contains $header) {
            return
        }

        $body = [string[]]@($lines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        & $writeLf $p (@($header) + $body)
    }
    $apiLineCount = {
        param($p)
        if (-not (Test-Path $p)) { return 0 }

        return @(
            Get-Content $p |
                Where-Object { $_ -ne $header -and -not [string]::IsNullOrWhiteSpace($_) }
        ).Count
    }
    # Back up any existing baseline so a build failure (a TFM whose workload this platform
    # lacks) restores it instead of wiping it.
    $shippedBak = if (Test-Path $Item.Shipped) { (Get-Content -Raw $Item.Shipped) -replace "`r`n", "`n" } else { $null }
    $unshippedBak = if (Test-Path $Item.Unshipped) { (Get-Content -Raw $Item.Unshipped) -replace "`r`n", "`n" } else { $null }

    & $ensureApiFile $Item.Shipped
    $beforeCount = & $apiLineCount $Item.Shipped
    # Keep Shipped as input so RS0016 reports only missing symbols. Reset Unshipped so
    # the generated delta is explicit and safe to fold after a successful analyzer run.
    & $writeLf $Item.Unshipped @($header)
    & dotnet format analyzers $proj -f $tfm --diagnostics $Diags --severity info -v quiet
    if ($LASTEXITCODE -eq 0) {
        # Fold the post-format Shipped file plus generated Unshipped delta into Shipped
        # (ordinally sorted+deduped) and reset Unshipped to the bare header.
        $surface = [string[]]@(
            Get-Content $Item.Shipped |
                Where-Object { $_ -ne $header -and -not [string]::IsNullOrWhiteSpace($_) }
            Get-Content $Item.Unshipped |
                Where-Object { $_ -ne $header -and -not [string]::IsNullOrWhiteSpace($_) }
        )
        if ($tfm -like '*-android*' -and -not [string]::IsNullOrWhiteSpace($apiNamespace)) {
            $surface += "$apiNamespace.Resource"
            $surface += "$apiNamespace.Resource.Resource() -> void"
        }
        $surface = [string[]]@($surface | Select-Object -Unique)
        [Array]::Sort($surface, [System.StringComparer]::Ordinal)
        if ($beforeCount -gt 0 -and $surface.Count -eq 0) {
            if ($null -ne $shippedBak) { [IO.File]::WriteAllText($Item.Shipped, $shippedBak) }
            if ($null -ne $unshippedBak) { [IO.File]::WriteAllText($Item.Unshipped, $unshippedBak) }
            Write-Host "FAIL [$tfm] $proj (refusing to replace non-empty Shipped with an empty baseline)"
            return [pscustomobject]@{ Ok = $false }
        }

        & $writeLf $Item.Shipped (@($header) + $surface)
        & $writeLf $Item.Unshipped @($header)
        Write-Host "OK   [$tfm] $proj"
        return [pscustomobject]@{ Ok = $true }
    }
    # Restore the prior baseline (if any) so nothing is wiped for a TFM we can't build here.
    if ($null -ne $shippedBak) { [IO.File]::WriteAllText($Item.Shipped, $shippedBak) }
    if ($null -ne $unshippedBak) { [IO.File]::WriteAllText($Item.Unshipped, $unshippedBak) }
    Write-Host "FAIL [$tfm] $proj (missing workload/SDK for this platform?)"
    return [pscustomobject]@{ Ok = $false }
}

$projects = Get-ChildItem -Path . -Recurse -Filter '*.csproj' |
    Where-Object {
        $p = $_.FullName -replace '\\', '/'
        $p -notmatch '/tests/' -and
            $p -notmatch '/benchmarks/' -and
            $_.BaseName -notlike '*.Generator'
    } |
    Sort-Object FullName

# Collect (project, TFM) work items; the worker seeds, generates, and folds each pair.
$items = [System.Collections.Generic.List[object]]::new()
$restoreSet = [System.Collections.Generic.List[string]]::new()
$skipped = 0

foreach ($projItem in $projects) {
    $proj = $projItem.FullName
    # Match the filter against a slash-normalized path so a forward-slash filter works on Windows too.
    if ($Filter -and (($proj -replace '\\', '/') -notlike "*$($Filter -replace '\\', '/')*")) { continue }

    $tfms = Get-MsBuildProperty -Project $proj -Name 'TargetFrameworks'
    if (-not $tfms) { $tfms = Get-MsBuildProperty -Project $proj -Name 'TargetFramework' }
    if (-not $tfms) {
        Write-Host "skip  (no TargetFramework(s)): $proj"
        $skipped++
        continue
    }

    $projDir = Split-Path -Parent $proj
    $apiNamespace = Get-MsBuildProperty -Project $proj -Name 'RootNamespace'
    if (-not $apiNamespace) { $apiNamespace = Get-MsBuildProperty -Project $proj -Name 'AssemblyName' }
    Write-Host "queue $proj"
    Write-Host "    TFMs: $tfms"
    $restoreSet.Add($proj)

    foreach ($tfm in ($tfms -split ';')) {
        $tfm = $tfm.Trim()
        if (-not $tfm) { continue }

        $apiDir = Join-Path $projDir (Join-Path 'PublicAPI' $tfm)
        New-Item -ItemType Directory -Force -Path $apiDir | Out-Null

        $shipped = Join-Path $apiDir 'PublicAPI.Shipped.txt'
        $unshipped = Join-Path $apiDir 'PublicAPI.Unshipped.txt'
        $items.Add([pscustomobject]@{ Proj = $proj; Tfm = $tfm; Shipped = $shipped; Unshipped = $unshipped; ApiNamespace = $apiNamespace })
    }
}
Write-Host ''

if ($items.Count -eq 0) {
    Write-Host "Nothing to generate. projects skipped: $skipped"
    return
}

# Restore once per project so the parallel workers never race on restore (they each load
# a read-only workspace afterwards).
Write-Host "Restoring $($restoreSet.Count) project(s)..."
foreach ($proj in $restoreSet) {
    & dotnet restore $proj -v quiet
    if ($LASTEXITCODE -ne 0) { Write-Host "    WARN: restore reported issues for $proj" }
}
Write-Host ''

Write-Host "Generating $($items.Count) (project, TFM) baseline(s) across $Jobs job(s)..."
$projectGroups = @($items | Group-Object -Property Proj)

# PublicApiAnalyzers fixes target AdditionalFiles, but dotnet-format can also apply
# source simplification operations (for example, removing imports). Preserve the exact
# pre-run source bytes, including any intentional dirty-worktree changes, and restore
# only files that the formatter actually changed.
$sourceSnapshot = [System.Collections.Generic.Dictionary[string, byte[]]]::new(
    [System.StringComparer]::OrdinalIgnoreCase)
$sourceFiles = Get-ChildItem -Path $srcDir -Recurse -Filter '*.cs' -File |
    Where-Object { $_.FullName -notmatch '[\\/](bin|obj)[\\/]' }
foreach ($sourceFile in $sourceFiles) {
    $sourceSnapshot.Add($sourceFile.FullName, [IO.File]::ReadAllBytes($sourceFile.FullName))
}

function Restore-SourceSnapshot {
    $restored = 0
    foreach ($entry in $sourceSnapshot.GetEnumerator()) {
        if (-not (Test-Path -LiteralPath $entry.Key)) { continue }

        $current = [IO.File]::ReadAllBytes($entry.Key)
        $original = $entry.Value
        $equal = $current.Length -eq $original.Length
        if ($equal) {
            for ($index = 0; $index -lt $current.Length; $index++) {
                if ($current[$index] -ne $original[$index]) {
                    $equal = $false
                    break
                }
            }
        }

        if (-not $equal) {
            [IO.File]::WriteAllBytes($entry.Key, $original)
            $restored++
        }
    }
    return $restored
}

$restoredSourceFiles = 0
try {
    if ($PSVersionTable.PSVersion.Major -ge 7 -and $Jobs -gt 1) {
        $funcDef = ${function:Invoke-PublicApiOne}.ToString()
        $results = $projectGroups | ForEach-Object -ThrottleLimit $Jobs -Parallel {
            ${function:Invoke-PublicApiOne} = $using:funcDef
            foreach ($it in $_.Group) {
                Invoke-PublicApiOne -Item $it -Diags $using:diags
            }
        }
    }
    else {
        if ($Jobs -gt 1) { Write-Host '  (PowerShell 5.1: running sequentially — use pwsh 7+ for parallelism)' }
        $results = foreach ($group in $projectGroups) {
            foreach ($it in $group.Group) {
                Invoke-PublicApiOne -Item $it -Diags $diags
            }
        }
    }
}
finally {
    $restoredSourceFiles = Restore-SourceSnapshot
}
Write-Host ''

$generated = @($results | Where-Object { $_.Ok }).Count
$failed = @($results | Where-Object { -not $_.Ok }).Count

Write-Host "Done. generated: $generated TFM baseline(s), failed: $failed, projects skipped: $skipped, source files restored: $restoredSourceFiles"
if ($failed -ne 0) { exit 1 }
