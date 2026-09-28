#Requires -Version 7.0
<#
.SYNOPSIS
    Packs the OccasionallyConnected packages and runs the section 18 release gates against them.

.DESCRIPTION
    1. Packs ReactiveUI.Disposables, ReactiveUI.Primitives.Core, ReactiveUI.Primitives and the seven
       OccasionallyConnected packages with one unique prerelease version into a fresh local feed.
    2. Rebuilds the OccasionallyConnected projects from scratch, packs them again into a second folder and
       compares both packs entry by entry (deterministic package comparison).
    3. Checks every OccasionallyConnected package for the expected lib/<tfm> folders, a .snupkg with portable
       PDBs that match the assemblies, deterministic source paths and Source Link.
    4. Copies samples/OccasionallyConnected.PackedSample into a clean folder with an empty Directory.Build.props,
       an empty Directory.Packages.props and a nuget.config that maps ReactiveUI.Primitives* to the local feed
       only. It restores into a private packages folder, then builds and runs the sample for each target framework.
    5. Publishes the sample for net10.0 trimmed and as NativeAOT, fails on trim/AOT warnings from
       ReactiveUI.Primitives assemblies, and runs both published binaries.

    Run it from any folder. It calls dotnet from ./src for the repository projects, as CLAUDE.md requires.

.PARAMETER Version
    The package version. Defaults to 0.1.0-octest.<UTC timestamp>.

.PARAMETER ArtifactsPath
    The output folder. Defaults to artifacts/oc-packages under the repository root. It is deleted first.

.PARAMETER SampleTargetFrameworks
    The sample frameworks to build and run. Defaults to net8.0, net9.0, net10.0, net11.0 (when the SDK
    supports it) and, on Windows, net462, net472, net48 and net481.

.EXAMPLE
    pwsh tools/Test-OccasionallyConnectedPackages.ps1
#>
[CmdletBinding()]
param(
    [string] $Version,
    [string] $ArtifactsPath,
    [string[]] $SampleTargetFrameworks,
    [switch] $SkipDeterminism,
    [switch] $SkipSample,
    [switch] $SkipAot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$src = Join-Path $repoRoot 'src'
if (-not $ArtifactsPath) { $ArtifactsPath = Join-Path $repoRoot 'artifacts/oc-packages' }
if (-not $Version) { $Version = "0.1.0-octest.$([DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))" }
$ArtifactsPath = [IO.Path]::GetFullPath($ArtifactsPath)

$dependencyProjects = @('ReactiveUI.Disposables', 'ReactiveUI.Primitives.Core', 'ReactiveUI.Primitives')
$ocProjects = @(
    'ReactiveUI.Primitives.OccasionallyConnected.Core',
    'ReactiveUI.Primitives.OccasionallyConnected',
    'ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection',
    'ReactiveUI.Primitives.OccasionallyConnected.Hosting',
    'ReactiveUI.Primitives.OccasionallyConnected.Server',
    'ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite',
    'ReactiveUI.Primitives.OccasionallyConnected.Transport.Http')
$libraryTfms = @('net8.0', 'net9.0', 'net10.0', 'net11.0', 'net462', 'net472', 'net48', 'net481')

$sdkMajor = [int](((& dotnet --version) -split '[.-]')[0])
if (-not $SampleTargetFrameworks) {
    $SampleTargetFrameworks = @('net8.0', 'net9.0', 'net10.0')
    if ($sdkMajor -ge 11) { $SampleTargetFrameworks += 'net11.0' }
    if ($IsWindows) { $SampleTargetFrameworks += @('net462', 'net472', 'net48', 'net481') }
}

if (Test-Path $ArtifactsPath) { Remove-Item $ArtifactsPath -Recurse -Force }
$feed = Join-Path $ArtifactsPath 'feed'
$secondPack = Join-Path $ArtifactsPath 'pack-second'
$logs = Join-Path $ArtifactsPath 'logs'
$sample = Join-Path $ArtifactsPath 'clean-sample'
New-Item -ItemType Directory -Force -Path $feed, $secondPack, $logs | Out-Null

$results = [System.Collections.Generic.List[object]]::new()

function Add-Result([string] $Gate, [string] $Status, [string] $Detail) {
    $results.Add([pscustomobject]@{ Gate = $Gate; Status = $Status; Detail = $Detail })
    $color = switch ($Status) { 'PASS' { 'Green' } 'FAIL' { 'Red' } default { 'Yellow' } }
    Write-Host ("[{0}] {1}: {2}" -f $Status, $Gate, $Detail) -ForegroundColor $color
}

# Runs dotnet in a folder, writes the full output to a log, and returns the exit code and output lines.
function Invoke-Dotnet([string] $WorkingDirectory, [string] $LogName, [string[]] $Arguments) {
    $log = Join-Path $logs "$LogName.log"
    Push-Location $WorkingDirectory
    try {
        $output = & dotnet @Arguments 2>&1 | ForEach-Object { "$_" }
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    $output | Set-Content -Path $log -Encoding utf8
    return [pscustomobject]@{ ExitCode = $exitCode; Output = @($output); Log = $log }
}

function Show-Tail($Run, [int] $Lines = 25) {
    $Run.Output | Select-Object -Last $Lines | ForEach-Object { Write-Host "    $_" }
    Write-Host "    (full log: $($Run.Log))"
}

$commit = (& git -C $repoRoot rev-parse HEAD).Trim()
# The clean consumer exercises desktop targets; platform workloads are not needed to pack its local feed.
$packProperties = @('-c', 'Release', '-nologo', "-p:MinVerVersionOverride=$Version", '-p:ContinuousIntegrationBuild=true',
    '-p:AndroidPrimitivesTargetFrameworks=', '-p:ApplePrimitivesTargetFrameworks=')
Write-Host "Version $Version, commit $commit, SDK $(& dotnet --version)"
Write-Host "Artifacts: $ArtifactsPath"

# 1. Pack everything into the local feed.
$packFailed = $false
foreach ($project in $dependencyProjects + $ocProjects) {
    $run = Invoke-Dotnet $src "pack-$project" (@('pack', "$project/$project.csproj", '-o', $feed) + $packProperties)
    if ($run.ExitCode -ne 0) {
        Add-Result 'pack' 'FAIL' "$project (exit $($run.ExitCode))"
        Show-Tail $run
        $packFailed = $true
    }
}

if ($packFailed) {
    $results | Format-Table -AutoSize | Out-String | Write-Host
    exit 1
}

$packages = Get-ChildItem $feed -Filter '*.nupkg' | Sort-Object Name
Add-Result 'pack' 'PASS' "$($packages.Count) packages at $Version in $feed"

$inspector = Join-Path $PSScriptRoot 'OccasionallyConnectedPackageInspector.cs'
function Invoke-Inspector([string] $Gate, [string[]] $Arguments) {
    $run = Invoke-Dotnet $PSScriptRoot "inspect-$Gate" (@('run', $inspector, '--') + $Arguments)
    $failed = @($run.Output | Where-Object { $_ -like 'FAIL *' })
    $passed = @($run.Output | Where-Object { $_ -like 'PASS *' })
    $failed | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
    if ($run.ExitCode -eq 0 -and $failed.Count -eq 0 -and $passed.Count -gt 0) {
        Add-Result $Gate 'PASS' "$($passed.Count) checks passed (log: $($run.Log))"
    }
    else {
        if ($failed.Count -eq 0) { Show-Tail $run }
        Add-Result $Gate 'FAIL' "$($failed.Count) of $($failed.Count + $passed.Count) checks failed (log: $($run.Log))"
    }
}

# 2. Deterministic package comparison: rebuild the OC projects from scratch and pack them again.
if ($SkipDeterminism) {
    Add-Result 'deterministic-packages' 'SKIP' 'skipped by -SkipDeterminism'
}
else {
    $rebuildFailed = $false
    foreach ($project in $ocProjects) {
        $build = Invoke-Dotnet $src "rebuild-$project" (@('build', "$project/$project.csproj", '--no-dependencies', '--no-incremental') + $packProperties)
        $pack = if ($build.ExitCode -eq 0) { Invoke-Dotnet $src "repack-$project" (@('pack', "$project/$project.csproj", '--no-build', '-o', $secondPack) + $packProperties) } else { $build }
        if ($pack.ExitCode -ne 0) {
            Add-Result 'deterministic-packages' 'FAIL' "second build of $project failed"
            Show-Tail $pack
            $rebuildFailed = $true
        }
    }

    if (-not $rebuildFailed) {
        Invoke-Inspector 'deterministic-packages' @('compare', '--left', $feed, '--right', $secondPack, '--version', $Version, '--packages', ($ocProjects -join ','))
    }
}

# 3. Symbol packages, portable PDBs, Source Link and lib/<tfm> folders.
Invoke-Inspector 'symbols-sourcelink-tfms' @('verify', '--feed', $feed, '--version', $Version, '--packages', ($ocProjects -join ','), '--tfms', ($libraryTfms -join ','), '--commit', $commit)

# 4. Clean-project install and the section 16 sample on every framework.
if (-not $SkipSample) {
    New-Item -ItemType Directory -Force -Path $sample | Out-Null
    Copy-Item (Join-Path $repoRoot 'samples/OccasionallyConnected.PackedSample/*') $sample -Include '*.cs', '*.csproj'
    '<Project />' | Set-Content (Join-Path $sample 'Directory.Build.props')
    '<Project />' | Set-Content (Join-Path $sample 'Directory.Build.targets')
    '<Project><PropertyGroup><ManagePackageVersionsCentrally>false</ManagePackageVersionsCentrally></PropertyGroup></Project>' |
        Set-Content (Join-Path $sample 'Directory.Packages.props')
    $packagesFolder = Join-Path $sample '.packages'
    @"
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <config>
    <add key="globalPackagesFolder" value="$packagesFolder" />
  </config>
  <packageSources>
    <clear />
    <add key="local-oc" value="$feed" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="local-oc">
      <package pattern="ReactiveUI.Primitives*" />
      <package pattern="ReactiveUI.Disposables" />
    </packageSource>
    <packageSource key="nuget.org">
      <package pattern="*" />
    </packageSource>
  </packageSourceMapping>
</configuration>
"@ | Set-Content (Join-Path $sample 'nuget.config')

    $sampleProject = 'OccasionallyConnected.PackedSample.csproj'
    $sampleProperties = @('-c', 'Release', '-nologo', "-p:OccasionallyConnectedPackageVersion=$Version")
    $restore = Invoke-Dotnet $sample 'sample-restore' (@('restore', $sampleProject, "-p:OccasionallyConnectedPackageVersion=$Version"))
    if ($restore.ExitCode -ne 0) {
        Add-Result 'clean-install' 'FAIL' 'restore from the local feed failed'
        Show-Tail $restore
    }
    else {
        $installed = Get-ChildItem $packagesFolder -Directory | Where-Object Name -like 'reactiveui.*' |
            ForEach-Object { "$($_.Name)/$((Get-ChildItem $_.FullName -Directory).Name -join ',')" }
        Add-Result 'clean-install' 'PASS' ("restored from the local feed: " + ($installed -join '; '))
        foreach ($tfm in $SampleTargetFrameworks) {
            $build = Invoke-Dotnet $sample "sample-build-$tfm" (@('build', $sampleProject, '-f', $tfm, '--no-restore') + $sampleProperties)
            if ($build.ExitCode -ne 0) {
                Add-Result "sample-$tfm" 'FAIL' 'build failed'
                Show-Tail $build
                continue
            }

            $run = Invoke-Dotnet $sample "sample-run-$tfm" (@('run', '--project', $sampleProject, '-f', $tfm, '--no-build') + $sampleProperties)
            $checks = @($run.Output | Where-Object { $_ -match '^(PASS|FAIL) ' })
            $summary = ($run.Output | Where-Object { $_ -like 'SUMMARY *' } | Select-Object -Last 1)
            $run.Output | Where-Object { $_ -match '^(FAIL|FAULT)' } | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
            if ($run.ExitCode -eq 0) {
                Add-Result "sample-$tfm" 'PASS' "$summary (log: $($run.Log))"
            }
            else {
                if ($checks.Count -eq 0) { Show-Tail $run }
                Add-Result "sample-$tfm" 'FAIL' "exit $($run.ExitCode); $summary (log: $($run.Log))"
            }
        }
    }
}

# Publishes the sample, classifies trim/AOT warnings and runs the published binary.
function Test-Publish([string] $Gate, [string[]] $PublishProperties) {
    $os = if ($IsWindows) { 'win' } elseif ($IsMacOS) { 'osx' } else { 'linux' }
    $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
    $rid = "$os-$arch"
    $output = Join-Path $ArtifactsPath $Gate
    # Warnings stay warnings here so each one can be attributed to its assembly below.
    # TargetFrameworks is pinned so restore does not evaluate the .NET Framework legs, which cannot use AOT.
    $arguments = @('publish', 'OccasionallyConnected.PackedSample.csproj', '-p:TargetFrameworks=net10.0', '-f', 'net10.0', '-r', $rid, '-o', $output,
        '-c', 'Release', '-nologo', "-p:OccasionallyConnectedPackageVersion=$Version", '-p:TreatWarningsAsErrors=false') + $PublishProperties
    $publish = Invoke-Dotnet $sample $Gate $arguments
    $warnings = @($publish.Output | Where-Object { $_ -match 'warning (IL\d{4})' } | Sort-Object -Unique)
    $ocWarnings = @($warnings | Where-Object { $_ -match 'ReactiveUI\.' })
    $otherWarnings = @($warnings | Where-Object { $_ -notmatch 'ReactiveUI\.' })
    $otherWarnings | ForEach-Object { Write-Host "    third-party: $_" -ForegroundColor Yellow }
    $ocWarnings | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
    if ($publish.ExitCode -ne 0) {
        $missing = $publish.Output | Where-Object { $_ -match 'Platform linker|vswhere|link\.exe|clang|Desktop development with C\+\+|NETSDK1(083|084|094|183|204)' }
        if ($missing) {
            Add-Result $Gate 'SKIP' "toolchain prerequisite missing: $(@($missing)[0].Trim())"
        }
        else {
            Add-Result $Gate 'FAIL' "publish failed (log: $($publish.Log))"
            Show-Tail $publish
        }

        return
    }

    $binary = Join-Path $output ($(if ($IsWindows) { 'OccasionallyConnected.PackedSample.exe' } else { 'OccasionallyConnected.PackedSample' }))
    $runOutput = & $binary 2>&1 | ForEach-Object { "$_" }
    $exitCode = $LASTEXITCODE
    $runOutput | Set-Content (Join-Path $logs "$Gate-run.log") -Encoding utf8
    $summary = $runOutput | Where-Object { $_ -like 'SUMMARY *' } | Select-Object -Last 1
    $runOutput | Where-Object { $_ -match '^(FAIL|FAULT)' } | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
    $detail = "$rid; OC warnings=$($ocWarnings.Count); third-party warnings=$($otherWarnings.Count); run exit $exitCode; $summary"
    Add-Result $Gate ($(if ($ocWarnings.Count -eq 0 -and $exitCode -eq 0) { 'PASS' } else { 'FAIL' })) $detail
}

# 5. Trimming and NativeAOT smoke tests.
if ($SkipAot -or $SkipSample) {
    Add-Result 'trim-aot' 'SKIP' 'skipped by -SkipAot or -SkipSample'
}
else {
    Test-Publish 'publish-trimmed' @('-p:PublishTrimmed=true', '--self-contained')
    Test-Publish 'publish-aot' @('-p:PublishAot=true')
}

Write-Host ''
$results | Format-Table -AutoSize -Wrap | Out-String -Width 220 | Write-Host
$failedGates = @($results | Where-Object Status -eq 'FAIL')
if ($failedGates.Count -gt 0) {
    Write-Host "$($failedGates.Count) gate(s) failed." -ForegroundColor Red
    exit 1
}

Write-Host 'All gates passed.' -ForegroundColor Green
exit 0
