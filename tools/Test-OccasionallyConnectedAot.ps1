#Requires -Version 7.0
<#
.SYNOPSIS
    Packs OccasionallyConnected packages, then publishes and runs a clean NativeAOT consumer.

.DESCRIPTION
    Builds the package feed with the same project set and versioning used by
    Test-OccasionallyConnectedPackages.ps1. It copies the packed sample into a clean consumer folder,
    restores exclusively from that feed for ReactiveUI packages, publishes net10.0 NativeAOT, and runs
    the resulting native executable. Run on a CI host with the platform AOT linker installed.
#>
[CmdletBinding()]
param(
    [string] $Version = "0.1.0-ocaot.$([DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))",
    [string] $ArtifactsPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$src = Join-Path $repoRoot 'src'
if (-not $ArtifactsPath) { $ArtifactsPath = Join-Path $repoRoot "artifacts/oc-aot/$Version" }
$ArtifactsPath = [IO.Path]::GetFullPath($ArtifactsPath)
if (Test-Path -LiteralPath $ArtifactsPath) {
    throw "AOT output path already exists: $ArtifactsPath. Choose a fresh ArtifactsPath."
}
if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$') {
    throw "Invalid package version: $Version"
}

$dependencyProjects = @('ReactiveUI.Disposables', 'ReactiveUI.Primitives.Core', 'ReactiveUI.Primitives')
$ocProjects = @(
    'ReactiveUI.Primitives.OccasionallyConnected.Core',
    'ReactiveUI.Primitives.OccasionallyConnected',
    'ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection',
    'ReactiveUI.Primitives.OccasionallyConnected.Hosting',
    'ReactiveUI.Primitives.OccasionallyConnected.Server',
    'ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite',
    'ReactiveUI.Primitives.OccasionallyConnected.Transport.Http')
$feed = Join-Path $ArtifactsPath 'feed'
$sample = Join-Path $ArtifactsPath 'clean-sample'
$packagesFolder = Join-Path $sample '.packages'
$publishOutput = Join-Path $ArtifactsPath 'publish-aot'
$logs = Join-Path $ArtifactsPath 'logs'
New-Item -ItemType Directory -Force -Path $feed, $logs | Out-Null

function Invoke-Dotnet([string] $WorkingDirectory, [string] $LogName, [string[]] $Arguments) {
    Push-Location $WorkingDirectory
    try {
        $output = @(& dotnet @Arguments 2>&1 | ForEach-Object { "$_" })
        $exitCode = $LASTEXITCODE
    }
    finally { Pop-Location }
    $log = Join-Path $logs "$LogName.log"
    $output | Set-Content -LiteralPath $log -Encoding utf8
    if ($exitCode -ne 0) {
        $output | Select-Object -Last 30 | ForEach-Object { Write-Host "    $_" }
        throw "dotnet $($Arguments -join ' ') failed (exit $exitCode). Full log: $log"
    }
    return [pscustomobject]@{ Output = $output; Log = $log }
}

Write-Host "NativeAOT packed-consumer gate: version $Version"
Write-Host "Artifacts: $ArtifactsPath"
# The AOT consumer targets net10.0; platform workloads are unrelated to its local package feed.
$packProperties = @('-p:AndroidPrimitivesTargetFrameworks=', '-p:ApplePrimitivesTargetFrameworks=')
foreach ($project in $dependencyProjects + $ocProjects) {
    Write-Host "Packing $project"
    $null = Invoke-Dotnet $src "pack-$project" (@(
        'pack', "$project/$project.csproj", '-c', 'Release', '-nologo', '-o', $feed,
        "-p:MinVerVersionOverride=$Version", '-p:ContinuousIntegrationBuild=true',
        '--disable-build-servers', '-m:1') + $packProperties)
}

$sampleSource = Join-Path $repoRoot 'samples/OccasionallyConnected.PackedSample'
New-Item -ItemType Directory -Force -Path $sample | Out-Null
Copy-Item -LiteralPath (Join-Path $sampleSource 'OccasionallyConnected.PackedSample.csproj') -Destination $sample
Copy-Item -Path (Join-Path $sampleSource '*.cs') -Destination $sample
'<Project />' | Set-Content -LiteralPath (Join-Path $sample 'Directory.Build.props')
'<Project />' | Set-Content -LiteralPath (Join-Path $sample 'Directory.Build.targets')
'<Project><PropertyGroup><ManagePackageVersionsCentrally>false</ManagePackageVersionsCentrally></PropertyGroup></Project>' |
    Set-Content -LiteralPath (Join-Path $sample 'Directory.Packages.props')
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
"@ | Set-Content -LiteralPath (Join-Path $sample 'nuget.config')

$projectFile = 'OccasionallyConnected.PackedSample.csproj'
$commonProperties = @('-c', 'Release', '-nologo', "-p:OccasionallyConnectedPackageVersion=$Version")
$restore = Invoke-Dotnet $sample 'restore' @('restore', $projectFile, '-p:TargetFrameworks=net10.0',
    "-p:OccasionallyConnectedPackageVersion=$Version")
$resolved = Get-ChildItem -LiteralPath $packagesFolder -Directory -ErrorAction Stop |
    Where-Object Name -like 'reactiveui.primitives.occasionallyconnected*'
if (@($resolved).Count -lt $ocProjects.Count) {
    throw "Clean restore found only $(@($resolved).Count) of $($ocProjects.Count) OccasionallyConnected packages. Restore log: $($restore.Log)"
}

$publishArguments = @(
    'publish', $projectFile, '-p:TargetFrameworks=net10.0', '-f', 'net10.0', '-r', 'win-x64',
    '-o', $publishOutput, '-p:PublishAot=true', '--self-contained') + $commonProperties
$publish = Invoke-Dotnet $sample 'publish-aot' $publishArguments
$aotWarnings = @($publish.Output | Where-Object { $_ -match 'warning IL\d{4}' } | Sort-Object -Unique)
$packageWarnings = @($aotWarnings | Where-Object { $_ -match 'ReactiveUI\.' })
$thirdPartyWarnings = @($aotWarnings | Where-Object { $_ -notmatch 'ReactiveUI\.' })
$thirdPartyWarnings | ForEach-Object { Write-Host "    third-party: $_" -ForegroundColor Yellow }
$packageWarnings | ForEach-Object { Write-Host "    package warning: $_" -ForegroundColor Red }
if ($packageWarnings.Count -gt 0) {
    throw "NativeAOT publish reported $($packageWarnings.Count) warning(s) from ReactiveUI packages. Full log: $($publish.Log)"
}

$binary = Join-Path $publishOutput 'OccasionallyConnected.PackedSample.exe'
if (-not (Test-Path -LiteralPath $binary)) { throw "NativeAOT executable was not produced: $binary" }
$runOutput = @(& $binary 2>&1 | ForEach-Object { "$_" })
$runExitCode = $LASTEXITCODE
$runLog = Join-Path $logs 'publish-aot-run.log'
$runOutput | Set-Content -LiteralPath $runLog -Encoding utf8
$runOutput | Where-Object { $_ -match '^(FAIL|FAULT)' } | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
if ($runExitCode -ne 0) {
    throw "NativeAOT consumer exited with code $runExitCode. Full log: $runLog"
}
$summary = $runOutput | Where-Object { $_ -like 'SUMMARY *' } | Select-Object -Last 1
Write-Host "NativeAOT clean-consumer gate passed: win-x64; ReactiveUI warnings=$($packageWarnings.Count); $summary"
