#Requires -Version 7.0
<#
.SYNOPSIS
    Packs the OccasionallyConnected packages and verifies their supply-chain evidence.

.DESCRIPTION
    Produces SPDX SBOM, dependency, license, vulnerability, and deprecation reports. The gate
    fails when NuGet reports a vulnerable or deprecated dependency, package license metadata is
    missing, the SBOM has no detected dependencies, or SBOM validation fails.
#>
[CmdletBinding()]
param(
    [string] $Version = '0.1.0-ocscan',
    [string] $ArtifactsPath,
    [string[]] $ProjectNames = @(
        'ReactiveUI.Primitives.OccasionallyConnected.Core',
        'ReactiveUI.Primitives.OccasionallyConnected',
        'ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection',
        'ReactiveUI.Primitives.OccasionallyConnected.Hosting',
        'ReactiveUI.Primitives.OccasionallyConnected.Server',
        'ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite',
        'ReactiveUI.Primitives.OccasionallyConnected.Transport.Http'
    )
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$src = Join-Path $repoRoot 'src'
if (-not $ArtifactsPath) { $ArtifactsPath = Join-Path $repoRoot 'artifacts/oc-supply-chain' }
$ArtifactsPath = [IO.Path]::GetFullPath($ArtifactsPath)
if (Test-Path -LiteralPath $ArtifactsPath) {
    throw "Supply-chain output path already exists: $ArtifactsPath. Choose a fresh ArtifactsPath."
}
if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$') {
    throw "Invalid package version: $Version"
}
if ($ProjectNames.Count -eq 0) { throw 'At least one package project is required.' }

$drop = Join-Path $ArtifactsPath 'packages'
$reports = Join-Path $ArtifactsPath 'reports'
$toolDir = Join-Path $ArtifactsPath 'tools'
New-Item -ItemType Directory -Force -Path $drop, $reports, $toolDir | Out-Null
$licenses = [System.Collections.Generic.Dictionary[string, object]]::new([StringComparer]::OrdinalIgnoreCase)
$findings = [System.Collections.Generic.List[object]]::new()

function Invoke-Dotnet([string[]] $Arguments, [string] $OutputPath) {
    Push-Location $src
    try {
        $output = @(& dotnet @Arguments 2>&1 | ForEach-Object { "$_" })
        $exitCode = $LASTEXITCODE
    }
    finally { Pop-Location }
    if ($OutputPath) { $output | Set-Content -LiteralPath $OutputPath -Encoding utf8 }
    if ($exitCode -ne 0) {
        throw "dotnet $($Arguments -join ' ') failed (exit $exitCode). $($output | Select-Object -Last 8 | Out-String)"
    }
    return $output
}

function Read-Audit([string] $ProjectName, [string] $ProjectFile, [string] $Kind) {
    $path = Join-Path $reports "$ProjectName.$Kind.json"
    $output = Invoke-Dotnet -Arguments @('package', 'list', '--project', $ProjectFile,
        '--include-transitive', "--$Kind", '--format', 'json', '--no-restore') -OutputPath $path
    $report = ($output -join "`n") | ConvertFrom-Json
    if ($report.version -ne 1 -or @($report.projects).Count -ne 1) {
        throw "Unexpected NuGet $Kind report shape for $ProjectName."
    }
    foreach ($project in $report.projects) {
        if (-not $project.PSObject.Properties['frameworks']) { continue }
        foreach ($framework in @($project.frameworks)) {
            foreach ($package in @($framework.topLevelPackages) + @($framework.transitivePackages)) {
                if ($null -eq $package) { continue }
                $findings.Add([pscustomobject]@{
                    Project = $ProjectName
                    Framework = $framework.framework
                    Kind = $Kind
                    Package = $package.id
                    Version = $package.resolvedVersion
                    Detail = if ($Kind -eq 'vulnerable') { $package.vulnerabilities } else { $package.deprecationReasons }
                })
            }
        }
    }
}

foreach ($name in $ProjectNames) {
    if ($name -notmatch '^ReactiveUI\.Primitives\.OccasionallyConnected(?:\.[A-Za-z0-9]+)*$') {
        throw "Unexpected package project name: $name"
    }
    $projectFile = Join-Path (Join-Path $src $name) "$name.csproj"
    if (-not (Test-Path -LiteralPath $projectFile)) { throw "Missing project: $projectFile" }
    Write-Host "Packing $name"
    # Audit the feature packages without restoring unrelated mobile targets from a referenced core project.
    Invoke-Dotnet -Arguments @('pack', $projectFile, '-c', 'Release', '-o', $drop,
        "-p:MinVerVersionOverride=$Version", '-p:ContinuousIntegrationBuild=true',
        '--disable-build-servers', '-m:1', '-p:AndroidPrimitivesTargetFrameworks=',
        '-p:ApplePrimitivesTargetFrameworks=')
    $packageFile = Join-Path $drop "$name.$Version.nupkg"
    if (-not (Test-Path -LiteralPath $packageFile)) { throw "Expected package not found: $packageFile" }

    $assetsPath = Join-Path (Join-Path (Join-Path $src $name) 'obj') 'project.assets.json'
    $assets = Get-Content -LiteralPath $assetsPath -Raw | ConvertFrom-Json
    $packageFolders = @($assets.packageFolders.PSObject.Properties.Name)
    if ($packageFolders.Count -eq 0) { throw "No NuGet package folder in $assetsPath" }
    foreach ($library in $assets.libraries.PSObject.Properties) {
        if ($library.Value.type -ne 'package') { continue }
        $id, $packageVersion = $library.Name -split '/', 2
        # SDK-provided .NET Framework reference assemblies are private build inputs, not shipped dependencies.
        if ($id.StartsWith('Microsoft.NETFramework.ReferenceAssemblies', [StringComparison]::OrdinalIgnoreCase)) {
            continue
        }
        $key = "$id/$packageVersion"
        if ($licenses.ContainsKey($key)) { continue }
        $nuspec = $null
        foreach ($folder in $packageFolders) {
            $lowerId = $id.ToLowerInvariant()
            $candidate = [IO.Path]::Combine($folder, $lowerId,
                $packageVersion.ToLowerInvariant(), "$lowerId.nuspec")
            if (Test-Path -LiteralPath $candidate) { $nuspec = $candidate; break }
        }
        if (-not $nuspec) { throw "NuGet metadata missing for $key" }
        [xml] $metadata = Get-Content -LiteralPath $nuspec -Raw
        $license = $metadata.SelectSingleNode('/*[local-name()="package"]/*[local-name()="metadata"]/*[local-name()="license"]')
        if ($null -eq $license -or [string]::IsNullOrWhiteSpace($license.InnerText)) {
            throw "License metadata missing for $key ($nuspec)"
        }
        if ($null -eq $license.Attributes['type']) {
            throw "License type metadata missing for $key ($nuspec)"
        }
        $licenseType = $license.Attributes['type'].Value
        if ($licenseType -eq 'file') {
            $licenseFile = Join-Path (Split-Path -Parent $nuspec) $license.InnerText
            if (-not (Test-Path -LiteralPath $licenseFile)) { throw "License file missing for $key" }
        }
        $licenses[$key] = [pscustomobject]@{
            Package = $id
            Version = $packageVersion
            LicenseType = $licenseType
            License = $license.InnerText
        }
    }
    Read-Audit $name $projectFile 'vulnerable'
    Read-Audit $name $projectFile 'deprecated'
}

if ($licenses.Count -eq 0) { throw 'No NuGet dependencies were found in the package assets.' }
$licenses.Values | Sort-Object Package, Version |
    ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $reports 'licenses.json') -Encoding utf8
ConvertTo-Json -InputObject $findings.ToArray() -Depth 8 |
    Set-Content -LiteralPath (Join-Path $reports 'audit-findings.json') -Encoding utf8

Write-Host 'Installing pinned Microsoft SBOM tool'
Invoke-Dotnet -Arguments @('tool', 'install', 'Microsoft.Sbom.DotNetTool', '--version', '4.1.5',
    '--tool-path', $toolDir)
$tool = Join-Path $toolDir "sbom-tool$(if ($IsWindows) { '.exe' })"
if (-not (Test-Path -LiteralPath $tool)) { throw "SBOM tool not found: $tool" }
$manifest = Join-Path $ArtifactsPath 'sbom'
New-Item -ItemType Directory -Force -Path $manifest | Out-Null
& $tool generate -b $drop -bc $src -m $manifest -pn 'ReactiveUI.Primitives.OccasionallyConnected' `
    -pv $Version -ps 'ReactiveUI' -nsb 'https://github.com/reactiveui/Primitives' -pm true
if ($LASTEXITCODE -ne 0) { throw "SBOM generation failed (exit $LASTEXITCODE)." }
$sbom = @(Get-ChildItem -LiteralPath $manifest -Recurse -Filter 'manifest.spdx.json')
if ($sbom.Count -ne 1) { throw "Expected one SPDX SBOM; found $($sbom.Count)." }
$document = Get-Content -LiteralPath $sbom[0].FullName -Raw | ConvertFrom-Json
if ($document.spdxVersion -ne 'SPDX-2.2' -or @($document.packages).Count -le $ProjectNames.Count) {
    throw 'The SPDX SBOM does not contain the packed packages and their dependencies.'
}
foreach ($name in $ProjectNames) {
    $matched = @($document.packages | Where-Object {
        $_.name -eq $name -and $_.versionInfo -eq $Version
    })
    if ($matched.Count -eq 0) { throw "Packed package missing from SPDX SBOM: $name $Version" }
}
& $tool validate -b $drop -m (Join-Path $manifest '_manifest') -n `
    -o (Join-Path $reports 'sbom-validation.json') -mi 'SPDX:2.2'
if ($LASTEXITCODE -ne 0) { throw "SBOM validation failed (exit $LASTEXITCODE)." }

# Keep a compact, machine-readable record tying the packages and SPDX document to
# the source revision and the tools that produced them.
$sourceCommit = (& git -C $repoRoot rev-parse HEAD 2>$null | Select-Object -First 1).Trim()
if ($LASTEXITCODE -ne 0 -or $sourceCommit -notmatch '^[0-9a-fA-F]{40,64}$') {
    throw 'Could not determine the source commit SHA for the provenance record.'
}
$dotnetVersion = (& dotnet --version 2>&1 | Select-Object -First 1).ToString().Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($dotnetVersion)) {
    throw 'Could not determine the .NET SDK version for the provenance record.'
}
$workingTreeChanges = @(& git -C $repoRoot status --porcelain --untracked-files=all)
if ($LASTEXITCODE -ne 0) { throw 'Could not determine the source working-tree state.' }
$packageEvidence = @(
    foreach ($name in $ProjectNames) {
        $path = Join-Path $drop "$name.$Version.nupkg"
        [pscustomobject]@{
            name = "$name.$Version.nupkg"
            sha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
        }
    }
) | Sort-Object name
$provenance = [ordered]@{
    schemaVersion = 1
    sourceCommit = $sourceCommit.ToLowerInvariant()
    workingTreeDirty = $workingTreeChanges.Count -gt 0
    runTimestampUtc = [DateTimeOffset]::UtcNow.ToString('O', [Globalization.CultureInfo]::InvariantCulture)
    tools = [ordered]@{
        powershell = $PSVersionTable.PSVersion.ToString()
        dotnetSdk = $dotnetVersion
        sbomTool = 'Microsoft.Sbom.DotNetTool 4.1.5'
        operatingSystem = [Runtime.InteropServices.RuntimeInformation]::OSDescription
    }
    packages = @($packageEvidence)
    spdxManifest = [ordered]@{
        name = [IO.Path]::GetRelativePath($ArtifactsPath, $sbom[0].FullName).Replace('\', '/')
        sha256 = (Get-FileHash -LiteralPath $sbom[0].FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    }
}
ConvertTo-Json -InputObject $provenance -Depth 6 |
    Set-Content -LiteralPath (Join-Path $reports 'provenance.json') -Encoding utf8

if ($findings.Count -gt 0) {
    $findings | Format-Table Project, Framework, Kind, Package, Version | Out-String | Write-Host
    throw "Supply-chain audit found $($findings.Count) vulnerable or deprecated dependency entries."
}
Write-Host "Supply-chain gate passed: $($ProjectNames.Count) packages; $($licenses.Count) dependencies; SPDX SBOM validated."
