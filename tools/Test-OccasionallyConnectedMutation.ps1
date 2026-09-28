#Requires -Version 7.0

# Runs one deliberate source mutation per durability, ordering, idempotency, and retry rule.
# The current source is copied into ignored artifacts; the repository checkout is never mutated.
param(
    [ValidateSet('All', 'Durability', 'Ordering', 'Idempotency', 'Retry')]
    [string] $Campaign = 'All'
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$artifactsRoot = Join-Path $repositoryRoot 'artifacts/oc-mutation'
$runRoot = Join-Path $artifactsRoot ([DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss-fff'))
$workspace = Join-Path $runRoot 'workspace'
$reportDirectory = Join-Path $runRoot 'reports'
New-Item -ItemType Directory -Path $workspace, $reportDirectory -Force | Out-Null

# Copy the current source, including uncommitted files, so each mutant can build in isolation.
Push-Location $repositoryRoot
try {
    $paths = @(git ls-files --cached --others --exclude-standard -- src global.json NuGet.Config nuget.config .editorconfig)
    if ($LASTEXITCODE -ne 0 -or $paths.Count -eq 0) { throw 'Could not enumerate repository source files.' }
    foreach ($path in $paths) {
        $source = Join-Path $repositoryRoot $path
        if (-not (Test-Path -LiteralPath $source -PathType Leaf)) { continue }
        $destination = Join-Path $workspace $path
        $destinationDirectory = Split-Path -Parent $destination
        New-Item -ItemType Directory -Path $destinationDirectory -Force | Out-Null
        Copy-Item -LiteralPath $source -Destination $destination
    }
}
finally {
    Pop-Location
}

$mutations = @(
    @{
        Name = 'Durability'
        File = 'src/ReactiveUI.Primitives.OccasionallyConnected/InMemoryLocalStoreAdapter.cs'
        Original = '_operations.Add(operation.OperationId, record);'
        Mutated = '_ = record;'
        TestProject = 'ReactiveUI.Primitives.OccasionallyConnected.Tests'
        TestClass = 'InMemoryLocalStoreAdapterTests'
    },
    @{
        Name = 'Ordering'
        File = 'src/ReactiveUI.Primitives.OccasionallyConnected/BatchSelectionPlanner.cs'
        Original = 'item.ClientSequence > previousSequence && item.EncodedBytes > 0'
        Mutated = 'item.ClientSequence >= previousSequence && item.EncodedBytes > 0'
        TestProject = 'ReactiveUI.Primitives.OccasionallyConnected.Tests'
        TestClass = 'BatchSelectionPlannerTests'
    },
    @{
        Name = 'Idempotency'
        File = 'src/ReactiveUI.Primitives.OccasionallyConnected.Server/ServerCommitJournalOperations.cs'
        Original = 'existing.Entry.Fingerprint.Matches(entry.Fingerprint) ? ServerCommitStatus.StaleRevision : ServerCommitStatus.IntentMismatch'
        Mutated = 'existing.Entry.Fingerprint.Matches(entry.Fingerprint) ? ServerCommitStatus.IntentMismatch : ServerCommitStatus.StaleRevision'
        TestProject = 'ReactiveUI.Primitives.OccasionallyConnected.Server.Tests'
        TestClass = 'InMemoryServerCommitJournalTests'
    },
    @{
        Name = 'Retry'
        File = 'src/ReactiveUI.Primitives.OccasionallyConnected/RetryPolicy.cs'
        Original = 'state.TransientAttemptCount >= _options.MaximumRetryAttempts'
        Mutated = 'state.TransientAttemptCount > _options.MaximumRetryAttempts'
        TestProject = 'ReactiveUI.Primitives.OccasionallyConnected.Tests'
        TestClass = 'RetryPolicyTests'
    }
)

function Invoke-LoggedCommand {
    param([string] $WorkingDirectory, [string] $LogPath, [string[]] $Arguments, [int] $TimeoutSeconds)

    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = 'dotnet'
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $startInfo.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::Start($startInfo)
    try {
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            $process.Kill($true)
            $process.WaitForExit()
            $partialOutput = if ($stdout.IsCompletedSuccessfully) { $stdout.Result } else { '' }
            $partialError = if ($stderr.IsCompletedSuccessfully) { $stderr.Result } else { '' }
            [IO.File]::WriteAllText($LogPath, $partialOutput + $partialError + "`nTimed out after $TimeoutSeconds seconds.")
            throw "dotnet command exceeded $TimeoutSeconds seconds; see $LogPath."
        }

        [IO.File]::WriteAllText($LogPath, $stdout.GetAwaiter().GetResult() + $stderr.GetAwaiter().GetResult())
        return $process.ExitCode
    }
    finally {
        $process.Dispose()
    }
}

$summary = @()
foreach ($mutation in $mutations) {
    if ($Campaign -ne 'All' -and $Campaign -ne $mutation.Name) { continue }

    $sourcePath = Join-Path $workspace $mutation.File
    $originalText = [IO.File]::ReadAllText($sourcePath)
    $first = $originalText.IndexOf($mutation.Original, [StringComparison]::Ordinal)
    if ($first -lt 0 -or $originalText.IndexOf($mutation.Original, $first + 1, [StringComparison]::Ordinal) -ge 0) {
        throw "$($mutation.Name): expected exactly one mutation target in $sourcePath."
    }

    $projectDirectory = Join-Path $workspace "src/tests/$($mutation.TestProject)"
    $projectFile = Join-Path $projectDirectory "$($mutation.TestProject).csproj"
    $assembly = Join-Path $projectDirectory "bin/Release/net10.0/$($mutation.TestProject).dll"
    $filter = "/*/*/$($mutation.TestClass)/*"
    $baselineBuildLog = Join-Path $reportDirectory "$($mutation.Name)-baseline-build.log"
    $baselineTestLog = Join-Path $reportDirectory "$($mutation.Name)-baseline-test.log"
    $mutantBuildLog = Join-Path $reportDirectory "$($mutation.Name)-mutant-build.log"
    $mutantTestLog = Join-Path $reportDirectory "$($mutation.Name)-mutant-test.log"

    $buildArguments = @('build', $projectFile, '-c', 'Release', '-f', 'net10.0', '--disable-build-servers', '-m:1', '-p:MinVerSkip=true', '-p:Version=0.1.0', '-p:AndroidPrimitivesTargetFrameworks=', '-p:ApplePrimitivesTargetFrameworks=')
    $buildCode = Invoke-LoggedCommand $workspace $baselineBuildLog $buildArguments 900
    if ($buildCode -ne 0) { throw "$($mutation.Name): baseline build failed; see $baselineBuildLog." }
    $testCode = Invoke-LoggedCommand $workspace $baselineTestLog @($assembly, '--treenode-filter', $filter, '--progress', 'off') 300
    $baselineOutput = (Get-Content -LiteralPath $baselineTestLog -Raw) -replace '\x1B\[[0-9;]*m', ''
    if ($testCode -ne 0 -or $baselineOutput -notmatch '(?im)^\s*total:\s*[1-9]\d*\s*$' -or $baselineOutput -notmatch '(?im)^\s*failed:\s*0\s*$') {
        throw "$($mutation.Name): baseline TUnit tests failed or no tests ran; see $baselineTestLog."
    }

    $mutatedText = $originalText.Remove($first, $mutation.Original.Length).Insert($first, $mutation.Mutated)
    [IO.File]::WriteAllText($sourcePath, $mutatedText)
    try {
        $buildCode = Invoke-LoggedCommand $workspace $mutantBuildLog $buildArguments 900
        if ($buildCode -ne 0) { throw "$($mutation.Name): mutant did not compile; see $mutantBuildLog." }
        $testCode = Invoke-LoggedCommand $workspace $mutantTestLog @($assembly, '--treenode-filter', $filter, '--progress', 'off') 300
        $mutantOutput = (Get-Content -LiteralPath $mutantTestLog -Raw) -replace '\x1B\[[0-9;]*m', ''
        if ($testCode -eq 0 -or $mutantOutput -notmatch '(?im)^\s*failed:\s*[1-9]\d*\s*$') {
            throw "$($mutation.Name): mutant was not killed by a TUnit assertion; see $mutantTestLog."
        }
    }
    finally {
        [IO.File]::WriteAllText($sourcePath, $originalText)
    }

    $summary += [pscustomobject]@{
        Campaign = $mutation.Name
        Source = $mutation.File
        Original = $mutation.Original
        Mutated = $mutation.Mutated
        Baseline = 'Passed'
        MutantBuild = 'Passed'
        Mutant = 'Killed'
        TestClass = $mutation.TestClass
    }
    Write-Output "$($mutation.Name): mutant killed by $($mutation.TestClass); logs: $reportDirectory"
}

$summary | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $reportDirectory 'summary.json')
