<#
.SYNOPSIS
    Rejects incomplete or missing OccasionallyConnected coverage in a fresh Cobertura report.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $ReportPath,

    [Parameter(Mandatory)]
    [string[]] $PackageNames
)

$ErrorActionPreference = 'Stop'
[xml] $report = Get-Content -LiteralPath $ReportPath -Raw

foreach ($packageName in $PackageNames) {
    $packages = @($report.coverage.packages.package | Where-Object {
        $_.name -eq $packageName -or $_.name -eq "$packageName.dll"
    })
    if ($packages.Count -ne 1) {
        throw "Expected exactly one coverage entry for '$packageName'; found $($packages.Count)."
    }

    $package = $packages[0]
    $lines = @($package.classes.class.lines.line)
    if ($lines.Count -eq 0) {
        throw "No executable lines were measured for '$packageName'."
    }

    $missedLines = @($lines | Where-Object { [long] $_.hits -eq 0 })
    $missedBranches = @($lines | Where-Object {
        $_.branch -eq 'true' -and $_.'condition-coverage' -notmatch '^100% '
    })
    if ([double] $package.'line-rate' -ne 1 -or [double] $package.'branch-rate' -ne 1 -or
        $missedLines.Count -gt 0 -or $missedBranches.Count -gt 0) {
        throw "'$packageName' requires 100% lines and branches; rates: $($package.'line-rate')/$($package.'branch-rate'); missed lines: $($missedLines.Count); partial branch lines: $($missedBranches.Count)."
    }

    Write-Output "$packageName`: 100% line and branch coverage ($($lines.Count) measured line entries)."
}
