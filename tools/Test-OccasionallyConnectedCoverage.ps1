<#
.SYNOPSIS
    Requires more than 98 percent handwritten line and branch coverage for each OccasionallyConnected package.
    Reports generated JSON serializer coverage separately.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string[]] $ReportPath,

    [Parameter(Mandatory)]
    [string[]] $PackageNames
)

$ErrorActionPreference = 'Stop'

function Get-RequiredAttribute {
    param(
        [Parameter(Mandatory)]
        [System.Xml.XmlElement] $Element,

        [Parameter(Mandatory)]
        [string] $Name,

        [Parameter(Mandatory)]
        [string] $Context
    )

    $value = $Element.GetAttribute($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Coverage report is missing '$Name' on $Context."
    }

    $value
}

function Get-RequiredDoubleAttribute {
    param(
        [Parameter(Mandatory)]
        [System.Xml.XmlElement] $Element,

        [Parameter(Mandatory)]
        [string] $Name,

        [Parameter(Mandatory)]
        [string] $Context
    )

    $value = Get-RequiredAttribute -Element $Element -Name $Name -Context $Context
    $result = 0.0
    if (-not [double]::TryParse($value, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref] $result)) {
        throw "Coverage report has malformed '$Name' value '$value' on $Context."
    }

    if ([double]::IsNaN($result) -or [double]::IsInfinity($result)) {
        throw "Coverage report has non-finite '$Name' value '$value' on $Context."
    }

    $result
}

function Get-RequiredLongAttribute {
    param(
        [Parameter(Mandatory)]
        [System.Xml.XmlElement] $Element,

        [Parameter(Mandatory)]
        [string] $Name,

        [Parameter(Mandatory)]
        [string] $Context
    )

    $value = Get-RequiredAttribute -Element $Element -Name $Name -Context $Context
    $result = 0L
    if (-not [long]::TryParse($value, [System.Globalization.NumberStyles]::Integer, [System.Globalization.CultureInfo]::InvariantCulture, [ref] $result)) {
        throw "Coverage report has malformed '$Name' value '$value' on $Context."
    }

    if ($result -lt 0) {
        throw "Coverage report has negative '$Name' value '$value' on $Context."
    }

    $result
}

function Test-GeneratedJsonSerializerPath {
    param(
        [Parameter(Mandatory)]
        [string] $Path,

        [Parameter(Mandatory)]
        [string] $PackageName
    )

    $generatorSegments = @(
        'obj',
        'Generated',
        'System.Text.Json.SourceGeneration',
        'System.Text.Json.SourceGeneration.JsonSourceGenerator'
    )
    $rawSegments = Get-PathSegments -Path $Path -NormalizeTraversal:$false
    $canonicalSegments = Get-PathSegments -Path $Path -NormalizeTraversal:$true
    $inputLooksGenerated = Test-SegmentSequence -Segments $rawSegments -Sequence $generatorSegments
    $normalizedLooksGenerated = Test-SegmentSequence -Segments $canonicalSegments -Sequence $generatorSegments
    $recognizedShape = Test-GeneratedJsonSerializerShape -Segments $canonicalSegments -PackageName $PackageName -GeneratorSegments $generatorSegments

    if ($inputLooksGenerated -and -not $normalizedLooksGenerated) {
        throw "Generated JSON serializer path '$Path' escapes recognized generated output after normalization."
    }

    if ($inputLooksGenerated -and -not $recognizedShape) {
        throw "Generated JSON serializer path '$Path' does not match recognized generated output shape for '$PackageName'."
    }

    $recognizedShape
}


function Test-GeneratedJsonSerializerShape {
    param(
        [Parameter(Mandatory)]
        [string[]] $Segments,

        [Parameter(Mandatory)]
        [string] $PackageName,

        [Parameter(Mandatory)]
        [string[]] $GeneratorSegments
    )

    if ($Segments.Count -lt ($GeneratorSegments.Count + 2)) {
        return $false
    }

    for ($i = 0; $i -le $Segments.Count - $GeneratorSegments.Count - 1; $i++) {
        if (-not [string]::Equals($Segments[$i], $PackageName, [System.StringComparison]::OrdinalIgnoreCase)) {
            continue
        }

        $generatorStart = $i + 1
        $matches = $true
        for ($j = 0; $j -lt $GeneratorSegments.Count; $j++) {
            if (-not [string]::Equals($Segments[$generatorStart + $j], $GeneratorSegments[$j], [System.StringComparison]::OrdinalIgnoreCase)) {
                $matches = $false
                break
            }
        }

        if ($matches -and $Segments.Count -gt ($generatorStart + $GeneratorSegments.Count)) {
            return $true
        }
    }

    $false
}

function Get-PathSegments {
    param(
        [Parameter(Mandatory)]
        [string] $Path,

        [Parameter(Mandatory)]
        [bool] $NormalizeTraversal
    )

    $segments = [System.Collections.Generic.List[string]]::new()
    foreach ($segment in [regex]::Split($Path, '[\\/]+')) {
        if ([string]::IsNullOrWhiteSpace($segment) -or $segment -eq '.') {
            continue
        }

        if ($NormalizeTraversal -and $segment -eq '..') {
            if ($segments.Count -gt 0 -and $segments[$segments.Count - 1] -ne '..') {
                $segments.RemoveAt($segments.Count - 1)
                continue
            }
        }

        $segments.Add($segment)
    }

    $segments.ToArray()
}

function Test-SegmentSequence {
    param(
        [Parameter(Mandatory)]
        [string[]] $Segments,

        [Parameter(Mandatory)]
        [string[]] $Sequence
    )

    if ($Segments.Count -lt $Sequence.Count) {
        return $false
    }

    for ($i = 0; $i -le $Segments.Count - $Sequence.Count; $i++) {
        $matches = $true
        for ($j = 0; $j -lt $Sequence.Count; $j++) {
            if (-not [string]::Equals($Segments[$i + $j], $Sequence[$j], [System.StringComparison]::OrdinalIgnoreCase)) {
                $matches = $false
                break
            }
        }

        if ($matches) {
            return $true
        }
    }

    $false
}

function Get-LineCoverageEntry {
    param(
        [Parameter(Mandatory)]
        [System.Xml.XmlElement] $Line,

        [Parameter(Mandatory)]
        [string] $Context
    )

    $number = Get-RequiredLongAttribute -Element $Line -Name 'number' -Context $Context
    $hits = Get-RequiredLongAttribute -Element $Line -Name 'hits' -Context "line $number in $Context"
    $branchValue = Get-RequiredAttribute -Element $Line -Name 'branch' -Context "line $number in $Context"
    if ($branchValue -ne 'true' -and $branchValue -ne 'false') {
        throw "Coverage report has malformed 'branch' value '$branchValue' on line $number in $Context."
    }

    $isBranch = $branchValue -eq 'true'
    $coveredBranches = 0L
    $totalBranches = 0L
    $partialBranch = $false

    if ($isBranch) {
        $conditionCoverage = Get-RequiredAttribute -Element $Line -Name 'condition-coverage' -Context "branch line $number in $Context"
        $match = [regex]::Match($conditionCoverage, '^(?<percent>\d+(?:\.\d+)?)%\s+\((?<covered>\d+)\/(?<total>\d+)\)$')
        if (-not $match.Success) {
            throw "Coverage report has malformed branch condition-coverage '$conditionCoverage' on line $number in $Context."
        }

        $reportedPercent = [double]::Parse($match.Groups['percent'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
        if ([double]::IsNaN($reportedPercent) -or [double]::IsInfinity($reportedPercent) -or $reportedPercent -lt 0 -or $reportedPercent -gt 100) {
            throw "Coverage report has invalid branch percentage '$conditionCoverage' on line $number in $Context."
        }

        $coveredBranches = [long]::Parse($match.Groups['covered'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
        $totalBranches = [long]::Parse($match.Groups['total'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
        if ($totalBranches -le 0 -or $coveredBranches -lt 0 -or $coveredBranches -gt $totalBranches) {
            throw "Coverage report has invalid branch counts '$conditionCoverage' on line $number in $Context."
        }

        $expectedPercent = 100.0 * $coveredBranches / $totalBranches
        if ([Math]::Abs($reportedPercent - $expectedPercent) -gt 0.01) {
            throw "Coverage report branch percentage '$conditionCoverage' does not match branch counts on line $number in $Context."
        }

        $partialBranch = $coveredBranches -ne $totalBranches
    }

    [pscustomobject]@{
        Number = $number
        Hits = $hits
        IsBranch = $isBranch
        CoveredBranches = $coveredBranches
        TotalBranches = $totalBranches
        MissedLine = $hits -eq 0
        PartialBranch = $partialBranch
        Context = $Context
    }
}

function Get-CoverageSummary {
    param(
        [object[]] $Lines
    )

    $coveredLines = @($Lines | Where-Object { -not $_.MissedLine }).Count
    $totalLines = $Lines.Count
    $coveredBranches = 0L
    $totalBranches = 0L
    foreach ($line in $Lines) {
        $coveredBranches += $line.CoveredBranches
        $totalBranches += $line.TotalBranches
    }

    [pscustomobject]@{
        TotalLines = $totalLines
        CoveredLines = $coveredLines
        MissedLines = $totalLines - $coveredLines
        TotalBranches = $totalBranches
        CoveredBranches = $coveredBranches
        PartialBranchLines = @($Lines | Where-Object { $_.PartialBranch }).Count
    }
}

function Format-Rate {
    param(
        [Parameter(Mandatory)]
        [long] $Covered,

        [Parameter(Mandatory)]
        [long] $Total
    )

    if ($Total -eq 0) {
        '100%'
    }
    else {
        (($Covered / $Total) * 100).ToString('F2', [System.Globalization.CultureInfo]::InvariantCulture) + '%'
    }
}

if ($ReportPath.Count -eq 0) {
    throw 'At least one coverage report path is required.'
}

$reports = @(
    foreach ($path in $ReportPath) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "Coverage report '$path' does not exist."
        }

        $reportText = Get-Content -LiteralPath $path -Raw
        if ([string]::IsNullOrWhiteSpace($reportText)) {
            throw "Coverage report '$path' is empty."
        }

        try {
            [xml] $report = $reportText
        }
        catch {
            throw "Coverage report '$path' is not valid XML. $($_.Exception.Message)"
        }

        if ($null -eq $report.coverage -or $null -eq $report.coverage.packages) {
            throw "Coverage report '$path' is missing Cobertura coverage/packages metadata."
        }

        [pscustomobject]@{ Path = $path; Xml = $report }
    }
)

foreach ($packageName in $PackageNames) {
    $handwrittenByLine = [System.Collections.Generic.Dictionary[string, object]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $generatedByLine = [System.Collections.Generic.Dictionary[string, object]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $classKeys = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $matchingPackages = 0
    $packageLineRate = 0.0
    $packageBranchRate = 0.0

    foreach ($reportInfo in $reports) {
        $packages = @($reportInfo.Xml.coverage.packages.package | Where-Object {
            $_.name -eq $packageName -or $_.name -eq "$packageName.dll"
        })

        if ($packages.Count -gt 1 -or ($reports.Count -eq 1 -and $packages.Count -ne 1)) {
            throw "Expected exactly one coverage entry for '$packageName' in '$($reportInfo.Path)'; found $($packages.Count)."
        }

        if ($packages.Count -eq 0) {
            continue
        }

        $matchingPackages++
        $package = $packages[0]
        $packageContext = "package '$packageName' in '$($reportInfo.Path)'"
        $packageLineRate = Get-RequiredDoubleAttribute -Element $package -Name 'line-rate' -Context $packageContext
        $packageBranchRate = Get-RequiredDoubleAttribute -Element $package -Name 'branch-rate' -Context $packageContext
        if ($packageLineRate -lt 0 -or $packageLineRate -gt 1 -or $packageBranchRate -lt 0 -or $packageBranchRate -gt 1) {
            throw "Coverage report has invalid rate metadata on $packageContext."
        }

        $classes = @($package.classes.class)
        if ($classes.Count -eq 0) {
            throw "No classes were measured for '$packageName' in '$($reportInfo.Path)'."
        }

        foreach ($class in $classes) {
            $className = Get-RequiredAttribute -Element $class -Name 'name' -Context $packageContext
            $filename = Get-RequiredAttribute -Element $class -Name 'filename' -Context "class '$className' in $packageContext"
            $classLineRate = Get-RequiredDoubleAttribute -Element $class -Name 'line-rate' -Context "class '$className'"
            $classBranchRate = Get-RequiredDoubleAttribute -Element $class -Name 'branch-rate' -Context "class '$className'"
            $classLines = @($class.lines.line)
            if ($classLines.Count -eq 0) {
                throw "No executable lines were measured for class '$className' in '$packageName'."
            }

            $lineEntries = @($classLines | ForEach-Object {
                Get-LineCoverageEntry -Line $_ -Context "class '$className' file '$filename'"
            })

            if ($classLineRate -lt 0 -or $classLineRate -gt 1 -or $classBranchRate -lt 0 -or $classBranchRate -gt 1) {
                throw "Coverage report has invalid rate metadata on class '$className'."
            }

            $isGeneratedJsonSerializer = Test-GeneratedJsonSerializerPath -Path $filename -PackageName $packageName
            $classSummary = Get-CoverageSummary -Lines $lineEntries
            $measuredClassLineRate = if ($classSummary.TotalLines -eq 0) { 1.0 } else { $classSummary.CoveredLines / $classSummary.TotalLines }
            $measuredClassBranchRate = if ($classSummary.TotalBranches -eq 0) { 1.0 } else { $classSummary.CoveredBranches / $classSummary.TotalBranches }
            if ([Math]::Abs($classLineRate - $measuredClassLineRate) -gt 0.0001) {
                throw "Coverage report class line-rate on '$className' does not match its measured line entries."
            }

            if ([Math]::Abs($classBranchRate - $measuredClassBranchRate) -gt 0.0001) {
                throw "Coverage report class branch-rate on '$className' does not match its measured branch entries."
            }

            $normalizedFilename = $filename.Replace('\', '/')
            $classKey = '{0}:{1}{2}:{3}' -f $className.Length, $className, $normalizedFilename.Length, $normalizedFilename
            [void] $classKeys.Add($classKey)
            $target = if ($isGeneratedJsonSerializer) { $generatedByLine } else { $handwrittenByLine }
            foreach ($line in $lineEntries) {
                $key = '{0}:{1}' -f $classKey, $line.Number
                if ($target.ContainsKey($key)) {
                    $previous = $target[$key]
                    if ($previous.IsBranch -ne $line.IsBranch -or $previous.TotalBranches -ne $line.TotalBranches) {
                        throw "Coverage reports disagree on branch metadata for line $($line.Number) in class '$className' file '$filename'."
                    }

                    $previous.Hits = [Math]::Max($previous.Hits, $line.Hits)
                    $previous.MissedLine = $previous.Hits -eq 0
                    $previous.CoveredBranches = [Math]::Max($previous.CoveredBranches, $line.CoveredBranches)
                    $previous.PartialBranch = $previous.CoveredBranches -ne $previous.TotalBranches
                }
                else {
                    $target.Add($key, $line)
                }
            }
        }
    }

    if ($matchingPackages -eq 0) {
        throw "Expected at least one coverage entry for '$packageName'; found 0."
    }

    $handwrittenLines = @($handwrittenByLine.Values)
    $generatedJsonSerializerLines = @($generatedByLine.Values)
    if ($handwrittenLines.Count -eq 0) {
        throw "No handwritten executable lines were measured for '$packageName'."
    }

    $packageSummary = Get-CoverageSummary -Lines ($handwrittenLines + $generatedJsonSerializerLines)
    $handwritten = Get-CoverageSummary -Lines $handwrittenLines
    $generatedJsonSerializer = Get-CoverageSummary -Lines $generatedJsonSerializerLines
    $handwrittenLineRate = Format-Rate -Covered $handwritten.CoveredLines -Total $handwritten.TotalLines
    $handwrittenBranchRate = Format-Rate -Covered $handwritten.CoveredBranches -Total $handwritten.TotalBranches
    $generatedLineRate = Format-Rate -Covered $generatedJsonSerializer.CoveredLines -Total $generatedJsonSerializer.TotalLines
    $generatedBranchRate = Format-Rate -Covered $generatedJsonSerializer.CoveredBranches -Total $generatedJsonSerializer.TotalBranches

    $packageMeasuredCounts = "lines $($packageSummary.CoveredLines)/$($packageSummary.TotalLines); branches $($packageSummary.CoveredBranches)/$($packageSummary.TotalBranches)"
    if ($reports.Count -eq 1) {
        Write-Output "$packageName package totals: line-rate $packageLineRate; branch-rate $packageBranchRate; classes $($classKeys.Count); $packageMeasuredCounts."
    }
    else {
        Write-Output "$packageName package totals across $matchingPackages reports: classes $($classKeys.Count); $packageMeasuredCounts."
    }

    $generatedLineCounts = "$generatedLineRate ($($generatedJsonSerializer.CoveredLines)/$($generatedJsonSerializer.TotalLines))"
    $generatedBranchCounts = "$generatedBranchRate ($($generatedJsonSerializer.CoveredBranches)/$($generatedJsonSerializer.TotalBranches))"
    $generatedMeasuredCounts = "lines $generatedLineCounts; branches $generatedBranchCounts"
    Write-Output "$packageName generated JSON serializer: $generatedMeasuredCounts."

    $handwrittenLineRatio = if ($handwritten.TotalLines -eq 0) { 0.0 } else { $handwritten.CoveredLines / $handwritten.TotalLines }
    $handwrittenBranchRatio = if ($handwritten.TotalBranches -eq 0) { 1.0 } else { $handwritten.CoveredBranches / $handwritten.TotalBranches }
    if ($handwrittenLineRatio -le 0.98 -or $handwrittenBranchRatio -le 0.98) {
        $handwrittenLineCounts = "$handwrittenLineRate ($($handwritten.CoveredLines)/$($handwritten.TotalLines))"
        $handwrittenBranchCounts = "$handwrittenBranchRate ($($handwritten.CoveredBranches)/$($handwritten.TotalBranches))"
        $handwrittenMeasuredCounts = "handwritten lines: $handwrittenLineCounts; handwritten branches: $handwrittenBranchCounts"
        $missedHandwrittenCounts = "missed handwritten lines: $($handwritten.MissedLines); partial handwritten branch lines: $($handwritten.PartialBranchLines)"
        throw "'$packageName' requires more than 98% handwritten lines and branches; $handwrittenMeasuredCounts; $missedHandwrittenCounts."
    }

    Write-Output (
        '{0} handwritten: more than 98% line coverage {1} ({2}/{3}); branch coverage {4} ({5}/{6}).' -f
        $packageName,
        $handwrittenLineRate,
        $handwritten.CoveredLines,
        $handwritten.TotalLines,
        $handwrittenBranchRate,
        $handwritten.CoveredBranches,
        $handwritten.TotalBranches)
}
