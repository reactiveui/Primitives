// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for the Test-OccasionallyConnectedCoverage script.</summary>
[NotInParallel]
public sealed class TestOccasionallyConnectedCoverageTests
{
    /// <summary>The package name passed to the coverage gate script.</summary>
    private const string PackageName = "ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>The environment variable used to override the script path in regression checks.</summary>
    private const string ScriptOverrideEnvironmentVariable = "OC_TEST_COVERAGE_SCRIPT";

    /// <summary>The expected message when handwritten package coverage does not exceed 98%.</summary>
    private const string HandwrittenCoverageFailure = "requires more than 98% handwritten lines and branches";

    /// <summary>The package segment used by fixture source paths.</summary>
    private const string PackagePath = @"D:\repo\src\ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>The POSIX package segment used by fixture source paths.</summary>
    private const string PosixPackagePath = "/repo/src/ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>A half coverage rate used by mixed handwritten/generated reports.</summary>
    private const string HalfRate = "0.5000";

    /// <summary>Complete coverage for a two-way branch fixture.</summary>
    private const string CompleteConditionCoverage = "100% (2/2)";

    /// <summary>Partial coverage for a two-way branch fixture.</summary>
    private const string HalfConditionCoverage = "50% (1/2)";

    /// <summary>The number of seconds allowed for each script process.</summary>
    private const int ScriptTimeoutSeconds = 30;

    /// <summary>The number of seconds allowed to clean up a timed-out script process.</summary>
    private const int ProcessCleanupTimeoutSeconds = 5;

    /// <summary>The repeated handwritten fixture class name.</summary>
    private const string HandwrittenClassName = "Sample.Handwritten";

    /// <summary>The repeated generated JSON context fixture class name.</summary>
    private const string PayloadJsonContextClassName = "Sample.PayloadJsonContext";

    /// <summary>The first handwritten fixture line.</summary>
    private const int FirstHandwrittenLine = 10;

    /// <summary>The second handwritten fixture line.</summary>
    private const int SecondHandwrittenLine = 11;

    /// <summary>The first generated fixture line.</summary>
    private const int FirstGeneratedLine = 22;

    /// <summary>The second generated fixture line.</summary>
    private const int SecondGeneratedLine = 23;

    /// <summary>The async branch fixture line.</summary>
    private const int AsyncBranchLine = 40;

    /// <summary>The total branches represented by a simple condition fixture.</summary>
    private const int ConditionBranchCount = 2;

    /// <summary>The fixture denominator for exact whole-percent threshold checks.</summary>
    private const int WholePercentTotal = 100;

    /// <summary>The exact whole-percent threshold covered count.</summary>
    private const int ExactThresholdCovered = 98;

    /// <summary>The fixture denominator for fractional-percent threshold checks.</summary>
    private const int FractionalPercentTotal = 1000;

    /// <summary>The covered count just above the strict threshold.</summary>
    private const int AboveThresholdCovered = 981;

    /// <summary>The covered count just below the strict threshold.</summary>
    private const int BelowThresholdCovered = 979;

    /// <summary>The factor that converts a ratio to a percentage.</summary>
    private const double PercentFactor = 100.0;

    /// <summary>A source-generated JSON serializer path recognized by the script.</summary>
    private const string GeneratedJsonPath =
        PackagePath
        + @"\obj\Generated\System.Text.Json.SourceGeneration\System.Text.Json.SourceGeneration.JsonSourceGenerator"
        + @"\PayloadJsonContext.Value.g.cs";

    /// <summary>A handwritten source file path used by fixture classes.</summary>
    private const string HandwrittenPath = @"D:\repo\src\ReactiveUI.Primitives.OccasionallyConnected\Serialization\JsonPayloadSerializer.cs";

    /// <summary>A handwritten source file path with a generated-looking suffix.</summary>
    private const string HandwrittenGeneratedSuffixPath = PackagePath + @"\Serialization\ManualJsonContext.g.cs";

    /// <summary>A source-generated path that traverses back into handwritten source.</summary>
    private const string TraversalGeneratedPath =
        PackagePath
        + @"\obj\Generated\System.Text.Json.SourceGeneration\System.Text.Json.SourceGeneration.JsonSourceGenerator"
        + @"\..\..\..\Serialization\JsonPayloadSerializer.cs";

    /// <summary>A generated-looking path outside the package source root.</summary>
    private const string SpoofedGeneratedJsonPath =
        @"D:\repo\src\OtherProject\obj\Generated\System.Text.Json.SourceGeneration\System.Text.Json.SourceGeneration.JsonSourceGenerator"
        + @"\PayloadJsonContext.Value.g.cs";

    /// <summary>A POSIX source-generated JSON serializer path recognized by the script.</summary>
    private static readonly string PosixGeneratedJsonPath = string.Join(
        '/',
        PosixPackagePath,
        "obj",
        "Generated",
        "System.Text.Json.SourceGeneration",
        "System.Text.Json.SourceGeneration.JsonSourceGenerator",
        "PayloadJsonContext.Value.g.cs");

    /// <summary>A POSIX source-generated path that traverses back into handwritten source.</summary>
    private static readonly string PosixTraversalGeneratedPath = string.Join(
        '/',
        PosixPackagePath,
        "obj",
        "Generated",
        "System.Text.Json.SourceGeneration",
        "System.Text.Json.SourceGeneration.JsonSourceGenerator",
        "..",
        "..",
        "..",
        "Serialization",
        "JsonPayloadSerializer.cs");

    /// <summary>Verifies generated JSON serializer lines are reported separately and do not fail the handwritten gate.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task GeneratedJsonSerializerMissesDoNotFailCompleteHandwrittenLines()
    {
        var report = CreateReport(
            HalfRate,
            HalfRate,
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLine(FirstHandwrittenLine, 1), CreateLine(SecondHandwrittenLine, 1, CompleteConditionCoverage)),
            CreateClass(PayloadJsonContextClassName, GeneratedJsonPath, "0", "0", CreateLine(FirstGeneratedLine, 0), CreateLine(SecondGeneratedLine, 0, "0% (0/2)")));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsEqualTo(0);
        await Assert.That(result.Output).Contains("package totals");
        await Assert.That(result.Output).Contains("lines 2/4");
        await Assert.That(result.Output).Contains("branches 2/4");
        await Assert.That(result.Output).Contains("generated JSON serializer");
        await Assert.That(result.Output).Contains("handwritten: more than 98% line coverage");
    }

    /// <summary>Verifies an uncovered handwritten line fails the gate.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task HandwrittenMissedLineFails()
    {
        var report = CreateReport(
            HalfRate,
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "0.5", "1", CreateLine(FirstHandwrittenLine, 1), CreateLine(SecondHandwrittenLine, 0)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains(HandwrittenCoverageFailure);
    }

    /// <summary>Verifies a .g.cs suffix alone does not mark a file as source-generated.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task GeneratedSuffixWithoutRecognizedPathFailsAsHandwritten()
    {
        var report = CreateReport(
            "0",
            "1",
            CreateClass("Sample.ManualJsonContext", HandwrittenGeneratedSuffixPath, "0", "1", CreateLine(FirstHandwrittenLine, 0)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains(HandwrittenCoverageFailure);
    }

    /// <summary>Verifies compiler async state-machine classes stay gated through their handwritten source path.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task AsyncStateMachineMappedToHandwrittenFileFailsPartialBranch()
    {
        var report = CreateReport(
            "1",
            "0.5",
            CreateClass("Sample.JsonPayloadSerializer.&lt;DeserializeAsync&gt;d__14", HandwrittenPath, "1", "0.5", CreateLine(AsyncBranchLine, 1, HalfConditionCoverage)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains(HandwrittenCoverageFailure);
    }

    /// <summary>Verifies branch lines fail closed when condition metadata is absent.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task BranchLineWithoutConditionMetadataFails()
    {
        var report = CreateReport(
            "1",
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLineWithoutConditionCoverage(FirstHandwrittenLine)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("missing 'condition-coverage'");
    }

    /// <summary>Verifies a source-generator-like class without a filename fails closed instead of being exempted.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task MissingGeneratedSourcePathFailsClosed()
    {
        var report = CreateReportWithClassWithoutFilename(
            PayloadJsonContextClassName,
            "1",
            "1",
            CreateLine(FirstHandwrittenLine, 1));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("missing 'filename'");
    }

    /// <summary>Verifies missing, empty and malformed reports fail closed.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task MissingEmptyAndMalformedReportsFailClosed()
    {
        using var directory = TestDirectory.Create();
        var missing = await RunScriptAsync(directory, report: null);
        var empty = await RunScriptAsync(directory, string.Empty, "empty.cobertura.xml");
        var malformed = await RunScriptAsync(directory, "<coverage>", "malformed.cobertura.xml");

        await Assert.That(missing.ExitCode).IsNotEqualTo(0);
        await Assert.That(missing.Output).Contains("does not exist");
        await Assert.That(empty.ExitCode).IsNotEqualTo(0);
        await Assert.That(empty.Output).Contains("is empty");
        await Assert.That(malformed.ExitCode).IsNotEqualTo(0);
        await Assert.That(malformed.Output).Contains("is not valid XML");
    }

    /// <summary>Verifies non-finite rate metadata fails closed.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task NonFiniteRatesFailClosed()
    {
        var report = CreateReport(
            "NaN",
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLine(FirstHandwrittenLine, 1)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("non-finite 'line-rate'");
    }

    /// <summary>Verifies lost branch metadata cannot hide uncovered handwritten branch rates.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task HandwrittenClassBranchRateBelowCompleteFailsWhenLineBranchMetadataIsMissing()
    {
        var report = CreateReport(
            "1",
            "0.5",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "0.5", CreateLine(FirstHandwrittenLine, 1)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("class branch-rate on 'Sample.Handwritten' does not match");
    }

    /// <summary>Verifies present branch lines cannot hide lost uncovered handwritten branch metadata.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task HandwrittenClassBranchRateBelowCompleteFailsWhenSomeBranchMetadataIsMissing()
    {
        var report = CreateReport(
            "1",
            "0.5",
            CreateClass(
                HandwrittenClassName,
                HandwrittenPath,
                "1",
                "0.5",
                CreateLine(FirstHandwrittenLine, 1, CompleteConditionCoverage),
                CreateLine(SecondHandwrittenLine, 1)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("class branch-rate on 'Sample.Handwritten' does not match");
    }

    /// <summary>Verifies handwritten class line-rate is gated even when every line entry reports hits.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task HandwrittenClassLineRateBelowCompleteFailsWhenLineHitsArePresent()
    {
        var report = CreateReport(
            "0.5",
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "0.5", "1", CreateLine(FirstHandwrittenLine, 1)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("class line-rate on 'Sample.Handwritten' does not match");
    }

    /// <summary>Verifies malformed branch attributes fail closed.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task MalformedBranchAttributeFailsClosed()
    {
        var report = CreateReport(
            "1",
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLineWithBranchAttribute(FirstHandwrittenLine, "maybe")));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("malformed 'branch'");
    }

    /// <summary>Verifies branch percentages must agree with covered and total branch counts.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task BranchPercentageMismatchFailsClosed()
    {
        var report = CreateReport(
            "1",
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLine(FirstHandwrittenLine, 1, "0% (2/2)")));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("does not match branch counts");
    }

    /// <summary>Verifies generated-looking paths outside the package source root fail closed.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task GeneratedPathOutsidePackageSourceRootFailsClosed()
    {
        var report = CreateReport(
            HalfRate,
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLine(FirstHandwrittenLine, 1)),
            CreateClass(PayloadJsonContextClassName, SpoofedGeneratedJsonPath, "0", "1", CreateLine(FirstGeneratedLine, 0)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("does not match recognized generated output shape");
    }

    /// <summary>Verifies generated path classification rejects traversal into a handwritten file.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task GeneratedPathTraversalFailsClosed()
    {
        var report = CreateReport(
            "1",
            "1",
            CreateClass(PayloadJsonContextClassName, TraversalGeneratedPath, "1", "1", CreateLine(FirstHandwrittenLine, 1)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("escapes recognized generated output");
    }

    /// <summary>Verifies POSIX generated JSON serializer paths are classified without weakening handwritten coverage.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task PosixGeneratedJsonSerializerMissesDoNotFailCompleteHandwrittenLines()
    {
        var report = CreateReport(
            HalfRate,
            HalfRate,
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLine(FirstHandwrittenLine, 1)),
            CreateClass(PayloadJsonContextClassName, PosixGeneratedJsonPath, "0", "1", CreateLine(FirstGeneratedLine, 0)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsEqualTo(0);
        await Assert.That(result.Output).Contains("generated JSON serializer");
        await Assert.That(result.Output).Contains("handwritten: more than 98% line coverage");
    }

    /// <summary>Verifies POSIX generated path traversal fails closed.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task PosixGeneratedPathTraversalFailsClosed()
    {
        var report = CreateReport(
            "1",
            "1",
            CreateClass(PayloadJsonContextClassName, PosixTraversalGeneratedPath, "1", "1", CreateLine(FirstHandwrittenLine, 1)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("escapes recognized generated output");
    }

    /// <summary>Verifies decimal parsing remains invariant under a culture that uses comma decimals.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task DecimalRatesUseInvariantCulture()
    {
        var report = CreateReport(
            HalfRate,
            HalfRate,
            CreateClass(
                HandwrittenClassName,
                HandwrittenPath,
                HalfRate,
                HalfRate,
                CreateLine(FirstHandwrittenLine, 1),
                CreateLine(SecondHandwrittenLine, 0, HalfConditionCoverage)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report, cultureName: "fr-FR");

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains(HandwrittenCoverageFailure);
    }

    /// <summary>Verifies exactly 98% handwritten line and branch coverage fails the strict threshold.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task CoverageAtExactly98PercentFails()
    {
        var report = CreateCoverageThresholdReport(WholePercentTotal, ExactThresholdCovered, WholePercentTotal, ExactThresholdCovered);

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains(HandwrittenCoverageFailure);
        await Assert.That(result.Output).Contains("98.00% (98/100)");
    }

    /// <summary>Verifies line coverage at 98% fails even when branch coverage is complete.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task LineCoverageAtExactly98PercentFailsWhenBranchesAreComplete()
    {
        var report = CreateCoverageThresholdReport(WholePercentTotal, ExactThresholdCovered, WholePercentTotal, WholePercentTotal);

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("98.00% (98/100)");
    }

    /// <summary>Verifies branch coverage at 98% fails even when line coverage is complete.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task BranchCoverageAtExactly98PercentFailsWhenLinesAreComplete()
    {
        var report = CreateCoverageThresholdReport(WholePercentTotal, WholePercentTotal, WholePercentTotal, ExactThresholdCovered);

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("handwritten branches: 98.00% (98/100)");
    }

    /// <summary>Verifies line and branch coverage just above 98% passes the package gate.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task CoverageJustAbove98PercentPasses()
    {
        var report = CreateCoverageThresholdReport(FractionalPercentTotal, AboveThresholdCovered, FractionalPercentTotal, AboveThresholdCovered);

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsEqualTo(0);
        await Assert.That(result.Output).Contains("98.10% (981/1000)");
    }

    /// <summary>Verifies line and branch coverage below 98% fails the package gate.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task CoverageBelow98PercentFails()
    {
        var report = CreateCoverageThresholdReport(FractionalPercentTotal, BelowThresholdCovered, FractionalPercentTotal, BelowThresholdCovered);

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, report);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains(HandwrittenCoverageFailure);
        await Assert.That(result.Output).Contains("97.90% (979/1000)");
    }

    /// <summary>Verifies repeated source lines are counted once and a hit in either report covers the line.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task MultipleReportsUnionDuplicateLineHits()
    {
        var first = CreateReport(
            HalfRate,
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, HalfRate, "1", CreateLine(FirstHandwrittenLine, 1), CreateLine(SecondHandwrittenLine, 0)));
        var second = CreateReport(
            HalfRate,
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, HalfRate, "1", CreateLine(FirstHandwrittenLine, 0), CreateLine(SecondHandwrittenLine, 1)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, first, additionalReport: second);

        await Assert.That(result.ExitCode).IsEqualTo(0);
        await Assert.That(result.Output).Contains("lines 2/2");
    }

    /// <summary>Verifies repeated branch totals are not added and only the highest observed covered count is used.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task MultipleReportsKeepConservativeBranchCounts()
    {
        var first = CreateReport(
            "1",
            HalfRate,
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", HalfRate, CreateLine(FirstHandwrittenLine, 1, HalfConditionCoverage)));
        var second = CreateReport(
            "1",
            HalfRate,
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", HalfRate, CreateLine(FirstHandwrittenLine, 1, HalfConditionCoverage)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, first, additionalReport: second);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("handwritten branches: 50.00% (1/2)");
    }

    /// <summary>Verifies the best observed branch count covers a repeated line without duplicating its total.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task MultipleReportsUseMaximumCoveredBranchCount()
    {
        var partial = CreateReport(
            "1",
            HalfRate,
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", HalfRate, CreateLine(FirstHandwrittenLine, 1, HalfConditionCoverage)));
        var complete = CreateReport(
            "1",
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLine(FirstHandwrittenLine, 1, CompleteConditionCoverage)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, partial, additionalReport: complete);

        await Assert.That(result.ExitCode).IsEqualTo(0);
        await Assert.That(result.Output).Contains("branches 2/2");
    }

    /// <summary>Verifies contradictory branch totals across reports fail closed.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task MultipleReportsRejectInconsistentBranchTotals()
    {
        var first = CreateReport(
            "1",
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLine(FirstHandwrittenLine, 1, CompleteConditionCoverage)));
        var second = CreateReport(
            "1",
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLine(FirstHandwrittenLine, 1, "100% (3/3)")));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, first, additionalReport: second);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("disagree on branch metadata");
    }

    /// <summary>Verifies malformed branch metadata in any report fails the aggregate gate.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task MultipleReportsFailClosedWhenOneReportLosesBranchMetadata()
    {
        var valid = CreateReport(
            "1",
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLine(FirstHandwrittenLine, 1, CompleteConditionCoverage)));
        var invalid = CreateReport(
            "1",
            "1",
            CreateClass(HandwrittenClassName, HandwrittenPath, "1", "1", CreateLineWithoutConditionCoverage(FirstHandwrittenLine)));

        using var directory = TestDirectory.Create();
        var result = await RunScriptAsync(directory, valid, additionalReport: invalid);

        await Assert.That(result.ExitCode).IsNotEqualTo(0);
        await Assert.That(result.Output).Contains("missing 'condition-coverage'");
    }

    /// <summary>Creates a Cobertura report fixture with one package.</summary>
    /// <param name="lineRate">The package line rate.</param>
    /// <param name="branchRate">The package branch rate.</param>
    /// <param name="classes">The class XML fragments.</param>
    /// <returns>The report XML.</returns>
    private static string CreateReport(string lineRate, string branchRate, params string[] classes) =>
        $"""
         <?xml version="1.0" encoding="utf-8"?>
         <coverage line-rate="{lineRate}" branch-rate="{branchRate}" version="1.0">
           <packages>
             <package name="{PackageName}" line-rate="{lineRate}" branch-rate="{branchRate}" complexity="1">
               <classes>
         {string.Concat(classes)}
               </classes>
             </package>
           </packages>
         </coverage>
         """;

    /// <summary>Creates a report with matching package and class line and branch counts.</summary>
    /// <param name="totalLines">The measured handwritten lines.</param>
    /// <param name="coveredLines">The covered handwritten lines.</param>
    /// <param name="totalBranches">The measured handwritten branches.</param>
    /// <param name="coveredBranches">The covered handwritten branches.</param>
    /// <returns>The report XML.</returns>
    private static string CreateCoverageThresholdReport(int totalLines, int coveredLines, int totalBranches, int coveredBranches)
    {
        var lineRate = FormatRatio(coveredLines, totalLines);
        var branchRate = FormatRatio(coveredBranches, totalBranches);
        var branchPercent = (PercentFactor * coveredBranches / totalBranches).ToString("0.00", System.Globalization.CultureInfo.InvariantCulture);
        var conditionCoverage = $"{branchPercent}% ({coveredBranches}/{totalBranches})";
        var lines = Enumerable.Range(1, totalLines)
            .Select(number => CreateLine(
                number,
                number <= coveredLines ? 1 : 0,
                number == 1 ? conditionCoverage : null))
            .ToArray();
        var classXml = CreateClass(HandwrittenClassName, HandwrittenPath, lineRate, branchRate, lines);
        return CreateReport(lineRate, branchRate, classXml);
    }

    /// <summary>Formats a coverage ratio using invariant decimal notation.</summary>
    /// <param name="covered">The covered item count.</param>
    /// <param name="total">The measured item count.</param>
    /// <returns>The decimal coverage ratio.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string FormatRatio(int covered, int total) =>
        ((double)covered / total).ToString("0.####", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Creates a Cobertura report fixture with a malformed class that has no filename.</summary>
    /// <param name="name">The class name.</param>
    /// <param name="lineRate">The class line rate.</param>
    /// <param name="branchRate">The class branch rate.</param>
    /// <param name="lines">The line XML fragments.</param>
    /// <returns>The report XML.</returns>
    private static string CreateReportWithClassWithoutFilename(string name, string lineRate, string branchRate, params string[] lines) =>
        $"""
         <?xml version="1.0" encoding="utf-8"?>
         <coverage line-rate="{lineRate}" branch-rate="{branchRate}" version="1.0">
           <packages>
             <package name="{PackageName}" line-rate="{lineRate}" branch-rate="{branchRate}" complexity="1">
               <classes>
                 <class name="{name}" line-rate="{lineRate}" branch-rate="{branchRate}" complexity="1">
                   <methods />
                   <lines>
         {string.Concat(lines)}
                   </lines>
                 </class>
               </classes>
             </package>
           </packages>
         </coverage>
         """;

    /// <summary>Creates a Cobertura class XML fragment.</summary>
    /// <param name="name">The class name.</param>
    /// <param name="filename">The source filename.</param>
    /// <param name="lineRate">The class line rate.</param>
    /// <param name="branchRate">The class branch rate.</param>
    /// <param name="lines">The line XML fragments.</param>
    /// <returns>The class XML.</returns>
    private static string CreateClass(string name, string filename, string lineRate, string branchRate, params string[] lines) =>
        $"""
                 <class name="{name}" filename="{SecurityElement.Escape(filename)}" line-rate="{lineRate}" branch-rate="{branchRate}" complexity="1">
                   <methods />
                   <lines>
         {string.Concat(lines)}
                   </lines>
                 </class>
         """;

    /// <summary>Creates a Cobertura line XML fragment.</summary>
    /// <param name="number">The source line number.</param>
    /// <param name="hits">The hit count.</param>
    /// <param name="conditionCoverage">The optional branch condition coverage.</param>
    /// <returns>The line XML.</returns>
    private static string CreateLine(int number, int hits, string? conditionCoverage = null) =>
        conditionCoverage is null
            ? $"""
                         <line number="{number}" hits="{hits}" branch="false" />

         """
            : $"""
                         <line number="{number}" hits="{hits}" branch="true" condition-coverage="{conditionCoverage}">
                           <conditions>
                             <condition number="0" type="jump" coverage="{conditionCoverage}" />
                           </conditions>
                         </line>

         """;

    /// <summary>Creates a malformed Cobertura branch line without condition coverage.</summary>
    /// <param name="number">The source line number.</param>
    /// <returns>The line XML.</returns>
    private static string CreateLineWithoutConditionCoverage(int number) =>
        $"""
                       <line number="{number}" hits="1" branch="true" />

         """;

    /// <summary>Creates a line with a custom branch attribute value.</summary>
    /// <param name="number">The source line number.</param>
    /// <param name="branch">The branch attribute value.</param>
    /// <returns>The line XML.</returns>
    private static string CreateLineWithBranchAttribute(int number, string branch) =>
        $"""
                       <line number="{number}" hits="1" branch="{branch}" condition-coverage="100% ({ConditionBranchCount}/{ConditionBranchCount})" />

         """;

    /// <summary>Runs the coverage gate script against a report fixture.</summary>
    /// <param name="directory">The temporary test directory.</param>
    /// <param name="report">The optional report content.</param>
    /// <param name="reportName">The report file name.</param>
    /// <param name="cultureName">The optional culture name used by the script process.</param>
    /// <param name="additionalReport">An optional second report to aggregate.</param>
    /// <returns>The script result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when PowerShell cannot be started.</exception>
    /// <exception cref="TimeoutException">Thrown when the script process does not exit in time.</exception>
    private static async Task<ScriptResult> RunScriptAsync(
        TestDirectory directory,
        string? report,
        string reportName = "coverage.cobertura.xml",
        string? cultureName = null,
        string? additionalReport = null)
    {
        var reportPath = Path.Combine(directory.Path, reportName);
        if (report is not null)
        {
            await File.WriteAllTextAsync(reportPath, report);
        }

        var additionalReportPath = additionalReport is null ? null : Path.Combine(directory.Path, "additional.cobertura.xml");
        if (additionalReportPath is not null)
        {
            await File.WriteAllTextAsync(additionalReportPath, additionalReport);
        }

        var startInfo = new ProcessStartInfo { FileName = "pwsh", RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        if (cultureName is not null || additionalReportPath is not null)
        {
            if (cultureName is not null)
            {
                startInfo.Environment["OC_CULTURE"] = cultureName;
            }

            startInfo.Environment["OC_SCRIPT"] = FindScriptPath();
            startInfo.Environment["OC_REPORT"] = reportPath;
            startInfo.Environment["OC_PACKAGE"] = PackageName;
            if (additionalReportPath is not null)
            {
                startInfo.Environment["OC_REPORT2"] = additionalReportPath;
            }
        }

        startInfo.ArgumentList.Add("-NoLogo");
        startInfo.ArgumentList.Add("-NoProfile");
        startInfo.ArgumentList.Add("-ExecutionPolicy");
        startInfo.ArgumentList.Add("Bypass");
        AddScriptArguments(startInfo, reportPath, cultureName, additionalReportPath);

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Could not start PowerShell.");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(ScriptTimeoutSeconds));
        var outputTask = ReadToEndAsync(process.StandardOutput);
        var errorTask = ReadToEndAsync(process.StandardError);
        int exitCode;

        try
        {
            exitCode = await WaitForScriptExitAsync(process, timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            await StopAndDrainScriptProcessAsync(process, outputTask, errorTask).ConfigureAwait(false);
            throw new TimeoutException("The coverage gate script did not finish before the test timeout.", ex);
        }

        var output = await outputTask.ConfigureAwait(false);
        var error = await errorTask.ConfigureAwait(false);

        return new(exitCode, output + error);
    }

    /// <summary>Waits for the script process and returns its exit code.</summary>
    /// <param name="process">The owned script process.</param>
    /// <param name="cancellationToken">The cancellation token used for the timeout.</param>
    /// <returns>The script process exit code.</returns>
    /// <exception cref="OperationCanceledException">Thrown when the process wait is canceled.</exception>
    private static async Task<int> WaitForScriptExitAsync(Process process, CancellationToken cancellationToken)
    {
#if NET11_0_OR_GREATER
        var exitStatus = await process.WaitForExitStatusAsync(cancellationToken).ConfigureAwait(false);
        if (exitStatus.Canceled)
        {
            throw new OperationCanceledException(cancellationToken);
        }

        return exitStatus.Signal is null ? exitStatus.ExitCode : -1;
#else
        await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
        return process.ExitCode;
#endif
    }

    /// <summary>Stops an owned script process after timeout and observes redirected output drains.</summary>
    /// <param name="process">The owned script process.</param>
    /// <param name="outputTask">The standard output drain task.</param>
    /// <param name="errorTask">The standard error drain task.</param>
    /// <returns>A task that represents the asynchronous cleanup.</returns>
    private static async Task StopAndDrainScriptProcessAsync(
        Process process,
        Task<string> outputTask,
        Task<string> errorTask)
    {
        var cleanupTimeout = TimeSpan.FromSeconds(ProcessCleanupTimeoutSeconds);
        if (!process.HasExited)
        {
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(CancellationToken.None).WaitAsync(cleanupTimeout, CancellationToken.None).ConfigureAwait(false);
        }

        _ = await outputTask.WaitAsync(cleanupTimeout, CancellationToken.None).ConfigureAwait(false);
        _ = await errorTask.WaitAsync(cleanupTimeout, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Reads a redirected text stream without linking it to the process timeout.</summary>
    /// <param name="reader">The text reader to drain.</param>
    /// <returns>The drained text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<string> ReadToEndAsync(TextReader reader) =>
        reader.ReadToEndAsync();

    /// <summary>Adds PowerShell arguments for script invocation.</summary>
    /// <param name="startInfo">The process start information.</param>
    /// <param name="reportPath">The report path.</param>
    /// <param name="cultureName">The optional culture name.</param>
    /// <param name="additionalReportPath">The optional second report path.</param>
    private static void AddScriptArguments(ProcessStartInfo startInfo, string reportPath, string? cultureName, string? additionalReportPath)
    {
        if (cultureName is null && additionalReportPath is null)
        {
            startInfo.ArgumentList.Add("-File");
            startInfo.ArgumentList.Add(FindScriptPath());
            startInfo.ArgumentList.Add("-ReportPath");
            startInfo.ArgumentList.Add(reportPath);
            startInfo.ArgumentList.Add("-PackageNames");
            startInfo.ArgumentList.Add(PackageName);
            return;
        }

        startInfo.ArgumentList.Add("-Command");
        var cultureSetup = cultureName is null
            ? string.Empty
            : "[System.Threading.Thread]::CurrentThread.CurrentCulture = [System.Globalization.CultureInfo]::GetCultureInfo($env:OC_CULTURE); "
              + "[System.Threading.Thread]::CurrentThread.CurrentUICulture = [System.Globalization.CultureInfo]::GetCultureInfo($env:OC_CULTURE); ";
        var reportArgument = additionalReportPath is null ? "$env:OC_REPORT" : "@($env:OC_REPORT, $env:OC_REPORT2)";
        startInfo.ArgumentList.Add($"{cultureSetup}& $env:OC_SCRIPT -ReportPath {reportArgument} -PackageNames $env:OC_PACKAGE");
    }

    /// <summary>Finds the coverage gate script from the test output directory.</summary>
    /// <returns>The script path.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the script cannot be found.</exception>
    private static string FindScriptPath()
    {
        var overridePath = Environment.GetEnvironmentVariable(ScriptOverrideEnvironmentVariable);
        if (!string.IsNullOrWhiteSpace(overridePath))
        {
            return overridePath;
        }

        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            var scriptPath = Path.Combine(directory.FullName, "tools", "Test-OccasionallyConnectedCoverage.ps1");
            if (File.Exists(scriptPath))
            {
                return scriptPath;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException("Could not locate Test-OccasionallyConnectedCoverage.ps1.");
    }

    /// <summary>Owns a temporary directory for script fixture files.</summary>
    private sealed class TestDirectory : IDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="TestDirectory"/> class.</summary>
        /// <param name="path">The directory path.</param>
        private TestDirectory(string path) => Path = path;

        /// <summary>Gets the directory path.</summary>
        public string Path { get; }

        /// <summary>Creates a temporary test directory.</summary>
        /// <returns>The created directory wrapper.</returns>
        public static TestDirectory Create()
        {
            var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"oc-coverage-script-{Guid.NewGuid():N}");
            _ = Directory.CreateDirectory(path);
            return new(path);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Directory.Delete(Path, recursive: true);
    }

    /// <summary>The result from a script process.</summary>
    /// <param name="ExitCode">The process exit code.</param>
    /// <param name="Output">The combined standard output and error.</param>
    private sealed record ScriptResult(int ExitCode, string Output);
}
