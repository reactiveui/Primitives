// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace OccasionallyConnected.Ci;

/// <summary>
/// Builds and tests the OccasionallyConnected suites and checks their coverage. Supports two
/// invocation shapes:
/// <list type="bullet">
/// <item><description>
/// A full CI invocation (<c>--framework</c>, <c>--run-id</c>, <c>--run-attempt</c>, <c>--runner-os</c>)
/// that verifies the exact 15 OccasionallyConnected test suites exist, builds and runs each of them for
/// the requested target framework into a fresh coverage output path, then validates/merges the resulting
/// Cobertura reports.
/// </description></item>
/// <item><description>
/// A standalone invocation (<c>--report-path</c> and <c>--package-name</c>, both repeatable) that only
/// performs the validation/merge/threshold step against already-produced Cobertura reports.
/// </description></item>
/// </list>
/// Every gate is strict: it throws (and returns a non-zero exit code) instead of skipping on any failure.
/// Uses only the BCL; never shells out to PowerShell.
/// </summary>
public static partial class Coverage
{
    private const double CoreRuntimeLineCoverageThreshold = 0.95;
    private const double CoreRuntimeBranchCoverageThreshold = 0.90;
    private const string OccasionallyConnectedPackageName = "ReactiveUI.Primitives.OccasionallyConnected";
    private const string ReactivePackageName = "ReactiveUI.Primitives.OccasionallyConnected.Reactive";

    private static readonly string[] CoreRuntimePackageNames =
    [
        "ReactiveUI.Primitives.OccasionallyConnected.Core",
        OccasionallyConnectedPackageName,
        "ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection",
        "ReactiveUI.Primitives.OccasionallyConnected.Hosting",
        ReactivePackageName,
        "ReactiveUI.Primitives.OccasionallyConnected.Server",
    ];

    private static readonly string[] AllowedFrameworks = ["net8.0", "net9.0", "net10.0", "net11.0"];

    private static readonly string[] ExpectedTestProjects =
    [
        "ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Core.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Examples.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Hosting.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Reactive.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Server.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests",
        "ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets.Tests",
    ];

    private static readonly string[] PackageNamesForCiInvocation =
    [
        "ReactiveUI.Primitives.OccasionallyConnected.Core",
        "ReactiveUI.Primitives.OccasionallyConnected",
        "ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection",
        "ReactiveUI.Primitives.OccasionallyConnected.Hosting",
        ReactivePackageName,
        "ReactiveUI.Primitives.OccasionallyConnected.Server",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb",
        "ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite",
        "ReactiveUI.Primitives.OccasionallyConnected.Transport.Http",
        "ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets",
    ];

    private static readonly string[] GeneratedJsonSerializerSegments =
    [
        "obj",
        "Generated",
        "System.Text.Json.SourceGeneration",
        "System.Text.Json.SourceGeneration.JsonSourceGenerator",
    ];

    [GeneratedRegex(@"^(?<percent>\d+(?:\.\d+)?)%\s+\((?<covered>\d+)/(?<total>\d+)\)$")]
    private static partial Regex ConditionCoverageRegex();

    /// <summary>
    /// Runs the OccasionallyConnected coverage gate.
    /// </summary>
    /// <param name="args">
    /// Either <c>--framework</c>/<c>--run-id</c>/<c>--run-attempt</c>/<c>--runner-os</c> for a full CI
    /// invocation that builds, tests, and validates all 15 suites, or one-or-more repeated
    /// <c>--report-path</c>/<c>--package-name</c> pairs for standalone validation of existing reports.
    /// </param>
    /// <returns>0 on success; 1 when a gate step fails.</returns>
    public static int Run(string[] args)
    {
        try
        {
            var options = ParseArguments(args);
            if (options.Mode == InvocationMode.Ci)
            {
                RunFullCiInvocation(options);
            }
            else
            {
                ValidateAndMergeCoverage([.. options.ReportPaths], [.. options.PackageNames]);
            }

            return 0;
        }
        catch (CoverageGateException ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }
    }

    private static void RunFullCiInvocation(ParsedOptions options)
    {
        var repoRoot = FindRepositoryRoot();
        var src = Path.Combine(repoRoot, "src");
        var testsRoot = Path.Combine(src, "tests");

        var foundProjects = Directory.Exists(testsRoot)
            ? Directory.GetDirectories(testsRoot, "ReactiveUI.Primitives.OccasionallyConnected*.Tests")
                .Select(Path.GetFileName)
                .Cast<string>()
                .ToArray()
            : [];
        if (!new HashSet<string>(ExpectedTestProjects, StringComparer.Ordinal)
                .SetEquals(new HashSet<string>(foundProjects, StringComparer.Ordinal)))
        {
            throw new CoverageGateException("OccasionallyConnected test project list differs from the expected 15 suites.");
        }

        var coverageRoot = Path.Combine(
            repoRoot, "artifacts", "occasionally-connected",
            $"{options.RunId}-{options.RunAttempt}-{options.RunnerOs}-{options.Framework}");
        if (Directory.Exists(coverageRoot) || File.Exists(coverageRoot))
        {
            throw new CoverageGateException($"Coverage output path already exists: {coverageRoot}");
        }

        var reportPaths = new List<string>();
        foreach (var name in ExpectedTestProjects)
        {
            var projectDirectory = Path.Combine(testsRoot, name);
            var projectFile = Path.Combine(projectDirectory, $"{name}.csproj");
            if (!File.Exists(projectFile))
            {
                throw new CoverageGateException($"Missing test project: {projectFile}");
            }

            if (!TargetsFramework(projectFile, options.Framework!))
            {
                Console.WriteLine($"Skipping {name} ({options.Framework}) because the project does not target that framework.");
                continue;
            }

            Console.WriteLine($"Building {name} ({options.Framework})");
            // Analyzer compliance is checked by the package gate; coverage builds focus on compiling and running the test suites.
            var buildExitCode = RunProcess(
                "dotnet", src,
                "build", projectFile, "-c", "Release", "-f", options.Framework!, "--disable-build-servers", "-m:1",
                "-p:LangVersion=preview",
                "-p:RunAnalyzers=false",
                "-p:AndroidPrimitivesTargetFrameworks=", "-p:ApplePrimitivesTargetFrameworks=");
            if (buildExitCode != 0)
            {
                throw new CoverageGateException($"Build failed: {name}");
            }

            var testAssembly = Path.Combine(projectDirectory, "bin", "Release", options.Framework!, $"{name}.dll");
            if (!File.Exists(testAssembly))
            {
                throw new CoverageGateException($"Expected test assembly was not produced: {testAssembly}");
            }

            var results = Path.Combine(coverageRoot, name, options.Framework!);
            Console.WriteLine($"Testing {name} ({options.Framework})");
            var testExitCode = RunProcess(
                "dotnet", src,
                testAssembly, "--coverage", "--coverage-output-format", "cobertura",
                "--results-directory", results, "--progress", "off");
            if (testExitCode != 0)
            {
                throw new CoverageGateException($"Tests failed: {name}");
            }

            var reports = Directory.Exists(results)
                ? Directory.GetFiles(results, "*.cobertura.xml", SearchOption.AllDirectories)
                : [];
            if (reports.Length != 1)
            {
                throw new CoverageGateException($"Expected one fresh coverage report for {name}; found {reports.Length}.");
            }

            reportPaths.Add(reports[0]);
        }

        var packageNames = PackageNamesForCiInvocation
            .Where(name => TargetsFramework(Path.Combine(src, name, $"{name}.csproj"), options.Framework!))
            .ToArray();
        ValidateAndMergeCoverage([.. reportPaths], packageNames, includeReactiveRecompiledSources: true);
    }

    private static bool TargetsFramework(string projectFile, string framework)
    {
        try
        {
            var document = XDocument.Load(projectFile);
            var targetFrameworkValues = document
                .Descendants()
                .Where(element => element.Name.LocalName is "TargetFramework" or "TargetFrameworks")
                .Select(element => element.Value.Trim())
                .Where(value => value.Length > 0)
                .ToArray();
            if (targetFrameworkValues.Length == 0)
            {
                throw new CoverageGateException($"Test project does not declare TargetFramework or TargetFrameworks: {projectFile}");
            }

            return targetFrameworkValues.Any(value => value.Contains("$(", StringComparison.Ordinal)
                || value.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    .Contains(framework, StringComparer.Ordinal));
        }
        catch (CoverageGateException)
        {
            throw;
        }
        catch (Exception exception) when (exception is IOException or System.Xml.XmlException)
        {
            throw new CoverageGateException($"Unable to discover target frameworks for test project: {projectFile}", exception);
        }
    }

    private static int RunProcess(string fileName, string workingDirectory, params string[] arguments)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo(fileName)
            {
                WorkingDirectory = workingDirectory,
                UseShellExecute = false,
                RedirectStandardOutput = false,
                RedirectStandardError = false,
            },
        };
        foreach (var argument in arguments)
        {
            process.StartInfo.ArgumentList.Add(argument);
        }

        process.Start();
        process.WaitForExit();
        return process.ExitCode;
    }

    private static ParsedOptions ParseArguments(string[] args)
    {
        string? framework = null;
        string? runId = null;
        string? runAttempt = null;
        string? runnerOs = null;
        var reportPaths = new List<string>();
        var packageNames = new List<string>();

        for (var index = 0; index < args.Length; index++)
        {
            var name = args[index];
            string Next(string optionName)
            {
                if (index + 1 >= args.Length)
                {
                    throw new CoverageGateException($"Missing value for '{optionName}'.");
                }

                return args[++index];
            }

            switch (name)
            {
                case "--framework":
                    framework = Next(name);
                    break;
                case "--run-id":
                    runId = Next(name);
                    break;
                case "--run-attempt":
                    runAttempt = Next(name);
                    break;
                case "--runner-os":
                    runnerOs = Next(name);
                    break;
                case "--report-path":
                    reportPaths.Add(Next(name));
                    break;
                case "--package-name":
                    packageNames.Add(Next(name));
                    break;
                default:
                    throw new CoverageGateException($"Unrecognized argument: '{name}'.");
            }
        }

        var hasCiArgument = framework is not null || runId is not null || runAttempt is not null || runnerOs is not null;
        var hasStandaloneArgument = reportPaths.Count > 0 || packageNames.Count > 0;

        if (hasCiArgument && hasStandaloneArgument)
        {
            throw new CoverageGateException(
                "Cannot combine --framework/--run-id/--run-attempt/--runner-os with --report-path/--package-name arguments.");
        }

        if (hasCiArgument)
        {
            var missing = new List<string>();
            if (framework is null)
            {
                missing.Add("--framework");
            }

            if (runId is null)
            {
                missing.Add("--run-id");
            }

            if (runAttempt is null)
            {
                missing.Add("--run-attempt");
            }

            if (runnerOs is null)
            {
                missing.Add("--runner-os");
            }

            if (missing.Count > 0)
            {
                throw new CoverageGateException(
                    $"Full CI invocation requires --framework, --run-id, --run-attempt, and --runner-os; missing {string.Join(", ", missing)}.");
            }

            if (!AllowedFrameworks.Contains(framework, StringComparer.Ordinal))
            {
                throw new CoverageGateException(
                    $"Invalid --framework '{framework}'. Expected one of: {string.Join(", ", AllowedFrameworks)}.");
            }

            if (!long.TryParse(runId, NumberStyles.None, CultureInfo.InvariantCulture, out _) ||
                !int.TryParse(runAttempt, NumberStyles.None, CultureInfo.InvariantCulture, out _) ||
                runnerOs is not ("Linux" or "Windows" or "macOS"))
            {
                throw new CoverageGateException("Invalid CI run id, attempt, or runner OS.");
            }

            return new ParsedOptions(InvocationMode.Ci, framework, runId, runAttempt, runnerOs, [], []);
        }

        if (hasStandaloneArgument)
        {
            if (reportPaths.Count == 0 || packageNames.Count == 0)
            {
                throw new CoverageGateException(
                    "Standalone coverage validation requires at least one --report-path and at least one --package-name.");
            }

            return new ParsedOptions(InvocationMode.Standalone, null, null, null, null, reportPaths, packageNames);
        }

        throw new CoverageGateException(
            "No recognized arguments. Provide --framework/--run-id/--run-attempt/--runner-os for a full CI invocation, " +
            "or --report-path/--package-name (each repeatable) for standalone coverage report validation.");
    }

    private static string FindRepositoryRoot()
    {
        foreach (var start in new[] { Environment.CurrentDirectory, AppContext.BaseDirectory })
        {
            for (var directory = new DirectoryInfo(start); directory is not null; directory = directory.Parent)
            {
                if (File.Exists(Path.Combine(directory.FullName, "src", "Directory.Build.props")) &&
                    Directory.Exists(Path.Combine(directory.FullName, "src")) &&
                    Directory.Exists(Path.Combine(directory.FullName, "tools")))
                {
                    return directory.FullName;
                }
            }
        }

        throw new DirectoryNotFoundException("Could not find the OccasionallyConnected repository root.");
    }

    private static void ValidateAndMergeCoverage(
        string[] reportPaths,
        string[] packageNames,
        bool includeReactiveRecompiledSources = false)
    {
        if (reportPaths.Length == 0)
        {
            throw new CoverageGateException("At least one coverage report path is required.");
        }

        if (packageNames.Length == 0)
        {
            throw new CoverageGateException("At least one package name is required.");
        }

        var reports = reportPaths.Select(LoadReport).ToArray();

        foreach (var packageName in packageNames)
        {
            if (includeReactiveRecompiledSources && string.Equals(packageName, ReactivePackageName, StringComparison.Ordinal))
            {
                ValidateRecompiledReactivePackage(reports);
            }
            else
            {
                ValidatePackage(packageName, reports);
            }
        }
    }

    private static ParsedReport LoadReport(string path)
    {
        if (!File.Exists(path))
        {
            throw new CoverageGateException($"Coverage report '{path}' does not exist.");
        }

        var reportText = File.ReadAllText(path);
        if (string.IsNullOrWhiteSpace(reportText))
        {
            throw new CoverageGateException($"Coverage report '{path}' is empty.");
        }

        XDocument document;
        try
        {
            document = XDocument.Parse(reportText, LoadOptions.None);
        }
        catch (Exception ex) when (ex is System.Xml.XmlException or ArgumentException)
        {
            throw new CoverageGateException($"Coverage report '{path}' is not valid XML. {ex.Message}");
        }

        var root = document.Root;
        if (root is null || root.Name.LocalName != "coverage" || root.Element("packages") is null)
        {
            throw new CoverageGateException($"Coverage report '{path}' is missing Cobertura coverage/packages metadata.");
        }

        return new ParsedReport(path, root);
    }

    private static void ValidatePackage(string packageName, ParsedReport[] reports)
    {
        var handwrittenByLine = new Dictionary<string, LineEntry>(StringComparer.OrdinalIgnoreCase);
        var generatedByLine = new Dictionary<string, LineEntry>(StringComparer.OrdinalIgnoreCase);
        var classKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var matchingPackages = 0;
        var packageLineRate = 0.0;
        var packageBranchRate = 0.0;

        foreach (var reportInfo in reports)
        {
            var packagesElement = reportInfo.Root.Element("packages")!;
            var packages = packagesElement.Elements("package")
                .Where(p => string.Equals((string?)p.Attribute("name"), packageName, StringComparison.Ordinal) ||
                            string.Equals((string?)p.Attribute("name"), $"{packageName}.dll", StringComparison.Ordinal))
                .ToArray();

            if (packages.Length > 1 || (reports.Length == 1 && packages.Length != 1))
            {
                throw new CoverageGateException(
                    $"Expected exactly one coverage entry for '{packageName}' in '{reportInfo.Path}'; found {packages.Length}.");
            }

            if (packages.Length == 0)
            {
                continue;
            }

            matchingPackages++;
            var package = packages[0];
            var packageContext = $"package '{packageName}' in '{reportInfo.Path}'";
            packageLineRate = GetRequiredDoubleAttribute(package, "line-rate", packageContext);
            packageBranchRate = GetRequiredDoubleAttribute(package, "branch-rate", packageContext);
            if (packageLineRate is < 0 or > 1 || packageBranchRate is < 0 or > 1)
            {
                throw new CoverageGateException($"Coverage report has invalid rate metadata on {packageContext}.");
            }

            var classes = package.Element("classes")?.Elements("class").ToArray() ?? [];
            if (classes.Length == 0)
            {
                throw new CoverageGateException($"No classes were measured for '{packageName}' in '{reportInfo.Path}'.");
            }

            foreach (var classElement in classes)
            {
                ValidateClass(classElement, packageName, classKeys, handwrittenByLine, generatedByLine);
            }
        }

        if (matchingPackages == 0)
        {
            throw new CoverageGateException($"Expected at least one coverage entry for '{packageName}'; found 0.");
        }

        var packageTotals = reports.Length == 1
            ? $"package totals: line-rate {packageLineRate}; branch-rate {packageBranchRate}; classes {classKeys.Count}"
            : $"package totals across {matchingPackages} reports: classes {classKeys.Count}";
        WriteCoverageSummary(packageName, packageTotals, handwrittenByLine, generatedByLine);
    }

    private static void ValidateRecompiledReactivePackage(ParsedReport[] reports)
    {
        var reactiveHandwrittenByLine = new Dictionary<string, LineEntry>(StringComparer.OrdinalIgnoreCase);
        var reactiveGeneratedByLine = new Dictionary<string, LineEntry>(StringComparer.OrdinalIgnoreCase);
        var sharedHandwrittenByLine = new Dictionary<string, LineEntry>(StringComparer.OrdinalIgnoreCase);
        var sharedGeneratedByLine = new Dictionary<string, LineEntry>(StringComparer.OrdinalIgnoreCase);
        var reactiveClassKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var sharedClassKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var reactiveReportCount = 0;
        var sharedReportCount = 0;

        foreach (var reportInfo in reports)
        {
            var packages = reportInfo.Root.Element("packages")!.Elements("package")
                .Where(package =>
                {
                    var name = (string?)package.Attribute("name");
                    return string.Equals(name, ReactivePackageName, StringComparison.Ordinal) ||
                           string.Equals(name, OccasionallyConnectedPackageName, StringComparison.Ordinal) ||
                           string.Equals(name, $"{ReactivePackageName}.dll", StringComparison.Ordinal) ||
                           string.Equals(name, $"{OccasionallyConnectedPackageName}.dll", StringComparison.Ordinal);
                })
                .ToArray();

            foreach (var package in packages)
            {
                var packageName = GetRequiredAttribute(package, "name", "package");
                var isReactiveProfile = string.Equals(packageName, ReactivePackageName, StringComparison.Ordinal) ||
                                        string.Equals(packageName, $"{ReactivePackageName}.dll", StringComparison.Ordinal);
                if (isReactiveProfile)
                {
                    reactiveReportCount++;
                }
                else
                {
                    sharedReportCount++;
                }

                var packageContext = $"package '{packageName}' in '{reportInfo.Path}'";
                var packageLineRate = GetRequiredDoubleAttribute(package, "line-rate", packageContext);
                var packageBranchRate = GetRequiredDoubleAttribute(package, "branch-rate", packageContext);
                if (packageLineRate is < 0 or > 1 || packageBranchRate is < 0 or > 1)
                {
                    throw new CoverageGateException($"Coverage report has invalid rate metadata on {packageContext}.");
                }

                var classes = package.Element("classes")?.Elements("class").ToArray() ?? [];
                if (classes.Length == 0)
                {
                    throw new CoverageGateException($"No classes were measured for '{packageName}' in '{reportInfo.Path}'.");
                }

                var classKeys = isReactiveProfile ? reactiveClassKeys : sharedClassKeys;
                var handwritten = isReactiveProfile ? reactiveHandwrittenByLine : sharedHandwrittenByLine;
                var generated = isReactiveProfile ? reactiveGeneratedByLine : sharedGeneratedByLine;
                foreach (var classElement in classes)
                {
                    ValidateClass(classElement, packageName, classKeys, handwritten, generated, normalizeReactiveClassName: true);
                }
            }
        }

        if (reactiveReportCount == 0 || sharedReportCount == 0)
        {
            throw new CoverageGateException(
                $"Reactive coverage requires both recompiled and shared-source reports; found {reactiveReportCount} and {sharedReportCount}.");
        }

        MergeSharedProfileCoverage(sharedHandwrittenByLine, reactiveHandwrittenByLine);
        MergeSharedProfileCoverage(sharedGeneratedByLine, reactiveGeneratedByLine);
        var packageTotals =
            $"recompiled profile totals from {reactiveReportCount} Reactive and {sharedReportCount} shared-source reports; classes {reactiveClassKeys.Count}";
        WriteCoverageSummary(ReactivePackageName, packageTotals, reactiveHandwrittenByLine, reactiveGeneratedByLine);
    }

    private static void MergeSharedProfileCoverage(
        Dictionary<string, LineEntry> sharedProfile,
        Dictionary<string, LineEntry> recompiledProfile)
    {
        foreach (var (key, sharedLine) in sharedProfile)
        {
            if (!recompiledProfile.TryGetValue(key, out var recompiledLine))
            {
                continue;
            }

            if (sharedLine.IsBranch != recompiledLine.IsBranch ||
                sharedLine.TotalBranches != recompiledLine.TotalBranches)
            {
                throw new CoverageGateException(
                    $"Recompiled coverage profiles disagree on branch metadata for {recompiledLine.Context}.");
            }

            recompiledLine.Hits = Math.Max(recompiledLine.Hits, sharedLine.Hits);
            recompiledLine.MissedLine = recompiledLine.Hits == 0;
            recompiledLine.CoveredBranches = Math.Max(recompiledLine.CoveredBranches, sharedLine.CoveredBranches);
            recompiledLine.PartialBranch = recompiledLine.CoveredBranches != recompiledLine.TotalBranches;
        }
    }

    private static string NormalizeReactiveClassName(string className) =>
        className
            .Replace(".OccasionallyConnected.Reactive", ".OccasionallyConnected", StringComparison.Ordinal)
            .Replace("OccasionallyConnected-Reactive-", "OccasionallyConnected-", StringComparison.Ordinal);

    private static void WriteCoverageSummary(
        string packageName,
        string packageTotals,
        Dictionary<string, LineEntry> handwrittenByLine,
        Dictionary<string, LineEntry> generatedByLine)
    {
        var handwrittenLines = handwrittenByLine.Values.ToArray();
        var generatedJsonSerializerLines = generatedByLine.Values.ToArray();
        if (handwrittenLines.Length == 0)
        {
            throw new CoverageGateException($"No handwritten executable lines were measured for '{packageName}'.");
        }

        var packageSummary = Summarize([.. handwrittenLines, .. generatedJsonSerializerLines]);
        var handwritten = Summarize(handwrittenLines);
        var generatedJsonSerializer = Summarize(generatedJsonSerializerLines);
        var handwrittenLineRate = FormatRate(handwritten.CoveredLines, handwritten.TotalLines);
        var handwrittenBranchRate = FormatRate(handwritten.CoveredBranches, handwritten.TotalBranches);
        var generatedLineRate = FormatRate(generatedJsonSerializer.CoveredLines, generatedJsonSerializer.TotalLines);
        var generatedBranchRate = FormatRate(generatedJsonSerializer.CoveredBranches, generatedJsonSerializer.TotalBranches);
        var packageMeasuredCounts =
            $"lines {packageSummary.CoveredLines}/{packageSummary.TotalLines}; branches {packageSummary.CoveredBranches}/{packageSummary.TotalBranches}";
        Console.WriteLine($"{packageName} {packageTotals}; {packageMeasuredCounts}.");

        var generatedLineCounts = $"{generatedLineRate} ({generatedJsonSerializer.CoveredLines}/{generatedJsonSerializer.TotalLines})";
        var generatedBranchCounts = $"{generatedBranchRate} ({generatedJsonSerializer.CoveredBranches}/{generatedJsonSerializer.TotalBranches})";
        Console.WriteLine($"{packageName} generated JSON serializer: lines {generatedLineCounts}; branches {generatedBranchCounts}.");

        var handwrittenLineRatio = handwritten.TotalLines == 0 ? 0.0 : (double)handwritten.CoveredLines / handwritten.TotalLines;
        var handwrittenBranchRatio = handwritten.TotalBranches == 0 ? 1.0 : (double)handwritten.CoveredBranches / handwritten.TotalBranches;
        var isCoreRuntimePackage = CoreRuntimePackageNames.Contains(packageName, StringComparer.Ordinal);
        if (!isCoreRuntimePackage)
        {
            Console.WriteLine(
                $"{packageName} handwritten coverage reported without core-runtime thresholds: " +
                $"line coverage {handwrittenLineRate} ({handwritten.CoveredLines}/{handwritten.TotalLines}); " +
                $"branch coverage {handwrittenBranchRate} ({handwritten.CoveredBranches}/{handwritten.TotalBranches}).");
            return;
        }

        if (handwrittenLineRatio < CoreRuntimeLineCoverageThreshold
            || handwrittenBranchRatio < CoreRuntimeBranchCoverageThreshold)
        {
            var handwrittenLineCounts = $"{handwrittenLineRate} ({handwritten.CoveredLines}/{handwritten.TotalLines})";
            var handwrittenBranchCounts = $"{handwrittenBranchRate} ({handwritten.CoveredBranches}/{handwritten.TotalBranches})";
            var handwrittenMeasuredCounts = $"handwritten lines: {handwrittenLineCounts}; handwritten branches: {handwrittenBranchCounts}";
            var missedHandwrittenCounts =
                $"missed handwritten lines: {handwritten.MissedLines}; partial handwritten branch lines: {handwritten.PartialBranchLines}";
            throw new CoverageGateException(
                $"'{packageName}' requires at least 95% handwritten line coverage and 90% branch coverage; " +
                $"{handwrittenMeasuredCounts}; {missedHandwrittenCounts}.");
        }

        Console.WriteLine(
            $"{packageName} handwritten: at least 95% line coverage {handwrittenLineRate} ({handwritten.CoveredLines}/{handwritten.TotalLines}); " +
            $"branch coverage {handwrittenBranchRate} ({handwritten.CoveredBranches}/{handwritten.TotalBranches}).");
    }

    private static void ValidateClass(
        XElement classElement,
        string packageName,
        HashSet<string> classKeys,
        Dictionary<string, LineEntry> handwrittenByLine,
        Dictionary<string, LineEntry> generatedByLine,
        bool normalizeReactiveClassName = false)
    {
        var className = GetRequiredAttribute(classElement, "name", "class");
        var classContext = $"class '{className}' in package '{packageName}'";
        var filename = GetRequiredAttribute(classElement, "filename", classContext);
        var classLineRate = GetRequiredDoubleAttribute(classElement, "line-rate", $"class '{className}'");
        var classBranchRate = GetRequiredDoubleAttribute(classElement, "branch-rate", $"class '{className}'");
        var classLines = classElement.Element("lines")?.Elements("line").ToArray() ?? [];
        if (classLines.Length == 0)
        {
            throw new CoverageGateException($"No executable lines were measured for class '{className}' in '{packageName}'.");
        }

        var lineContext = $"class '{className}' file '{filename}'";
        var lineEntries = classLines.Select(line => GetLineCoverageEntry(line, lineContext)).ToArray();

        if (classLineRate is < 0 or > 1 || classBranchRate is < 0 or > 1)
        {
            throw new CoverageGateException($"Coverage report has invalid rate metadata on class '{className}'.");
        }

        var isGeneratedJsonSerializer = IsGeneratedJsonSerializerPath(filename, packageName);
        var classSummary = Summarize(lineEntries);
        var measuredClassLineRate = classSummary.TotalLines == 0 ? 1.0 : (double)classSummary.CoveredLines / classSummary.TotalLines;
        var measuredClassBranchRate = classSummary.TotalBranches == 0 ? 1.0 : (double)classSummary.CoveredBranches / classSummary.TotalBranches;
        if (Math.Abs(classLineRate - measuredClassLineRate) > 0.0001)
        {
            throw new CoverageGateException($"Coverage report class line-rate on '{className}' does not match its measured line entries.");
        }

        if (Math.Abs(classBranchRate - measuredClassBranchRate) > 0.0001)
        {
            throw new CoverageGateException($"Coverage report class branch-rate on '{className}' does not match its measured branch entries.");
        }

        var normalizedFilename = filename.Replace('\\', '/');
        var coverageClassName = normalizeReactiveClassName ? NormalizeReactiveClassName(className) : className;
        var classKey = $"{coverageClassName.Length}:{coverageClassName}{normalizedFilename.Length}:{normalizedFilename}";
        classKeys.Add(classKey);
        var target = isGeneratedJsonSerializer ? generatedByLine : handwrittenByLine;
        foreach (var line in lineEntries)
        {
            var key = $"{classKey}:{line.Number}";
            if (target.TryGetValue(key, out var previous))
            {
                if (previous.IsBranch != line.IsBranch || previous.TotalBranches != line.TotalBranches)
                {
                    throw new CoverageGateException(
                        $"Coverage reports disagree on branch metadata for line {line.Number} in class '{className}' file '{filename}'.");
                }

                previous.Hits = Math.Max(previous.Hits, line.Hits);
                previous.MissedLine = previous.Hits == 0;
                previous.CoveredBranches = Math.Max(previous.CoveredBranches, line.CoveredBranches);
                previous.PartialBranch = previous.CoveredBranches != previous.TotalBranches;
            }
            else
            {
                target.Add(key, line);
            }
        }
    }

    private static LineEntry GetLineCoverageEntry(XElement line, string context)
    {
        var number = GetRequiredLongAttribute(line, "number", context);
        var lineContext = $"line {number} in {context}";
        var hits = GetRequiredLongAttribute(line, "hits", lineContext);
        var branchValue = GetRequiredAttribute(line, "branch", lineContext);
        if (!bool.TryParse(branchValue, out var isBranch))
        {
            throw new CoverageGateException($"Coverage report has malformed 'branch' value '{branchValue}' on {lineContext}.");
        }

        long coveredBranches = 0;
        long totalBranches = 0;
        var partialBranch = false;

        if (isBranch)
        {
            var branchContext = $"branch {lineContext}";
            var conditionCoverage = GetRequiredAttribute(line, "condition-coverage", branchContext);
            var match = ConditionCoverageRegex().Match(conditionCoverage);
            if (!match.Success)
            {
                throw new CoverageGateException($"Coverage report has malformed branch condition-coverage '{conditionCoverage}' on {lineContext}.");
            }

            var reportedPercent = double.Parse(match.Groups["percent"].Value, NumberStyles.Float, CultureInfo.InvariantCulture);
            if (double.IsNaN(reportedPercent) || double.IsInfinity(reportedPercent) || reportedPercent < 0 || reportedPercent > 100)
            {
                throw new CoverageGateException($"Coverage report has invalid branch percentage '{conditionCoverage}' on {lineContext}.");
            }

            coveredBranches = long.Parse(match.Groups["covered"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture);
            totalBranches = long.Parse(match.Groups["total"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (totalBranches <= 0 || coveredBranches < 0 || coveredBranches > totalBranches)
            {
                throw new CoverageGateException($"Coverage report has invalid branch counts '{conditionCoverage}' on {lineContext}.");
            }

            var expectedPercent = 100.0 * coveredBranches / totalBranches;
            if (Math.Abs(reportedPercent - expectedPercent) > 0.01)
            {
                throw new CoverageGateException($"Coverage report branch percentage '{conditionCoverage}' does not match branch counts on {lineContext}.");
            }

            partialBranch = coveredBranches != totalBranches;
        }

        return new LineEntry
        {
            Number = number,
            Hits = hits,
            IsBranch = isBranch,
            CoveredBranches = coveredBranches,
            TotalBranches = totalBranches,
            MissedLine = hits == 0,
            PartialBranch = partialBranch,
            Context = context,
        };
    }

    private static CoverageSummary Summarize(IReadOnlyCollection<LineEntry> lines)
    {
        var coveredLines = lines.Count(line => !line.MissedLine);
        var totalLines = lines.Count;
        long coveredBranches = 0;
        long totalBranches = 0;
        foreach (var line in lines)
        {
            coveredBranches += line.CoveredBranches;
            totalBranches += line.TotalBranches;
        }

        return new CoverageSummary
        {
            TotalLines = totalLines,
            CoveredLines = coveredLines,
            MissedLines = totalLines - coveredLines,
            TotalBranches = totalBranches,
            CoveredBranches = coveredBranches,
            PartialBranchLines = lines.Count(line => line.PartialBranch),
        };
    }

    private static string FormatRate(long covered, long total) =>
        total == 0 ? "100%" : (covered * 100.0 / total).ToString("F2", CultureInfo.InvariantCulture) + "%";

    private static string GetRequiredAttribute(XElement element, string name, string context)
    {
        var value = (string?)element.Attribute(name);
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new CoverageGateException($"Coverage report is missing '{name}' on {context}.");
        }

        return value;
    }

    private static double GetRequiredDoubleAttribute(XElement element, string name, string context)
    {
        var value = GetRequiredAttribute(element, name, context);
        if (!double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var result))
        {
            throw new CoverageGateException($"Coverage report has malformed '{name}' value '{value}' on {context}.");
        }

        if (double.IsNaN(result) || double.IsInfinity(result))
        {
            throw new CoverageGateException($"Coverage report has non-finite '{name}' value '{value}' on {context}.");
        }

        return result;
    }

    private static long GetRequiredLongAttribute(XElement element, string name, string context)
    {
        var value = GetRequiredAttribute(element, name, context);
        if (!long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result))
        {
            throw new CoverageGateException($"Coverage report has malformed '{name}' value '{value}' on {context}.");
        }

        if (result < 0)
        {
            throw new CoverageGateException($"Coverage report has negative '{name}' value '{value}' on {context}.");
        }

        return result;
    }

    private static bool IsGeneratedJsonSerializerPath(string path, string packageName)
    {
        var rawSegments = GetPathSegments(path, normalizeTraversal: false);
        var canonicalSegments = GetPathSegments(path, normalizeTraversal: true);
        var inputLooksGenerated = ContainsSegmentSequence(rawSegments, GeneratedJsonSerializerSegments);
        var normalizedLooksGenerated = ContainsSegmentSequence(canonicalSegments, GeneratedJsonSerializerSegments);
        var recognizedShape = MatchesGeneratedJsonSerializerShape(canonicalSegments, packageName);

        if (inputLooksGenerated && !normalizedLooksGenerated)
        {
            throw new CoverageGateException($"Generated JSON serializer path '{path}' escapes recognized generated output after normalization.");
        }

        if (inputLooksGenerated && !recognizedShape)
        {
            throw new CoverageGateException($"Generated JSON serializer path '{path}' does not match recognized generated output shape for '{packageName}'.");
        }

        return recognizedShape;
    }

    private static bool MatchesGeneratedJsonSerializerShape(string[] segments, string packageName)
    {
        var generatorSegments = GeneratedJsonSerializerSegments;
        if (segments.Length < generatorSegments.Length + 2)
        {
            return false;
        }

        for (var i = 0; i <= segments.Length - generatorSegments.Length - 1; i++)
        {
            if (!string.Equals(segments[i], packageName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var generatorStart = i + 1;
            var matches = true;
            for (var j = 0; j < generatorSegments.Length; j++)
            {
                if (!string.Equals(segments[generatorStart + j], generatorSegments[j], StringComparison.OrdinalIgnoreCase))
                {
                    matches = false;
                    break;
                }
            }

            if (matches && segments.Length > generatorStart + generatorSegments.Length)
            {
                return true;
            }
        }

        return false;
    }

    private static string[] GetPathSegments(string path, bool normalizeTraversal)
    {
        var segments = new List<string>();
        foreach (var segment in path.Split(['\\', '/'], StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment == ".")
            {
                continue;
            }

            if (normalizeTraversal && segment == ".." && segments.Count > 0 && segments[^1] != "..")
            {
                segments.RemoveAt(segments.Count - 1);
                continue;
            }

            segments.Add(segment);
        }

        return [.. segments];
    }

    private static bool ContainsSegmentSequence(string[] segments, string[] sequence)
    {
        if (segments.Length < sequence.Length)
        {
            return false;
        }

        for (var i = 0; i <= segments.Length - sequence.Length; i++)
        {
            var matches = true;
            for (var j = 0; j < sequence.Length; j++)
            {
                if (!string.Equals(segments[i + j], sequence[j], StringComparison.OrdinalIgnoreCase))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
            {
                return true;
            }
        }

        return false;
    }

    private enum InvocationMode
    {
        Ci,
        Standalone,
    }

    private sealed record ParsedOptions(
        InvocationMode Mode,
        string? Framework,
        string? RunId,
        string? RunAttempt,
        string? RunnerOs,
        List<string> ReportPaths,
        List<string> PackageNames);

    private sealed class CoverageGateException : Exception
    {
        public CoverageGateException()
        {
        }

        public CoverageGateException(string message)
            : base(message)
        {
        }

        public CoverageGateException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    private sealed class LineEntry
    {
        public required long Number { get; init; }

        public long Hits { get; set; }

        public required bool IsBranch { get; init; }

        public long CoveredBranches { get; set; }

        public required long TotalBranches { get; init; }

        public bool MissedLine { get; set; }

        public bool PartialBranch { get; set; }

        public required string Context { get; init; }
    }

    private sealed class CoverageSummary
    {
        public long TotalLines { get; init; }

        public long CoveredLines { get; init; }

        public long MissedLines { get; init; }

        public long TotalBranches { get; init; }

        public long CoveredBranches { get; init; }

        public long PartialBranchLines { get; init; }
    }

    private sealed record ParsedReport(string Path, XElement Root);
}
