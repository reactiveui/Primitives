// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace OccasionallyConnected.Ci;

/// <summary>
/// Packs the OccasionallyConnected
/// packages and runs the section 18 release gates against them (deterministic packs, symbol/SourceLink
/// inspection, a clean sample restore/build/run per target framework, and trim/NativeAOT publish smoke
/// tests). Uses only the BCL; never shells out to PowerShell.
/// </summary>
public static partial class Packages
{
    private static readonly string[] DependencyProjects = OccasionallyConnectedPackageSet.Dependencies;

    private static readonly string[] OccasionallyConnectedProjects = OccasionallyConnectedPackageSet.Names;

    private static readonly string[] LibraryTargetFrameworks = ["net8.0", "net9.0", "net10.0", "net11.0", "net462", "net472", "net48", "net481"];

    private static readonly string[] SkipSwitches = ["skip-determinism", "skip-sample", "skip-aot"];

    [GeneratedRegex(@"warning (IL\d{4})")]
    private static partial Regex WarningRegex();

    [GeneratedRegex(@"Platform linker|vswhere|link\.exe|clang|Desktop development with C\+\+|NETSDK1(083|084|094|183|204)")]
    private static partial Regex MissingToolchainRegex();

    [GeneratedRegex(@"^(PASS|FAIL) ")]
    private static partial Regex PassFailRegex();

    [GeneratedRegex(@"^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$")]
    private static partial Regex VersionRegex();

    /// <summary>
    /// Runs the OccasionallyConnected package release gates.
    /// </summary>
    /// <param name="args">
    /// Command-line arguments: --version, --artifacts-path, --sample-target-frameworks (comma list),
    /// --skip-determinism, --skip-sample, --skip-aot.
    /// </param>
    /// <returns>0 when every gate passed; 1 otherwise.</returns>
    public static int Run(string[] args)
    {
        var (options, flags) = ParseOptions(args);
        var repoRoot = GetRepoRoot();
        var toolsDirectory = Path.Combine(repoRoot, "tools");
        var src = Path.Combine(repoRoot, "src");

        var version = options.TryGetValue("version", out var versionOption)
            ? versionOption
            : $"0.1.0-octest.{DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture)}";
        if (!VersionRegex().IsMatch(version))
        {
            throw new ArgumentException($"Invalid package version: {version}");
        }

        var artifactsPath = Path.GetFullPath(options.TryGetValue("artifacts-path", out var artifactsOption)
            ? artifactsOption
            : Path.Combine(repoRoot, "artifacts", "oc-packages"));
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        var artifactsRoot = Path.GetFullPath(Path.Combine(repoRoot, "artifacts")) + Path.DirectorySeparatorChar;
        var repositoryPath = Path.GetFullPath(repoRoot) + Path.DirectorySeparatorChar;
        if (repoRoot.StartsWith(artifactsPath.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar, comparison) ||
            artifactsPath.Equals(repoRoot, comparison) ||
            (artifactsPath.StartsWith(repositoryPath, comparison) && !artifactsPath.StartsWith(artifactsRoot, comparison)))
        {
            throw new ArgumentException($"Package artifacts path cannot replace repository files: {artifactsPath}");
        }

        var skipDeterminism = flags.Contains("skip-determinism");
        var skipSample = flags.Contains("skip-sample");
        var skipAot = flags.Contains("skip-aot");

        var (sdkExitCode, sdkOutput) = RunProcess("dotnet", ["--version"], repoRoot);
        var sdkVersionText = sdkOutput.FirstOrDefault() ?? "";
        if (sdkExitCode != 0 || !TryParseMajorVersion(sdkVersionText, out var sdkMajor))
        {
            throw new InvalidOperationException($"Could not determine .NET SDK version: {string.Join(Environment.NewLine, sdkOutput)}");
        }

        var sampleTargetFrameworks = options.TryGetValue("sample-target-frameworks", out var sampleTfmOption)
            ? sampleTfmOption.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            : BuildDefaultSampleTargetFrameworks(sdkMajor);
        if (sampleTargetFrameworks.Length == 0 ||
            sampleTargetFrameworks.Any(tfm => !LibraryTargetFrameworks.Contains(tfm, StringComparer.Ordinal)))
        {
            throw new ArgumentException("Sample target frameworks must be a nonempty comma-separated list of supported library TFMs.");
        }

        if (Directory.Exists(artifactsPath))
        {
            Directory.Delete(artifactsPath, recursive: true);
        }

        var feed = Path.Combine(artifactsPath, "feed");
        var secondPack = Path.Combine(artifactsPath, "pack-second");
        var logs = Path.Combine(artifactsPath, "logs");
        var sample = Path.Combine(artifactsPath, "clean-sample");
        Directory.CreateDirectory(feed);
        Directory.CreateDirectory(secondPack);
        Directory.CreateDirectory(logs);

        var results = new List<GateResult>();
        var releaseFilter = CreateReleaseFilter(src, Path.Combine(artifactsPath, "release.slnf"));
        var packageFrameworks = version.Contains('-', StringComparison.Ordinal)
            ? LibraryTargetFrameworks
            : LibraryTargetFrameworks.Where(tfm => tfm != "net11.0").ToArray();

        var (commitExitCode, commitOutput) = RunProcess("git", ["-C", repoRoot, "rev-parse", "HEAD"], repoRoot);
        var commit = commitExitCode == 0 ? (commitOutput.FirstOrDefault() ?? string.Empty).Trim() : string.Empty;
        if (commit.Length is not (40 or 64) || !commit.All(Uri.IsHexDigit))
        {
            throw new InvalidOperationException($"Could not determine source commit: {string.Join(Environment.NewLine, commitOutput)}");
        }

        // The clean consumer exercises desktop targets; platform workloads are not needed to pack its local feed.
        string[] packProperties =
        [
            "-c", "Release", "-nologo", $"-p:MinVerVersionOverride={version}", "-p:ContinuousIntegrationBuild=true",
            "-p:LangVersion=preview",
            $"-p:OccasionallyConnectedPackageBuildId={Guid.NewGuid():N}",
            .. OccasionallyConnectedPackageSet.NeutralFrameworkProperties,
            "-p:RestoreForce=true",
        ];
        Console.WriteLine($"Version {version}, commit {commit}, SDK {sdkVersionText}");
        Console.WriteLine($"Artifacts: {artifactsPath}");

        RunResult BuildAndPackReleaseFilter(string prefix, string output)
        {
            var build = InvokeDotnet(
                src, logs, $"{prefix}-build-release-filter",
                ["build", releaseFilter, "--no-incremental", .. packProperties]);
            return build.ExitCode == 0
                ? InvokeDotnet(src, logs, $"{prefix}-pack-release-filter",
                    ["pack", releaseFilter, "--no-build", "-o", output, .. packProperties])
                : build;
        }

        // Rebuild the same complete graph in both rounds so compiler references cannot come from another version.
        var releasePack = BuildAndPackReleaseFilter("first", feed);
        if (releasePack.ExitCode != 0)
        {
            AddResult(results, "pack", "FAIL", $"release filter (exit {releasePack.ExitCode})");
            ShowTail(releasePack);
            PrintResultsTable(results);
            return 1;
        }

        ValidateReleasePackages(feed, version);
        var packageCount = Directory.GetFiles(feed, "*.nupkg").Length;
        AddResult(results, "pack", "PASS", $"{packageCount} packages at {version} in {feed}");

        var inspectorPath = Path.Combine(toolsDirectory, "OccasionallyConnectedPackageInspector.cs");

        void InvokeInspector(string gate, string[] arguments)
        {
            var run = InvokeDotnet(toolsDirectory, logs, $"inspect-{gate}", [.. new[] { "run", inspectorPath, "--" }, .. arguments]);
            var failed = run.Output.Where(line => line.StartsWith("FAIL ", StringComparison.Ordinal)).ToArray();
            var passed = run.Output.Where(line => line.StartsWith("PASS ", StringComparison.Ordinal)).ToArray();
            foreach (var line in failed)
            {
                WriteColored(line, ConsoleColor.Red);
            }

            if (run.ExitCode == 0 && failed.Length == 0 && passed.Length > 0)
            {
                AddResult(results, gate, "PASS", $"{passed.Length} checks passed (log: {run.Log})");
            }
            else
            {
                if (failed.Length == 0)
                {
                    ShowTail(run);
                }

                AddResult(results, gate, "FAIL", $"{failed.Length} of {failed.Length + passed.Length} checks failed (log: {run.Log})");
            }
        }

        // 2. Deterministic package comparison: rebuild the complete release graph and pack it again.
        if (skipDeterminism)
        {
            AddResult(results, "deterministic-packages", "SKIP", "skipped by --skip-determinism");
        }
        else
        {
            var secondReleasePack = BuildAndPackReleaseFilter("second", secondPack);
            if (secondReleasePack.ExitCode != 0)
            {
                AddResult(results, "deterministic-packages", "FAIL", "second release filter build or pack failed");
                ShowTail(secondReleasePack);
            }
            else
            {
                InvokeInspector("deterministic-packages", ["compare", "--left", feed, "--right", secondPack, "--version", version, "--packages", string.Join(',', OccasionallyConnectedProjects)]);
            }
        }

        // 3. Symbol packages, portable PDBs, Source Link and lib/<tfm> folders.
        var fullFrameworkPackages = OccasionallyConnectedProjects
            .Where(project => project != OccasionallyConnectedPackageSet.WebSockets
                && project is not ("ReactiveUI.Primitives.OccasionallyConnected.SignalR"
                    or "ReactiveUI.Primitives.OccasionallyConnected.Mobile"
                    or "ReactiveUI.Primitives.OccasionallyConnected.Web"
                    or "ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB"
                    or "ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb"));
        InvokeInspector(
            "symbols-sourcelink-tfms",
            ["verify", "--feed", feed, "--version", version, "--packages", string.Join(',', fullFrameworkPackages), "--tfms", string.Join(',', packageFrameworks), "--commit", commit]);
        InvokeInspector(
            "symbols-sourcelink-websockets",
            ["verify", "--feed", feed, "--version", version, "--packages",
                string.Join(',', OccasionallyConnectedPackageSet.WebSockets, "ReactiveUI.Primitives.OccasionallyConnected.SignalR", "ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb"),
                "--tfms", string.Join(',', packageFrameworks.Where(tfm => tfm.StartsWith("net", StringComparison.Ordinal) && tfm.Contains('.', StringComparison.Ordinal))), "--commit", commit]);
        InvokeInspector(
            "symbols-sourcelink-platform-compositions",
            ["verify", "--feed", feed, "--version", version, "--packages",
                "ReactiveUI.Primitives.OccasionallyConnected.Web,ReactiveUI.Primitives.OccasionallyConnected.Mobile,ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB",
                "--tfms", string.Join(',', packageFrameworks.Where(tfm => tfm is "net10.0" or "net11.0")), "--commit", commit]);

        var sampleProject = "OccasionallyConnected.PackedSample.csproj";
        var sampleProperties = new[]
        {
            "-c", "Release", "-nologo", $"-p:OccasionallyConnectedPackageVersion={version}",
            "-p:LangVersion=preview",
        };

        // 4. Clean-project install and the section 16 sample on every framework.
        if (!skipSample)
        {
            Directory.CreateDirectory(sample);
            var sampleSource = Path.Combine(repoRoot, "samples", "OccasionallyConnected.PackedSample");
            foreach (var file in Directory.EnumerateFiles(sampleSource, "*.cs").Concat(Directory.EnumerateFiles(sampleSource, "*.csproj")))
            {
                File.Copy(file, Path.Combine(sample, Path.GetFileName(file)), overwrite: true);
            }

            AddConsumerPackages(Path.Combine(sample, sampleProject));
            File.WriteAllText(Path.Combine(sample, "Directory.Build.props"), "<Project />");
            File.WriteAllText(Path.Combine(sample, "Directory.Build.targets"), "<Project />");
            File.WriteAllText(
                Path.Combine(sample, "Directory.Packages.props"),
                "<Project><PropertyGroup><ManagePackageVersionsCentrally>false</ManagePackageVersionsCentrally></PropertyGroup></Project>");
            var packagesFolder = Path.Combine(sample, ".packages");
            File.WriteAllText(Path.Combine(sample, "nuget.config"), BuildNugetConfig(packagesFolder, feed));

            var restore = InvokeDotnet(sample, logs, "sample-restore", [
                "restore", sampleProject, $"-p:OccasionallyConnectedPackageVersion={version}", "-p:LangVersion=preview",
            ]);
            if (restore.ExitCode != 0)
            {
                AddResult(results, "clean-install", "FAIL", "restore from the local feed failed");
                ShowTail(restore);
            }
            else
            {
                var installed = Directory.Exists(packagesFolder)
                    ? Directory.GetDirectories(packagesFolder)
                        .Where(dir => Path.GetFileName(dir).StartsWith("reactiveui.", StringComparison.OrdinalIgnoreCase))
                        .Select(dir => $"{Path.GetFileName(dir)}/{string.Join(",", Directory.GetDirectories(dir).Select(Path.GetFileName))}")
                        .ToArray()
                    : [];
                AddResult(results, "clean-install", "PASS", $"restored from the local feed: {string.Join("; ", installed)}");

                foreach (var tfm in sampleTargetFrameworks)
                {
                    var build = InvokeDotnet(sample, logs, $"sample-build-{tfm}", [.. new[] { "build", sampleProject, "-f", tfm, "--no-restore" }, .. sampleProperties]);
                    if (build.ExitCode != 0)
                    {
                        AddResult(results, $"sample-{tfm}", "FAIL", "build failed");
                        ShowTail(build);
                        continue;
                    }

                    var run = InvokeDotnet(sample, logs, $"sample-run-{tfm}", [.. new[] { "run", "--project", sampleProject, "-f", tfm, "--no-build" }, .. sampleProperties]);
                    var checks = run.Output.Where(line => PassFailRegex().IsMatch(line)).ToArray();
                    var summary = run.Output.LastOrDefault(line => line.StartsWith("SUMMARY ", StringComparison.Ordinal));
                    foreach (var line in run.Output.Where(l => l.StartsWith("FAIL", StringComparison.Ordinal) || l.StartsWith("FAULT", StringComparison.Ordinal)))
                    {
                        WriteColored(line, ConsoleColor.Red);
                    }

                    if (run.ExitCode == 0)
                    {
                        AddResult(results, $"sample-{tfm}", "PASS", $"{summary} (log: {run.Log})");
                    }
                    else
                    {
                        if (checks.Length == 0)
                        {
                            ShowTail(run);
                        }

                        AddResult(results, $"sample-{tfm}", "FAIL", $"exit {run.ExitCode}; {summary} (log: {run.Log})");
                    }
                }
            }
        }

        // Publishes the sample, classifies trim/AOT warnings and runs the published binary.
        void TestPublish(string gate, string[] publishProperties)
        {
            var os = OperatingSystem.IsWindows() ? "win" : OperatingSystem.IsMacOS() ? "osx" : "linux";
            var arch = RuntimeInformation.OSArchitecture.ToString().ToLowerInvariant();
            var rid = $"{os}-{arch}";
            var output = Path.Combine(artifactsPath, gate);

            // Warnings stay warnings here so each one can be attributed to its assembly below.
            // TargetFrameworks is pinned so restore does not evaluate the .NET Framework legs, which cannot use AOT.
            var arguments = new List<string>
            {
                "publish", sampleProject, "-p:TargetFrameworks=net10.0", "-f", "net10.0", "-r", rid, "-o", output,
                "-c", "Release", "-nologo", $"-p:OccasionallyConnectedPackageVersion={version}", "-p:TreatWarningsAsErrors=false",
            };
            arguments.AddRange(publishProperties);
            var publish = InvokeDotnet(sample, logs, gate, [.. arguments]);
            var warnings = publish.Output.Where(line => WarningRegex().IsMatch(line)).Distinct().OrderBy(line => line, StringComparer.Ordinal).ToArray();
            var ocWarnings = warnings.Where(line => line.Contains("ReactiveUI.", StringComparison.Ordinal)).ToArray();
            var otherWarnings = warnings.Except(ocWarnings).ToArray();
            foreach (var line in otherWarnings)
            {
                WriteColored($"third-party: {line}", ConsoleColor.Yellow);
            }

            foreach (var line in ocWarnings)
            {
                WriteColored(line, ConsoleColor.Red);
            }

            if (publish.ExitCode != 0)
            {
                var missing = publish.Output.FirstOrDefault(line => MissingToolchainRegex().IsMatch(line));
                if (missing is not null)
                {
                    AddResult(results, gate, "SKIP", $"toolchain prerequisite missing: {missing.Trim()}");
                }
                else
                {
                    AddResult(results, gate, "FAIL", $"publish failed (log: {publish.Log})");
                    ShowTail(publish);
                }

                return;
            }

            var binaryName = OperatingSystem.IsWindows() ? "OccasionallyConnected.PackedSample.exe" : "OccasionallyConnected.PackedSample";
            var binary = Path.Combine(output, binaryName);
            var (runExitCode, runOutput) = RunProcess(binary, [], output);
            File.WriteAllLines(Path.Combine(logs, $"{gate}-run.log"), runOutput, Encoding.UTF8);
            var runSummary = runOutput.LastOrDefault(line => line.StartsWith("SUMMARY ", StringComparison.Ordinal));
            foreach (var line in runOutput.Where(l => l.StartsWith("FAIL", StringComparison.Ordinal) || l.StartsWith("FAULT", StringComparison.Ordinal)))
            {
                WriteColored(line, ConsoleColor.Red);
            }

            var detail = $"{rid}; OC warnings={ocWarnings.Length}; third-party warnings={otherWarnings.Length}; run exit {runExitCode}; {runSummary}";
            AddResult(results, gate, ocWarnings.Length == 0 && runExitCode == 0 ? "PASS" : "FAIL", detail);
        }

        // 5. Trimming and NativeAOT smoke tests.
        if (skipAot || skipSample)
        {
            AddResult(results, "trim-aot", "SKIP", "skipped by --skip-aot or --skip-sample");
        }
        else
        {
            TestPublish("publish-trimmed", ["-p:PublishTrimmed=true", "--self-contained"]);
            TestPublish("publish-aot", ["-p:PublishAot=true"]);
        }

        Console.WriteLine();
        PrintResultsTable(results);
        var failedGates = results.Where(result => result.Status == "FAIL").ToArray();
        if (failedGates.Length > 0)
        {
            WriteColored($"{failedGates.Length} gate(s) failed.", ConsoleColor.Red);
            return 1;
        }

        WriteColored("All gates passed.", ConsoleColor.Green);
        return 0;
    }

    private static string[] BuildDefaultSampleTargetFrameworks(int sdkMajor)
    {
        var frameworks = new List<string> { "net8.0", "net9.0", "net10.0" };
        if (sdkMajor >= 11)
        {
            frameworks.Add("net11.0");
        }

        if (OperatingSystem.IsWindows())
        {
            frameworks.AddRange(["net462", "net472", "net48", "net481"]);
        }

        return [.. frameworks];
    }

    internal static void AddConsumerPackages(string projectPath)
    {
        var document = XDocument.Load(projectPath);
        var itemGroup = document.Root?.Elements("ItemGroup")
            .FirstOrDefault(group => group.Elements("PackageReference").Any()) ??
            throw new InvalidOperationException($"Clean sample has no package references: {projectPath}");
        foreach (var name in OccasionallyConnectedPackageSet.NewConsumerPackages)
        {
            if (document.Descendants("PackageReference")
                .Any(reference => (string?)reference.Attribute("Include") == name))
            {
                continue;
            }

            var reference = new XElement("PackageReference",
                new XAttribute("Include", name),
                new XAttribute("Version", "$(OccasionallyConnectedPackageVersion)"));
            if (name is "ReactiveUI.Primitives.OccasionallyConnected.Web"
                or "ReactiveUI.Primitives.OccasionallyConnected.Mobile"
                or "ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB")
            {
                reference.SetAttributeValue(
                    "Condition",
                    "$([MSBuild]::IsTargetFrameworkCompatible('$(TargetFramework)', 'net10.0'))");
            }
            else if (name == OccasionallyConnectedPackageSet.WebSockets
                || name is "ReactiveUI.Primitives.OccasionallyConnected.SignalR"
                    or "ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb")
            {
                reference.SetAttributeValue("Condition", "!$(TargetFramework.StartsWith('net4'))");
            }

            itemGroup.Add(reference);
        }

        document.Save(projectPath);
    }

    private static bool TryParseMajorVersion(string sdkVersionText, out int major)
    {
        var majorText = sdkVersionText.Split(['.', '-'], StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        return int.TryParse(majorText, NumberStyles.Integer, CultureInfo.InvariantCulture, out major);
    }

    private static string GetRepoRoot([CallerFilePath] string sourceFilePath = "")
    {
        var ciDirectory = Path.GetDirectoryName(sourceFilePath) ?? throw new InvalidOperationException("Unable to determine the source directory.");
        var toolsDirectory = Path.GetDirectoryName(ciDirectory) ?? throw new InvalidOperationException("Unable to determine the tools directory.");
        return Path.GetDirectoryName(toolsDirectory) ?? throw new InvalidOperationException("Unable to determine the repository root.");
    }

    private static (Dictionary<string, string> Options, HashSet<string> Flags) ParseOptions(string[] args)
    {
        var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var flags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var index = 0; index < args.Length; index++)
        {
            if (!args[index].StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Unexpected package gate argument: {args[index]}");
            }

            var name = args[index][2..];
            if (SkipSwitches.Contains(name, StringComparer.OrdinalIgnoreCase))
            {
                if (!flags.Add(name))
                {
                    throw new ArgumentException($"Duplicate package gate argument: --{name}");
                }
                continue;
            }

            if (name is not ("version" or "artifacts-path" or "sample-target-frameworks") ||
                index + 1 >= args.Length || string.IsNullOrWhiteSpace(args[index + 1]) ||
                args[index + 1].StartsWith("--", StringComparison.Ordinal) ||
                !options.TryAdd(name, args[++index]))
            {
                throw new ArgumentException($"Invalid package gate argument: --{name}");
            }
        }

        return (options, flags);
    }

    private static (int ExitCode, string[] Output) RunProcess(string fileName, string[] arguments, string workingDirectory)
    {
        var startInfo = new ProcessStartInfo(fileName)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        var output = new List<string>();
        var sync = new object();
        using var process = new Process { StartInfo = startInfo };
        process.OutputDataReceived += (_, e) =>
        {
            if (e.Data is not null)
            {
                lock (sync)
                {
                    output.Add(e.Data);
                }
            }
        };
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is not null)
            {
                lock (sync)
                {
                    output.Add(e.Data);
                }
            }
        };
        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        process.WaitForExit();
        return (process.ExitCode, [.. output]);
    }

    private static RunResult InvokeDotnet(string workingDirectory, string logsDirectory, string logName, string[] arguments)
    {
        var (exitCode, output) = RunProcess("dotnet", arguments, workingDirectory);
        var log = Path.Combine(logsDirectory, $"{logName}.log");
        File.WriteAllLines(log, output, Encoding.UTF8);
        return new RunResult(exitCode, output, log);
    }

    private static void ShowTail(RunResult run, int lines = 25)
    {
        foreach (var line in run.Output.Skip(Math.Max(0, run.Output.Length - lines)))
        {
            Console.WriteLine($"    {line}");
        }

        Console.WriteLine($"    (full log: {run.Log})");
    }

    private static void AddResult(List<GateResult> results, string gate, string status, string detail)
    {
        results.Add(new GateResult(gate, status, detail));
        WriteColored($"[{status}] {gate}: {detail}", status switch
        {
            "PASS" => ConsoleColor.Green,
            "FAIL" => ConsoleColor.Red,
            _ => ConsoleColor.Yellow,
        });
    }

    private static void WriteColored(string? line, ConsoleColor color)
    {
        var original = Console.ForegroundColor;
        try
        {
            Console.ForegroundColor = color;
            Console.WriteLine(line);
        }
        finally
        {
            Console.ForegroundColor = original;
        }
    }

    private static void PrintResultsTable(List<GateResult> results)
    {
        if (results.Count == 0)
        {
            return;
        }

        var gateWidth = Math.Max("Gate".Length, results.Max(result => result.Gate.Length));
        var statusWidth = Math.Max("Status".Length, results.Max(result => result.Status.Length));
        Console.WriteLine($"{"Gate".PadRight(gateWidth)}  {"Status".PadRight(statusWidth)}  Detail");
        foreach (var result in results)
        {
            Console.WriteLine($"{result.Gate.PadRight(gateWidth)}  {result.Status.PadRight(statusWidth)}  {result.Detail}");
        }

        Console.WriteLine();
    }

    private static string BuildNugetConfig(string packagesFolder, string feed) =>
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
        "<configuration>\n" +
        "  <config>\n" +
        $"    <add key=\"globalPackagesFolder\" value=\"{packagesFolder}\" />\n" +
        "  </config>\n" +
        "  <packageSources>\n" +
        "    <clear />\n" +
        $"    <add key=\"local-oc\" value=\"{feed}\" />\n" +
        "    <add key=\"nuget.org\" value=\"https://api.nuget.org/v3/index.json\" />\n" +
        "  </packageSources>\n" +
        "  <packageSourceMapping>\n" +
        "    <packageSource key=\"local-oc\">\n" +
        "      <package pattern=\"ReactiveUI.Primitives*\" />\n" +
        "      <package pattern=\"ReactiveUI.Disposables\" />\n" +
        "    </packageSource>\n" +
        "    <packageSource key=\"nuget.org\">\n" +
        "      <package pattern=\"*\" />\n" +
        "    </packageSource>\n" +
        "  </packageSourceMapping>\n" +
        "</configuration>\n";

    private sealed record GateResult(string Gate, string Status, string Detail);

    private sealed record RunResult(int ExitCode, string[] Output, string Log);
}
