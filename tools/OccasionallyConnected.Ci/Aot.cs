// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;

namespace OccasionallyConnected.Ci;

/// <summary>
/// Packs the OccasionallyConnected packages
/// into a fresh local feed, restores a clean consumer exclusively from that feed, publishes it net10.0
/// NativeAOT for win-x64, and runs the resulting native executable. This gate is strict: it throws (and
/// returns a non-zero exit code) instead of skipping on any failure, including trim/AOT warnings
/// attributed to ReactiveUI assemblies. Uses only the BCL; never shells out to PowerShell.
/// </summary>
public static partial class Aot
{
    private static readonly string[] DependencyProjects = OccasionallyConnectedPackageSet.Dependencies;

    private static readonly string[] OccasionallyConnectedProjects = OccasionallyConnectedPackageSet.Names;

    [GeneratedRegex(@"warning IL\d{4}")]
    private static partial Regex WarningRegex();

    [GeneratedRegex(@"^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$")]
    private static partial Regex VersionRegex();

    /// <summary>
    /// Runs the NativeAOT packed-consumer gate.
    /// </summary>
    /// <param name="args">Command-line arguments: --version, --artifacts-path.</param>
    /// <returns>0 on success; 1 when a gate step fails.</returns>
    public static int Run(string[] args)
    {
        try
        {
            RunCore(args);
            return 0;
        }
        catch (AotGateException ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }
    }

    private static void RunCore(string[] args)
    {
        var options = ParseOptions(args);
        var repoRoot = GetRepoRoot();
        var src = Path.Combine(repoRoot, "src");

        var version = options.TryGetValue("version", out var versionOption)
            ? versionOption
            : $"0.1.0-ocaot.{DateTime.UtcNow:yyyyMMddHHmmss}";
        var artifactsPath = Path.GetFullPath(options.TryGetValue("artifacts-path", out var artifactsOption)
            ? artifactsOption
            : Path.Combine(repoRoot, "artifacts", "oc-aot", version));

        if (Directory.Exists(artifactsPath) || File.Exists(artifactsPath))
        {
            throw new AotGateException($"AOT output path already exists: {artifactsPath}. Choose a fresh --artifacts-path.");
        }

        if (!VersionRegex().IsMatch(version))
        {
            throw new AotGateException($"Invalid package version: {version}");
        }

        var feed = Path.Combine(artifactsPath, "feed");
        var sample = Path.Combine(artifactsPath, "clean-sample");
        var packagesFolder = Path.Combine(sample, ".packages");
        var publishOutput = Path.Combine(artifactsPath, "publish-aot");
        var logs = Path.Combine(artifactsPath, "logs");
        Directory.CreateDirectory(feed);
        Directory.CreateDirectory(logs);

        Console.WriteLine($"NativeAOT packed-consumer gate: version {version}");
        Console.WriteLine($"Artifacts: {artifactsPath}");

        // The AOT consumer targets net10.0; platform workloads are unrelated to its local package feed.
        var packProperties = new[]
        {
            "-p:LangVersion=preview",
            "-p:AndroidPrimitivesTargetFrameworks=",
            "-p:ApplePrimitivesTargetFrameworks=",
        };
        foreach (var project in DependencyProjects.Concat(OccasionallyConnectedProjects))
        {
            Console.WriteLine($"Packing {project}");
            var arguments = new List<string>
            {
                "pack", $"{project}/{project}.csproj", "-c", "Release", "-nologo", "-o", feed,
                $"-p:MinVerVersionOverride={version}", "-p:ContinuousIntegrationBuild=true",
                "--disable-build-servers", "-m:1",
            };
            arguments.AddRange(packProperties);
            InvokeDotnetOrThrow(src, logs, $"pack-{project}", [.. arguments]);
        }

        var sampleSource = Path.Combine(repoRoot, "samples", "OccasionallyConnected.PackedSample");
        Directory.CreateDirectory(sample);
        File.Copy(
            Path.Combine(sampleSource, "OccasionallyConnected.PackedSample.csproj"),
            Path.Combine(sample, "OccasionallyConnected.PackedSample.csproj"),
            overwrite: true);
        foreach (var file in Directory.EnumerateFiles(sampleSource, "*.cs"))
        {
            File.Copy(file, Path.Combine(sample, Path.GetFileName(file)), overwrite: true);
        }

        Packages.AddConsumerPackages(Path.Combine(sample, "OccasionallyConnected.PackedSample.csproj"));
        File.WriteAllText(Path.Combine(sample, "Directory.Build.props"), "<Project />");
        File.WriteAllText(Path.Combine(sample, "Directory.Build.targets"), "<Project />");
        File.WriteAllText(
            Path.Combine(sample, "Directory.Packages.props"),
            "<Project><PropertyGroup><ManagePackageVersionsCentrally>false</ManagePackageVersionsCentrally></PropertyGroup></Project>");
        File.WriteAllText(Path.Combine(sample, "nuget.config"), BuildNugetConfig(packagesFolder, feed));

        var projectFile = "OccasionallyConnected.PackedSample.csproj";
        var commonProperties = new[]
        {
            "-c", "Release", "-nologo", $"-p:OccasionallyConnectedPackageVersion={version}",
            "-p:LangVersion=preview",
        };

        var restore = InvokeDotnetOrThrow(
            sample,
            logs,
            "restore",
            [
                "restore", projectFile, "-p:TargetFrameworks=net10.0",
                $"-p:OccasionallyConnectedPackageVersion={version}", "-p:LangVersion=preview",
            ]);

        var resolved = Directory.Exists(packagesFolder)
            ? Directory.GetDirectories(packagesFolder)
                .Where(dir => Path.GetFileName(dir).StartsWith("reactiveui.primitives.occasionallyconnected", StringComparison.OrdinalIgnoreCase))
                .ToArray()
            : [];
        if (resolved.Length < OccasionallyConnectedProjects.Length)
        {
            throw new AotGateException(
                $"Clean restore found only {resolved.Length} of {OccasionallyConnectedProjects.Length} OccasionallyConnected packages. Restore log: {restore.Log}");
        }

        var publishArguments = new List<string>
        {
            "publish", projectFile, "-p:TargetFrameworks=net10.0", "-f", "net10.0", "-r", "win-x64",
            "-o", publishOutput, "-p:PublishAot=true", "--self-contained",
        };
        publishArguments.AddRange(commonProperties);
        var publish = InvokeDotnetOrThrow(sample, logs, "publish-aot", [.. publishArguments]);

        var aotWarnings = publish.Output.Where(line => WarningRegex().IsMatch(line)).Distinct().OrderBy(line => line, StringComparer.Ordinal).ToArray();
        var packageWarnings = aotWarnings.Where(line => line.Contains("ReactiveUI.", StringComparison.Ordinal)).ToArray();
        var thirdPartyWarnings = aotWarnings.Except(packageWarnings).ToArray();
        foreach (var line in thirdPartyWarnings)
        {
            WriteColored($"third-party: {line}", ConsoleColor.Yellow);
        }

        foreach (var line in packageWarnings)
        {
            WriteColored($"package warning: {line}", ConsoleColor.Red);
        }

        if (packageWarnings.Length > 0)
        {
            throw new AotGateException(
                $"NativeAOT publish reported {packageWarnings.Length} warning(s) from ReactiveUI packages. Full log: {publish.Log}");
        }

        var binary = Path.Combine(publishOutput, "OccasionallyConnected.PackedSample.exe");
        if (!File.Exists(binary))
        {
            throw new AotGateException($"NativeAOT executable was not produced: {binary}");
        }

        var (runExitCode, runOutput) = RunProcess(binary, [], publishOutput);
        var runLog = Path.Combine(logs, "publish-aot-run.log");
        File.WriteAllLines(runLog, runOutput, Encoding.UTF8);
        foreach (var line in runOutput.Where(l => l.StartsWith("FAIL", StringComparison.Ordinal) || l.StartsWith("FAULT", StringComparison.Ordinal)))
        {
            WriteColored(line, ConsoleColor.Red);
        }

        if (runExitCode != 0)
        {
            throw new AotGateException($"NativeAOT consumer exited with code {runExitCode}. Full log: {runLog}");
        }

        var summary = runOutput.LastOrDefault(line => line.StartsWith("SUMMARY ", StringComparison.Ordinal));
        Console.WriteLine($"NativeAOT clean-consumer gate passed: win-x64; ReactiveUI warnings={packageWarnings.Length}; {summary}");
    }

    private static string GetRepoRoot([CallerFilePath] string sourceFilePath = "")
    {
        var ciDirectory = Path.GetDirectoryName(sourceFilePath) ?? throw new InvalidOperationException("Unable to determine the source directory.");
        var toolsDirectory = Path.GetDirectoryName(ciDirectory) ?? throw new InvalidOperationException("Unable to determine the tools directory.");
        return Path.GetDirectoryName(toolsDirectory) ?? throw new InvalidOperationException("Unable to determine the repository root.");
    }

    private static Dictionary<string, string> ParseOptions(string[] args)
    {
        var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var index = 0; index < args.Length; index++)
        {
            if (!args[index].StartsWith("--", StringComparison.Ordinal))
            {
                throw new AotGateException($"Unexpected AOT argument: {args[index]}");
            }

            var name = args[index][2..];
            if (name is not ("version" or "artifacts-path") ||
                index + 1 >= args.Length || string.IsNullOrWhiteSpace(args[index + 1]) ||
                args[index + 1].StartsWith("--", StringComparison.Ordinal) ||
                !options.TryAdd(name, args[++index]))
            {
                throw new AotGateException($"Invalid AOT argument: --{name}");
            }
        }

        return options;
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

    private static RunResult InvokeDotnetOrThrow(string workingDirectory, string logsDirectory, string logName, string[] arguments)
    {
        var run = InvokeDotnet(workingDirectory, logsDirectory, logName, arguments);
        if (run.ExitCode != 0)
        {
            foreach (var line in run.Output.Skip(Math.Max(0, run.Output.Length - 30)))
            {
                Console.WriteLine($"    {line}");
            }

            throw new AotGateException($"dotnet {string.Join(' ', arguments)} failed (exit {run.ExitCode}). Full log: {run.Log}");
        }

        return run;
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

    private sealed record RunResult(int ExitCode, string[] Output, string Log);

    private sealed class AotGateException : Exception
    {
        public AotGateException()
        {
        }

        public AotGateException(string message)
            : base(message)
        {
        }

        public AotGateException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
