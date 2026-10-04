// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace OccasionallyConnected.Ci;

public static partial class Mutation
{
    private static readonly Campaign[] Campaigns =
    [
        new(
            "Durability",
            "src/ReactiveUI.Primitives.OccasionallyConnected/InMemoryLocalStoreAdapter.cs",
            "_operations.Add(operation.OperationId, record);",
            "_ = record;",
            "ReactiveUI.Primitives.OccasionallyConnected.Tests",
            "InMemoryLocalStoreAdapterTests"),
        new(
            "Ordering",
            "src/ReactiveUI.Primitives.OccasionallyConnected/BatchSelectionPlanner.cs",
            "item.ClientSequence > previousSequence && item.EncodedBytes > 0",
            "item.ClientSequence >= previousSequence && item.EncodedBytes > 0",
            "ReactiveUI.Primitives.OccasionallyConnected.Tests",
            "BatchSelectionPlannerTests"),
        new(
            "Idempotency",
            "src/ReactiveUI.Primitives.OccasionallyConnected.Server/ServerCommitJournalOperations.cs",
            "existing.Entry.Fingerprint.Matches(entry.Fingerprint) ? ServerCommitStatus.StaleRevision : ServerCommitStatus.IntentMismatch",
            "existing.Entry.Fingerprint.Matches(entry.Fingerprint) ? ServerCommitStatus.IntentMismatch : ServerCommitStatus.StaleRevision",
            "ReactiveUI.Primitives.OccasionallyConnected.Server.Tests",
            "InMemoryServerCommitJournalTests"),
        new(
            "Retry",
            "src/ReactiveUI.Primitives.OccasionallyConnected/RetryPolicy.cs",
            "state.TransientAttemptCount >= _options.MaximumRetryAttempts",
            "state.TransientAttemptCount > _options.MaximumRetryAttempts",
            "ReactiveUI.Primitives.OccasionallyConnected.Tests",
            "RetryPolicyTests"),
    ];

    private static readonly JsonSerializerOptions SummaryJsonOptions = new() { WriteIndented = true };

    [GeneratedRegex(@"\x1B\[[0-9;]*m")]
    private static partial Regex AnsiEscape();

    [GeneratedRegex(@"^\s*total:\s*[1-9]\d*\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline)]
    private static partial Regex TotalTests();

    [GeneratedRegex(@"^\s*failed:\s*0\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline)]
    private static partial Regex NoFailures();

    [GeneratedRegex(@"^\s*failed:\s*[1-9]\d*\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline)]
    private static partial Regex SomeFailures();

    public static int Run(string[] args)
    {
        try
        {
            var selected = ParseCampaign(args);
            var repositoryRoot = FindRepositoryRoot();
            var runRoot = Path.Combine(
                repositoryRoot, "artifacts", "oc-mutation",
                $"{DateTime.UtcNow:yyyyMMdd-HHmmss-fff}-{Guid.NewGuid():N}");
            var workspace = Path.Combine(runRoot, "workspace");
            var reports = Path.Combine(runRoot, "reports");
            Directory.CreateDirectory(workspace);
            Directory.CreateDirectory(reports);
            CopyCurrentSource(repositoryRoot, workspace);

            var summary = new List<object>();
            foreach (var campaign in Campaigns)
            {
                if (selected != "All" && selected != campaign.Name)
                {
                    continue;
                }

                RunCampaign(campaign, workspace, reports);
                summary.Add(new
                {
                    Campaign = campaign.Name,
                    Source = campaign.File,
                    campaign.Original,
                    campaign.Mutated,
                    Baseline = "Passed",
                    MutantBuild = "Passed",
                    Mutant = "Killed",
                    campaign.TestClass,
                });
                Console.WriteLine($"{campaign.Name}: mutant killed by {campaign.TestClass}; logs: {reports}");
            }

            // Preserve a concise object for one selected campaign and an array for a full run.
            File.WriteAllText(
                Path.Combine(reports, "summary.json"),
                JsonSerializer.Serialize(summary.Count == 1 ? summary[0] : summary, SummaryJsonOptions));
            return 0;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine(error.Message);
            return 1;
        }
    }

    private static string ParseCampaign(string[] args)
    {
        if (args.Length == 0)
        {
            return "All";
        }

        string value;
        if (args.Length == 2 && (args[0].Equals("--campaign", StringComparison.OrdinalIgnoreCase) ||
                                 args[0].Equals("-Campaign", StringComparison.OrdinalIgnoreCase)))
        {
            value = args[1];
        }
        else if (args.Length == 1 && args[0].StartsWith("--campaign=", StringComparison.OrdinalIgnoreCase))
        {
            value = args[0]["--campaign=".Length..];
        }
        else
        {
            throw new ArgumentException("Usage: mutation [--campaign All|Durability|Ordering|Idempotency|Retry]");
        }

        if (value.Equals("All", StringComparison.OrdinalIgnoreCase))
        {
            return "All";
        }

        return Campaigns.FirstOrDefault(c => c.Name.Equals(value, StringComparison.OrdinalIgnoreCase))?.Name
               ?? throw new ArgumentException($"Invalid campaign '{value}'. Expected All, Durability, Ordering, Idempotency, or Retry.");
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

    private static void CopyCurrentSource(string repositoryRoot, string workspace)
    {
        using var git = new Process
        {
            StartInfo = new ProcessStartInfo("git")
            {
                WorkingDirectory = repositoryRoot,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            },
        };
        foreach (var argument in new[]
                 {
                     "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--",
                     "src", "tools", "global.json", "NuGet.Config", "nuget.config", ".editorconfig",
                 })
        {
            git.StartInfo.ArgumentList.Add(argument);
        }

        git.Start();
        var outputTask = git.StandardOutput.ReadToEndAsync();
        var errorTask = git.StandardError.ReadToEndAsync();
        if (!git.WaitForExit(60_000))
        {
            git.Kill(entireProcessTree: true);
            git.WaitForExit();
            throw new TimeoutException("git ls-files exceeded 60 seconds.");
        }

        var paths = outputTask.GetAwaiter().GetResult().Split('\0', StringSplitOptions.RemoveEmptyEntries);
        var errors = errorTask.GetAwaiter().GetResult();
        if (git.ExitCode != 0 || paths.Length == 0)
        {
            throw new InvalidOperationException($"Could not enumerate repository source files. {errors}");
        }

        foreach (var path in paths)
        {
            var relativePath = path.Replace('/', Path.DirectorySeparatorChar);
            var source = Path.GetFullPath(Path.Combine(repositoryRoot, relativePath));
            var destination = Path.GetFullPath(Path.Combine(workspace, relativePath));
            if (!source.StartsWith(repositoryRoot + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase) ||
                !destination.StartsWith(workspace + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Source path is outside the repository: {path}");
            }

            if (!File.Exists(source))
            {
                continue;
            }

            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(source, destination);
        }
    }

    private static void RunCampaign(Campaign campaign, string workspace, string reports)
    {
        var sourcePath = Path.Combine(workspace, campaign.File.Replace('/', Path.DirectorySeparatorChar));
        var originalBytes = File.ReadAllBytes(sourcePath);
        var originalText = Encoding.UTF8.GetString(originalBytes);
        var position = originalText.IndexOf(campaign.Original, StringComparison.Ordinal);
        if (position < 0 || originalText.IndexOf(campaign.Original, position + 1, StringComparison.Ordinal) >= 0)
        {
            throw new InvalidOperationException($"{campaign.Name}: expected exactly one mutation target in {sourcePath}.");
        }

        var projectDirectory = Path.Combine(workspace, "src", "tests", campaign.TestProject);
        var projectFile = Path.Combine(projectDirectory, campaign.TestProject + ".csproj");
        var assembly = Path.Combine(projectDirectory, "bin", "Release", "net10.0", campaign.TestProject + ".dll");
        var filter = $"/*/*/{campaign.TestClass}/*";
        string[] buildArguments =
        [
            "build", projectFile, "-c", "Release", "-f", "net10.0", "--disable-build-servers", "-m:1",
            "-p:MinVerSkip=true", "-p:Version=0.1.0", "-p:LangVersion=preview",
            .. OccasionallyConnectedPackageSet.NeutralFrameworkProperties,
        ];

        var baselineBuildLog = Path.Combine(reports, $"{campaign.Name}-baseline-build.log");
        var baselineTestLog = Path.Combine(reports, $"{campaign.Name}-baseline-test.log");
        var mutantBuildLog = Path.Combine(reports, $"{campaign.Name}-mutant-build.log");
        var mutantTestLog = Path.Combine(reports, $"{campaign.Name}-mutant-test.log");
        if (InvokeDotnet(workspace, baselineBuildLog, buildArguments, 900) != 0)
        {
            throw new InvalidOperationException($"{campaign.Name}: baseline build failed; see {baselineBuildLog}.");
        }

        var testArguments = new[] { assembly, "--treenode-filter", filter, "--progress", "off" };
        var baselineCode = InvokeDotnet(workspace, baselineTestLog, testArguments, 300);
        var baselineOutput = AnsiEscape().Replace(File.ReadAllText(baselineTestLog), "");
        if (baselineCode != 0 || !TotalTests().IsMatch(baselineOutput) || !NoFailures().IsMatch(baselineOutput))
        {
            throw new InvalidOperationException($"{campaign.Name}: baseline TUnit tests failed or no tests ran; see {baselineTestLog}.");
        }

        var mutatedText = originalText.Remove(position, campaign.Original.Length).Insert(position, campaign.Mutated);
        try
        {
            File.WriteAllBytes(sourcePath, Encoding.UTF8.GetBytes(mutatedText));
            if (InvokeDotnet(workspace, mutantBuildLog, buildArguments, 900) != 0)
            {
                throw new InvalidOperationException($"{campaign.Name}: mutant did not compile; see {mutantBuildLog}.");
            }

            var mutantCode = InvokeDotnet(workspace, mutantTestLog, testArguments, 300);
            var mutantOutput = AnsiEscape().Replace(File.ReadAllText(mutantTestLog), "");
            if (mutantCode == 0 || !SomeFailures().IsMatch(mutantOutput))
            {
                throw new InvalidOperationException($"{campaign.Name}: mutant was not killed by a TUnit assertion; see {mutantTestLog}.");
            }
        }
        finally
        {
            File.WriteAllBytes(sourcePath, originalBytes);
        }
    }

    private static int InvokeDotnet(string workspace, string logPath, string[] arguments, int timeoutSeconds)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo("dotnet")
            {
                WorkingDirectory = workspace,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            },
        };
        foreach (var argument in arguments)
        {
            process.StartInfo.ArgumentList.Add(argument);
        }

        process.Start();
        var outputTask = process.StandardOutput.ReadToEndAsync();
        var errorTask = process.StandardError.ReadToEndAsync();
        var timedOut = !process.WaitForExit(checked(timeoutSeconds * 1000));
        if (timedOut)
        {
            process.Kill(entireProcessTree: true);
            process.WaitForExit();
        }

        File.WriteAllText(logPath, outputTask.GetAwaiter().GetResult() + errorTask.GetAwaiter().GetResult() +
                                   (timedOut ? $"\nTimed out after {timeoutSeconds} seconds." : ""));
        if (timedOut)
        {
            throw new TimeoutException($"dotnet command exceeded {timeoutSeconds} seconds; see {logPath}.");
        }

        return process.ExitCode;
    }

    private sealed record Campaign(
        string Name,
        string File,
        string Original,
        string Mutated,
        string TestProject,
        string TestClass);
}
