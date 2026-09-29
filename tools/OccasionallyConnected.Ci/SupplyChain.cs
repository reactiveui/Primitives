// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace OccasionallyConnected.Ci;

public static partial class SupplyChain
{
    private const string DefaultVersion = "0.1.0-ocscan";
    private const string SbomToolVersion = "4.1.5";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static async Task<int> Run(string[] args)
    {
        try
        {
            var (version, outputArgument, projects) = ParseArguments(args);
            var root = FindRepositoryRoot();
            var src = Path.Combine(root, "src");
            var output = Path.GetFullPath(outputArgument ?? Path.Combine(root, "artifacts", "oc-supply-chain"));
            if (Directory.Exists(output) || File.Exists(output))
            {
                throw new InvalidOperationException($"Supply-chain output path already exists: {output}. Choose a fresh artifacts path.");
            }

            var projectFiles = projects.Select(name => Path.Combine(src, name, $"{name}.csproj")).ToArray();
            foreach (var projectFile in projectFiles)
            {
                if (!File.Exists(projectFile))
                {
                    throw new FileNotFoundException($"Missing project: {projectFile}", projectFile);
                }
            }

            var drop = Path.Combine(output, "packages");
            var reports = Path.Combine(output, "reports");
            var toolDir = Path.Combine(output, "tools");
            Directory.CreateDirectory(drop);
            Directory.CreateDirectory(reports);
            Directory.CreateDirectory(toolDir);

            var licenses = new Dictionary<string, LicenseEntry>(StringComparer.OrdinalIgnoreCase);
            var findings = new List<AuditFinding>();
            for (var index = 0; index < projects.Length; index++)
            {
                var name = projects[index];
                var projectFile = projectFiles[index];
                Console.WriteLine($"Packing {name}");
                await RunCommand("dotnet", src, null, "pack", projectFile, "-c", "Release", "-o", drop,
                    $"-p:MinVerVersionOverride={version}", "-p:ContinuousIntegrationBuild=true",
                    "-p:LangVersion=preview",
                    "--disable-build-servers", "-m:1", "-p:AndroidPrimitivesTargetFrameworks=",
                    "-p:ApplePrimitivesTargetFrameworks=");

                var packageFile = Path.Combine(drop, $"{name}.{version}.nupkg");
                if (!File.Exists(packageFile))
                {
                    throw new FileNotFoundException($"Expected package not found: {packageFile}", packageFile);
                }

                CheckLicenses(Path.Combine(src, name, "obj", "project.assets.json"), licenses);
                await ReadAudit(src, reports, name, projectFile, "vulnerable", findings);
                await ReadAudit(src, reports, name, projectFile, "deprecated", findings);
            }

            if (licenses.Count == 0)
            {
                throw new InvalidOperationException("No NuGet dependencies were found in the package assets.");
            }

            await WriteJson(Path.Combine(reports, "licenses.json"), licenses.Values
                .OrderBy(license => license.Package, StringComparer.Ordinal)
                .ThenBy(license => license.Version, StringComparer.Ordinal).ToArray());
            await WriteJson(Path.Combine(reports, "audit-findings.json"), findings);

            Console.WriteLine("Installing pinned Microsoft SBOM tool");
            await RunCommand("dotnet", src, null, "tool", "install", "Microsoft.Sbom.DotNetTool",
                "--version", SbomToolVersion, "--tool-path", toolDir);
            var tool = Path.Combine(toolDir, OperatingSystem.IsWindows() ? "sbom-tool.exe" : "sbom-tool");
            if (!File.Exists(tool))
            {
                throw new FileNotFoundException($"SBOM tool not found: {tool}", tool);
            }

            var manifest = Path.Combine(output, "sbom");
            Directory.CreateDirectory(manifest);
            await RunCommand(tool, src, null, "generate", "-b", drop, "-bc", src, "-m", manifest,
                "-pn", "ReactiveUI.Primitives.OccasionallyConnected", "-pv", version, "-ps", "ReactiveUI",
                "-nsb", "https://github.com/reactiveui/Primitives", "-pm", "true");
            var sboms = Directory.GetFiles(manifest, "manifest.spdx.json", SearchOption.AllDirectories);
            if (sboms.Length != 1)
            {
                throw new InvalidOperationException($"Expected one SPDX SBOM; found {sboms.Length}.");
            }

            var sbom = sboms[0];
            using (var document = JsonDocument.Parse(await File.ReadAllTextAsync(sbom)))
            {
                var data = document.RootElement;
                if (!TryGetString(data, "spdxVersion", out var spdxVersion) || spdxVersion != "SPDX-2.2" ||
                    !TryGetArray(data, "packages", out var packages) || packages.GetArrayLength() <= projects.Length)
                {
                    throw new InvalidOperationException("The SPDX SBOM does not contain the packed packages and their dependencies.");
                }

                foreach (var name in projects)
                {
                    if (!packages.EnumerateArray().Any(package =>
                            TryGetString(package, "name", out var packageName) && packageName == name &&
                            TryGetString(package, "versionInfo", out var packageVersion) && packageVersion == version))
                    {
                        throw new InvalidOperationException($"Packed package missing from SPDX SBOM: {name} {version}");
                    }
                }
            }

            await RunCommand(tool, src, null, "validate", "-b", drop, "-m", Path.Combine(manifest, "_manifest"),
                "-n", "-o", Path.Combine(reports, "sbom-validation.json"), "-mi", "SPDX:2.2");

            var sourceCommit = (await RunCommand("git", root, null, "rev-parse", "HEAD")).Trim();
            if (!CommitPattern().IsMatch(sourceCommit))
            {
                throw new InvalidOperationException("Could not determine the source commit SHA for the provenance record.");
            }

            var dotnetVersion = (await RunCommand("dotnet", src, null, "--version")).Trim();
            if (string.IsNullOrWhiteSpace(dotnetVersion))
            {
                throw new InvalidOperationException("Could not determine the .NET SDK version for the provenance record.");
            }

            var changes = await RunCommand("git", root, null, "status", "--porcelain", "--untracked-files=all");
            var evidence = projects.Select(name =>
            {
                var fileName = $"{name}.{version}.nupkg";
                return new { name = fileName, sha256 = HashFile(Path.Combine(drop, fileName)) };
            }).OrderBy(package => package.name, StringComparer.Ordinal).ToArray();
            await WriteJson(Path.Combine(reports, "provenance.json"), new
            {
                schemaVersion = 1,
                sourceCommit = sourceCommit.ToLowerInvariant(),
                workingTreeDirty = !string.IsNullOrWhiteSpace(changes),
                runTimestampUtc = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture),
                tools = new
                {
                    dotnetSdk = dotnetVersion,
                    sbomTool = $"Microsoft.Sbom.DotNetTool {SbomToolVersion}",
                    operatingSystem = RuntimeInformation.OSDescription,
                },
                packages = evidence,
                spdxManifest = new
                {
                    name = Path.GetRelativePath(output, sbom).Replace('\\', '/'),
                    sha256 = HashFile(sbom),
                },
            });

            if (findings.Count > 0)
            {
                foreach (var finding in findings)
                {
                    await Console.Error.WriteLineAsync($"{finding.Project} {finding.Framework} {finding.Kind} {finding.Package} {finding.Version}");
                }

                throw new InvalidOperationException($"Supply-chain audit found {findings.Count} vulnerable or deprecated dependency entries.");
            }

            Console.WriteLine($"Supply-chain gate passed: {projects.Length} packages; {licenses.Count} dependencies; SPDX SBOM validated.");
            return 0;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            await Console.Error.WriteLineAsync($"Supply-chain gate failed: {exception.Message}");
            return 1;
        }
    }

    private static (string Version, string? Output, string[] Projects) ParseArguments(string[] args)
    {
        var version = DefaultVersion;
        string? output = null;
        var names = new List<string>();
        for (var index = 0; index < args.Length; index++)
        {
            var option = args[index];
            if (option is not ("--version" or "--artifacts-path" or "--project-name") ||
                ++index >= args.Length || string.IsNullOrWhiteSpace(args[index]) ||
                args[index].StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Invalid supply-chain argument: {option}");
            }

            switch (option)
            {
                case "--version":
                    version = args[index];
                    break;
                case "--artifacts-path":
                    output = args[index];
                    break;
                case "--project-name":
                    names.Add(args[index]);
                    break;
            }
        }

        if (!VersionPattern().IsMatch(version))
        {
            throw new ArgumentException($"Invalid package version: {version}");
        }

        var projects = names.Count > 0 ? names.ToArray() : OccasionallyConnectedPackageSet.Names;
        foreach (var name in projects)
        {
            if (!ProjectPattern().IsMatch(name))
            {
                throw new ArgumentException($"Unexpected package project name: {name}");
            }
        }

        if (output is not null && (string.IsNullOrWhiteSpace(output) || output.IndexOfAny(Path.GetInvalidPathChars()) >= 0))
        {
            throw new ArgumentException($"Invalid artifacts path: {output}");
        }

        return (version, output, projects);
    }

    private static string FindRepositoryRoot()
    {
        foreach (var start in new[] { Environment.CurrentDirectory, AppContext.BaseDirectory })
        {
            for (var directory = new DirectoryInfo(start); directory is not null; directory = directory.Parent)
            {
                if (File.Exists(Path.Combine(directory.FullName, "src", "Directory.Build.props")) &&
                    Directory.Exists(Path.Combine(directory.FullName, "src", "ReactiveUI.Primitives.OccasionallyConnected.Core")))
                {
                    return directory.FullName;
                }
            }
        }

        throw new DirectoryNotFoundException("Could not locate the repository root.");
    }

    private static void CheckLicenses(string assetsPath, Dictionary<string, LicenseEntry> licenses)
    {
        using var assets = JsonDocument.Parse(File.ReadAllText(assetsPath));
        var root = assets.RootElement;
        if (!root.TryGetProperty("packageFolders", out var folders) || folders.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty("libraries", out var libraries) || libraries.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException($"Invalid NuGet package assets: {assetsPath}");
        }

        var packageFolders = folders.EnumerateObject().Select(folder => folder.Name).ToArray();
        if (packageFolders.Length == 0)
        {
            throw new InvalidOperationException($"No NuGet package folder in {assetsPath}");
        }

        foreach (var library in libraries.EnumerateObject())
        {
            if (!TryGetString(library.Value, "type", out var type) || type != "package")
            {
                continue;
            }

            var slash = library.Name.IndexOf('/');
            if (slash <= 0 || slash == library.Name.Length - 1)
            {
                throw new InvalidOperationException($"Invalid NuGet library: {library.Name}");
            }

            var id = library.Name[..slash];
            var version = library.Name[(slash + 1)..];
            if (id.StartsWith("Microsoft.NETFramework.ReferenceAssemblies", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var key = $"{id}/{version}";
            if (licenses.ContainsKey(key))
            {
                continue;
            }

            var lowerId = id.ToLowerInvariant();
            var nuspec = packageFolders.Select(folder =>
                    Path.Combine(folder, lowerId, version.ToLowerInvariant(), $"{lowerId}.nuspec"))
                .FirstOrDefault(File.Exists);
            if (nuspec is null)
            {
                throw new InvalidOperationException($"NuGet metadata missing for {key}");
            }

            var metadata = XDocument.Load(nuspec);
            var license = metadata.Root?.Elements().FirstOrDefault(element => element.Name.LocalName == "metadata")?
                .Elements().FirstOrDefault(element => element.Name.LocalName == "license");
            if (string.IsNullOrWhiteSpace(license?.Value))
            {
                throw new InvalidOperationException($"License metadata missing for {key} ({nuspec})");
            }

            var licenseType = license.Attribute("type")?.Value;
            if (licenseType is null)
            {
                throw new InvalidOperationException($"License type metadata missing for {key} ({nuspec})");
            }

            if (licenseType == "file" && !File.Exists(Path.Combine(Path.GetDirectoryName(nuspec)!, license.Value)))
            {
                throw new InvalidOperationException($"License file missing for {key}");
            }

            licenses.Add(key, new LicenseEntry(id, version, licenseType, license.Value));
        }
    }

    private static async Task ReadAudit(string src, string reports, string name, string projectFile,
        string kind, List<AuditFinding> findings)
    {
        var path = Path.Combine(reports, $"{name}.{kind}.json");
        var result = await RunCommand("dotnet", src, path, "package", "list", "--project", projectFile,
            "--include-transitive", $"--{kind}", "--format", "json", "--no-restore");
        using var report = JsonDocument.Parse(result);
        var root = report.RootElement;
        if (!root.TryGetProperty("version", out var schemaVersion) || schemaVersion.ValueKind != JsonValueKind.Number ||
            schemaVersion.GetInt32() != 1 || !TryGetArray(root, "projects", out var projects) ||
            projects.GetArrayLength() != 1)
        {
            throw new InvalidOperationException($"Unexpected NuGet {kind} report shape for {name}.");
        }

        foreach (var project in projects.EnumerateArray())
        {
            if (!TryGetArray(project, "frameworks", out var frameworks))
            {
                continue;
            }

            foreach (var framework in frameworks.EnumerateArray())
            {
                foreach (var packageKind in new[] { "topLevelPackages", "transitivePackages" })
                {
                    if (!TryGetArray(framework, packageKind, out var packages))
                    {
                        continue;
                    }

                    foreach (var package in packages.EnumerateArray())
                    {
                        if (package.ValueKind == JsonValueKind.Null)
                        {
                            continue;
                        }

                        findings.Add(new AuditFinding(
                            name,
                            GetString(framework, "framework"),
                            kind,
                            GetString(package, "id"),
                            GetString(package, "resolvedVersion"),
                            package.TryGetProperty(kind == "vulnerable" ? "vulnerabilities" : "deprecationReasons", out var detail)
                                ? detail.Clone() : null));
                    }
                }
            }
        }
    }

    private static string? GetString(JsonElement element, string property) =>
        TryGetString(element, property, out var value) ? value : null;

    private static bool TryGetString(JsonElement element, string property, out string? value)
    {
        value = null;
        if (element.ValueKind != JsonValueKind.Object || !element.TryGetProperty(property, out var field) ||
            field.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        value = field.GetString();
        return true;
    }

    private static bool TryGetArray(JsonElement element, string property, out JsonElement value)
    {
        value = default;
        return element.ValueKind == JsonValueKind.Object && element.TryGetProperty(property, out value) &&
            value.ValueKind == JsonValueKind.Array;
    }

    private static Task WriteJson<T>(string path, T value) =>
        File.WriteAllTextAsync(path, JsonSerializer.Serialize(value, JsonOptions) + Environment.NewLine);

    private static string HashFile(string path)
    {
        using var input = File.OpenRead(path);
        return Convert.ToHexString(SHA256.HashData(input)).ToLowerInvariant();
    }

    private static async Task<string> RunCommand(string executable, string workingDirectory, string? reportPath,
        params string[] arguments)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo(executable)
            {
                WorkingDirectory = workingDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            },
        };
        foreach (var argument in arguments)
        {
            process.StartInfo.ArgumentList.Add(argument);
        }

        process.Start();
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        var output = await stdout;
        var error = await stderr;
        if (reportPath is not null)
        {
            await File.WriteAllTextAsync(reportPath, output);
        }

        if (process.ExitCode != 0)
        {
            var tail = string.Join(Environment.NewLine, (output + Environment.NewLine + error)
                .Split('\n', StringSplitOptions.RemoveEmptyEntries).TakeLast(8));
            throw new InvalidOperationException($"{Path.GetFileName(executable)} {string.Join(' ', arguments)} failed (exit {process.ExitCode}). {tail}");
        }

        return output;
    }

    private sealed record LicenseEntry(string Package, string Version, string LicenseType, string License);

    private sealed record AuditFinding(string Project, string? Framework, string Kind, string? Package,
        string? Version, JsonElement? Detail);

    [GeneratedRegex(@"^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$", RegexOptions.CultureInvariant)]
    private static partial Regex VersionPattern();

    [GeneratedRegex(@"^ReactiveUI\.Primitives\.OccasionallyConnected(?:\.[A-Za-z0-9]+)*$", RegexOptions.CultureInvariant)]
    private static partial Regex ProjectPattern();

    [GeneratedRegex("^[0-9a-fA-F]{40,64}$", RegexOptions.CultureInvariant)]
    private static partial Regex CommitPattern();
}
