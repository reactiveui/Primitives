using System.IO.Compression;
using System.Text.Json;
using System.Xml.Linq;

namespace OccasionallyConnected.Ci;

public static partial class Packages
{
    internal static string CreateReleaseFilter(string src, string outputPath)
    {
        using var filter = JsonDocument.Parse(File.ReadAllText(Path.Combine(src, "ReactiveUI.Primitives.slnf")));
        var solution = filter.RootElement.GetProperty("solution");
        var projects = solution.GetProperty("projects").EnumerateArray().Select(value => value.GetString()!).ToArray();
        var selected = SelectReleaseProjects(projects);
        File.WriteAllText(outputPath, JsonSerializer.Serialize(new
        {
            solution = new
            {
                path = Path.GetFullPath(Path.Combine(src, solution.GetProperty("path").GetString()!)),
                projects = selected,
            },
        }));
        return outputPath;
    }

    internal static string[] SelectReleaseProjects(IEnumerable<string> projects)
    {
        var selected = DependencyProjects.Concat(OccasionallyConnectedProjects).Select(name => $"{name}\\{name}.csproj").ToArray();
        var missing = selected.Except(projects, StringComparer.Ordinal).ToArray();
        if (missing.Length != 0)
        {
            throw new InvalidOperationException($"Release filter omits production packages: {string.Join(", ", missing)}");
        }

        return selected;
    }

    internal static void ValidateReleasePackages(string feed, string version)
    {
        foreach (var name in DependencyProjects.Concat(OccasionallyConnectedProjects))
        {
            var path = Path.Combine(feed, $"{name}.{version}.nupkg");
            if (!File.Exists(path))
            {
                throw new InvalidOperationException($"Release package is missing: {path}");
            }

            using var package = ZipFile.OpenRead(path);
            using var nuspecStream = package.Entries.Single(entry => entry.FullName.EndsWith(".nuspec", StringComparison.Ordinal)).Open();
            var nuspec = XDocument.Load(nuspecStream);
            ValidateReleasePackage(name, version, package.Entries.Select(entry => entry.FullName), nuspec, feed);
        }
    }

    internal static void ValidateReleasePackage(string name, string version, IEnumerable<string> entries, XDocument nuspec, string feed)
    {
        var stable = !version.Contains('-', StringComparison.Ordinal);
        var dependencies = nuspec.Descendants().Where(element => element.Name.LocalName == "dependency").ToArray();
        if (stable && (entries.Any(entry => entry.StartsWith("lib/net11.0", StringComparison.Ordinal)
                || entry.StartsWith("ref/net11.0", StringComparison.Ordinal))
            || nuspec.Descendants().Any(element => element.Name.LocalName == "group"
                && (element.Attribute("targetFramework")?.Value.StartsWith("net11.0", StringComparison.OrdinalIgnoreCase) ?? false))
            || dependencies.Any(element => element.Attribute("version")?.Value.Contains('-', StringComparison.Ordinal) ?? false)))
        {
            throw new InvalidOperationException($"Stable package {name} contains preview assets or dependencies.");
        }

        foreach (var dependency in dependencies)
        {
            var id = dependency.Attribute("id")?.Value ?? string.Empty;
            if ((id.StartsWith("ReactiveUI.Primitives", StringComparison.Ordinal) || id == "ReactiveUI.Disposables")
                && !File.Exists(Path.Combine(feed, $"{id}.{version}.nupkg")))
            {
                throw new InvalidOperationException($"Release package {name} has an unpublished dependency: {id}.");
            }
        }
    }
}
