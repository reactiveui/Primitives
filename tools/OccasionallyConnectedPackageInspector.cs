// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

// A file-based app (dotnet run OccasionallyConnectedPackageInspector.cs -- ...) used by
// OccasionallyConnected.Ci's packages command. It only uses the BCL (System.IO.Compression and
// System.Reflection.Metadata), so it adds no NuGet dependencies.
//
// Modes:
//   verify  --feed <dir> --version <v> --packages <id,...> --tfms <tfm,...> --commit <sha>
//           Checks lib/<tfm> folders, the .snupkg, portable PDB identity, deterministic paths and Source Link.
//   compare --left <dir> --right <dir> --version <v> --packages <id,...>
//           Compares two packs of the same packages entry by entry.
//   verify-native --feed <dir> --version <v> --commit <sha>
//           Requires all four net10 Mobile native heads, neutral net10, symbols and matching Source Link.
// Every result line starts with PASS, FAIL or INFO. The exit code is 0 only when no line is FAIL.

using System.IO.Compression;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;

return PackageInspector.Run(args);

internal static class PackageInspector
{
    public static int Run(string[] args)
    {
        var options = ParseOptions(args);
        var failures = 0;
        var mode = args.Length > 0 ? args[0] : string.Empty;
        switch (mode)
        {
            case "verify":
                foreach (var id in Split(options["packages"]))
                {
                    failures += Verify(options["feed"], id, options["version"], Split(options["tfms"]), options["commit"]);
                }

                break;
            case "compare":
                foreach (var id in Split(options["packages"]))
                {
                    failures += Compare(options["left"], options["right"], $"{id}.{options["version"]}.nupkg");
                    failures += Compare(options["left"], options["right"], $"{id}.{options["version"]}.snupkg");
                }

                break;
            case "verify-native":
                failures += VerifyNative(options["feed"], options["version"], options["commit"]);
                break;
            default:
                Console.WriteLine("FAIL usage: verify|compare|verify-native --option value ...");
                return 2;
        }

        return failures == 0 ? 0 : 1;
    }

    static int VerifyNative(string feed, string version, string commit)
    {
        const string id = "ReactiveUI.Primitives.OccasionallyConnected.Mobile";
        string[] heads = ["net10.0-android", "net10.0-windows", "net10.0-ios", "net10.0-maccatalyst"];
        var path = Path.Combine(feed, $"{id}.{version}.nupkg");
        if (!File.Exists(path))
        {
            return Report(false, "native-package", id, $"missing {path}");
        }

        using var package = ZipFile.OpenRead(path);
        var tfms = package.Entries
            .Where(entry => entry.FullName.StartsWith("lib/", StringComparison.Ordinal)
                && entry.FullName.EndsWith($"/{id}.dll", StringComparison.Ordinal))
            .Select(entry => entry.FullName.Split('/')[1])
            .ToArray();
        var failures = Report(
            tfms.Contains("net10.0", StringComparer.Ordinal)
            && tfms.Length == heads.Length + 1
            && heads.All(head => tfms.Any(tfm => tfm.StartsWith(head, StringComparison.Ordinal)))
            && tfms.All(tfm => tfm == "net10.0" || heads.Any(head => tfm.StartsWith(head, StringComparison.Ordinal))),
            "complete-native-tfms", id, $"actual=[{string.Join(",", tfms)}]");
        using var manifestStream = package.Entries.Single(entry => entry.FullName.EndsWith(".nuspec", StringComparison.Ordinal)).Open();
        var manifest = XDocument.Load(manifestStream);
        failures += Report(
            manifest.Descendants().Single(element => element.Name.LocalName == "id").Value == id
            && manifest.Descendants().Single(element => element.Name.LocalName == "version").Value == version,
            "native-package-identity", id, version);
        if (!version.Contains('-', StringComparison.Ordinal))
        {
            failures += Report(
                !manifest.Descendants().Where(element => element.Name.LocalName == "dependency")
                    .Any(element => element.Attribute("version")?.Value.Contains('-', StringComparison.Ordinal) ?? false),
                "stable-native-dependencies", id, "no prerelease dependency versions");
        }

        return failures + Verify(feed, id, version, tfms, commit);
    }

    static int Verify(string feed, string id, string version, string[] tfms, string commit)
    {
        var failures = 0;
        var packagePath = Path.Combine(feed, $"{id}.{version}.nupkg");
        var symbolsPath = Path.Combine(feed, $"{id}.{version}.snupkg");
        if (!File.Exists(packagePath))
        {
            return Report(false, "package", id, $"missing {packagePath}");
        }

        using var package = ZipFile.OpenRead(packagePath);
        var nuspec = XDocument.Load(package.Entries.Single(entry => entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase)).Open());
        var repository = nuspec.Descendants().FirstOrDefault(element => element.Name.LocalName == "repository");
        var packageCommit = repository?.Attribute("commit")?.Value;
        failures += Report(string.Equals(packageCommit, commit, StringComparison.OrdinalIgnoreCase), "nuspec-repository-commit", id, $"expected={commit} actual={packageCommit ?? "<none>"}");

        var libFolders = package.Entries
            .Where(entry => entry.FullName.StartsWith("lib/", StringComparison.Ordinal) && entry.FullName.EndsWith($"/{id}.dll", StringComparison.Ordinal))
            .Select(entry => entry.FullName.Split('/')[1])
            .OrderBy(tfm => tfm, StringComparer.Ordinal)
            .ToArray();
        var missing = tfms.Except(libFolders, StringComparer.Ordinal).ToArray();
        var unexpected = libFolders.Except(tfms, StringComparer.Ordinal).ToArray();
        failures += Report(missing.Length == 0 && unexpected.Length == 0, "lib-tfms", id, $"actual=[{string.Join(",", libFolders)}] missing=[{string.Join(",", missing)}] unexpected=[{string.Join(",", unexpected)}]");
        var pdbInPackage = package.Entries.Where(entry => entry.FullName.EndsWith(".pdb", StringComparison.OrdinalIgnoreCase)).Select(entry => entry.FullName).ToArray();
        failures += Report(pdbInPackage.Length == 0, "no-pdb-in-nupkg", id, pdbInPackage.Length == 0 ? "symbols live only in the .snupkg" : string.Join(",", pdbInPackage));

        if (!File.Exists(symbolsPath))
        {
            return failures + Report(false, "snupkg", id, $"missing {symbolsPath}");
        }

        using var symbols = ZipFile.OpenRead(symbolsPath);
        failures += Report(true, "snupkg", id, Path.GetFileName(symbolsPath));
        foreach (var tfm in libFolders)
        {
            var dll = package.GetEntry($"lib/{tfm}/{id}.dll");
            var pdb = symbols.GetEntry($"lib/{tfm}/{id}.pdb");
            if (dll is null || pdb is null)
            {
                failures += Report(false, "pdb", $"{id}/{tfm}", "the .snupkg has no matching PDB");
                continue;
            }

            failures += VerifyPdb(id, tfm, ReadAll(dll), ReadAll(pdb), commit);
        }

        return failures;
    }

    static int VerifyPdb(string id, string tfm, byte[] assembly, byte[] pdb, string commit)
    {
        var failures = 0;
        var subject = $"{id}/{tfm}";
        using var peReader = new PEReader(new MemoryStream(assembly));
        var debugEntries = peReader.ReadDebugDirectory();
        var codeView = debugEntries.FirstOrDefault(entry => entry.Type == DebugDirectoryEntryType.CodeView);
        var embedded = debugEntries.Any(entry => entry.Type == DebugDirectoryEntryType.EmbeddedPortablePdb);
        var reproducible = debugEntries.Any(entry => entry.Type == DebugDirectoryEntryType.Reproducible);
        failures += Report(reproducible, "deterministic-assembly", subject, reproducible ? "Reproducible debug entry present" : "no Reproducible debug entry");
        failures += Report(!embedded, "pdb-not-embedded", subject, embedded ? "the assembly embeds its PDB" : "external portable PDB");

        using var provider = MetadataReaderProvider.FromPortablePdbStream(new MemoryStream(pdb));
        var reader = provider.GetMetadataReader();
        var pdbId = new BlobContentId(reader.DebugMetadataHeader!.Id);
        var codeViewData = peReader.ReadCodeViewDebugDirectoryData(codeView);
        var matches = pdbId.Guid == codeViewData.Guid && pdbId.Stamp == codeView.Stamp;
        failures += Report(matches, "pdb-matches-assembly", subject, $"pdb={pdbId.Guid:D}/{pdbId.Stamp:X8} assembly={codeViewData.Guid:D}/{codeView.Stamp:X8}");

        var sourceLinkKind = new Guid("CC110556-A091-4D38-9FEC-25AB9A351A6A");
        var embeddedSourceKind = new Guid("0E8A571B-6926-466E-B4AD-8AB04611F5FE");
        string? sourceLinkJson = null;
        var embeddedDocuments = new HashSet<DocumentHandle>();
        foreach (var handle in reader.CustomDebugInformation)
        {
            var information = reader.GetCustomDebugInformation(handle);
            var kind = reader.GetGuid(information.Kind);
            if (kind == sourceLinkKind)
            {
                sourceLinkJson = Encoding.UTF8.GetString(reader.GetBlobBytes(information.Value));
            }
            else if (kind == embeddedSourceKind && information.Parent.Kind == HandleKind.Document)
            {
                _ = embeddedDocuments.Add((DocumentHandle)information.Parent);
            }
        }

        if (sourceLinkJson is null)
        {
            return failures + Report(false, "source-link", subject, "the PDB has no Source Link JSON");
        }

        var mappings = new List<(string LocalPrefix, string UrlPrefix)>();
        using (var json = JsonDocument.Parse(sourceLinkJson))
        {
            foreach (var mapping in json.RootElement.GetProperty("documents").EnumerateObject())
            {
                mappings.Add((mapping.Name.TrimEnd('*'), mapping.Value.GetString()!.TrimEnd('*')));
            }
        }

        var expectedUrl = $"https://raw.githubusercontent.com/reactiveui/Primitives/{commit}/";
        var urlOk = mappings.Count > 0 && mappings.All(mapping => mapping.UrlPrefix.StartsWith(expectedUrl, StringComparison.OrdinalIgnoreCase));
        failures += Report(urlOk, "source-link", subject, string.Join("; ", mappings.Select(mapping => $"{mapping.LocalPrefix}* -> {mapping.UrlPrefix}*")));

        var documents = 0;
        var uncovered = new List<string>();
        var nonDeterministic = new List<string>();
        foreach (var handle in reader.Documents)
        {
            documents++;
            var name = reader.GetString(reader.GetDocument(handle).Name);
            if (!name.StartsWith("/_/", StringComparison.Ordinal))
            {
                nonDeterministic.Add(name);
            }

            var linked = mappings.Any(mapping => name.StartsWith(mapping.LocalPrefix, StringComparison.OrdinalIgnoreCase));
            if (!linked && !embeddedDocuments.Contains(handle))
            {
                uncovered.Add(name);
            }
        }

        failures += Report(nonDeterministic.Count == 0, "deterministic-source-paths", subject, nonDeterministic.Count == 0 ? $"{documents} documents under /_/" : string.Join(", ", nonDeterministic.Take(5)));
        failures += Report(uncovered.Count == 0, "sources-resolvable", subject, uncovered.Count == 0 ? $"{documents} documents: {documents - embeddedDocuments.Count} linked, {embeddedDocuments.Count} embedded" : $"{uncovered.Count} documents neither linked nor embedded, e.g. {string.Join(", ", uncovered.Take(3))}");
        return failures;
    }

    static int Compare(string left, string right, string fileName)
    {
        var leftPath = Path.Combine(left, fileName);
        var rightPath = Path.Combine(right, fileName);
        if (!File.Exists(leftPath) || !File.Exists(rightPath))
        {
            return Report(false, "deterministic-package", fileName, $"missing {(File.Exists(leftPath) ? rightPath : leftPath)}");
        }

        var archiveIdentical = Hash(File.ReadAllBytes(leftPath)) == Hash(File.ReadAllBytes(rightPath));
        using var leftZip = ZipFile.OpenRead(leftPath);
        using var rightZip = ZipFile.OpenRead(rightPath);
        var leftEntries = leftZip.Entries.ToDictionary(entry => entry.FullName, entry => Hash(ReadAll(entry)), StringComparer.Ordinal);
        var rightEntries = rightZip.Entries.ToDictionary(entry => entry.FullName, entry => Hash(ReadAll(entry)), StringComparer.Ordinal);
        var differences = leftEntries.Keys.Union(rightEntries.Keys, StringComparer.Ordinal)
            .Where(name => !leftEntries.TryGetValue(name, out var leftHash) || !rightEntries.TryGetValue(name, out var rightHash) || leftHash != rightHash)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();
        var detail = differences.Length == 0
            ? $"{leftEntries.Count} entries identical; archive bytes {(archiveIdentical ? "identical" : "differ only in zip timestamps/metadata")}"
            : $"{differences.Length} entries differ: {string.Join(", ", differences)}";
        return Report(differences.Length == 0, "deterministic-package", fileName, detail);
    }

    static int Report(bool passed, string gate, string subject, string detail)
    {
        Console.WriteLine($"{(passed ? "PASS" : "FAIL")} {gate} {subject}: {detail}");
        return passed ? 0 : 1;
    }

    static byte[] ReadAll(ZipArchiveEntry entry)
    {
        using var stream = entry.Open();
        using var buffer = new MemoryStream();
        stream.CopyTo(buffer);
        return buffer.ToArray();
    }

    static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes));

    static string[] Split(string value) => value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    static Dictionary<string, string> ParseOptions(string[] arguments)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var index = 1; index + 1 < arguments.Length; index += 2)
        {
            result[arguments[index].TrimStart('-')] = arguments[index + 1];
        }

        return result;
    }
}
