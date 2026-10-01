using System.IO.Compression;
using System.Xml.Linq;

namespace OccasionallyConnected.Ci;

/// <summary>Combines native Mobile assets built on their supported hosts before package signing.</summary>
internal static class MobileNativePackage
{
    private const string PackageId = "ReactiveUI.Primitives.OccasionallyConnected.Mobile";
    private const string WindowsFramework = "net10.0-windows10.0.19041";
    private const int MaximumEntries = 1_024;
    private const long MaximumUncompressedBytes = 64L * 1_024L * 1_024L;

    /// <summary>Runs the native package composition command.</summary>
    /// <param name="args">Windows input, Apple input, output, version, and source commit.</param>
    /// <returns>The command result.</returns>
    internal static int Run(string[] args)
    {
        if (args is not [var windows, var apple, var output, var version, var commit])
        {
            throw new ArgumentException("merge-mobile requires Windows directory, Apple directory, output directory, version and commit.");
        }

        Merge(windows, apple, output, version, commit);
        return 0;
    }

    /// <summary>Merges matching unsigned package and symbol artifacts without rebuilding a foreign platform.</summary>
    /// <param name="windowsDirectory">The Windows build artifacts.</param>
    /// <param name="appleDirectory">The Apple build artifacts.</param>
    /// <param name="outputDirectory">The new output directory.</param>
    /// <param name="version">The exact package version.</param>
    /// <param name="commit">The exact repository commit.</param>
    internal static void Merge(string windowsDirectory, string appleDirectory, string outputDirectory, string version, string commit)
    {
        if (Directory.Exists(outputDirectory))
        {
            throw new IOException("Native package output must be a fresh directory.");
        }

        var packages = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var extension in new[] { "nupkg", "snupkg" })
        {
            var file = $"{PackageId}.{version}.{extension}";
            packages.Add(file, MergeArchive(
                Path.Combine(windowsDirectory, file),
                Path.Combine(appleDirectory, file),
                version,
                commit));
        }

        Directory.CreateDirectory(outputDirectory);
        try
        {
            foreach (var package in packages)
            {
                File.WriteAllBytes(Path.Combine(outputDirectory, package.Key), package.Value);
            }
        }
        catch
        {
            foreach (var name in packages.Keys)
            {
                File.Delete(Path.Combine(outputDirectory, name));
            }

            Directory.Delete(outputDirectory);
            throw;
        }
    }

    private static byte[] MergeArchive(string windowsPath, string applePath, string version, string commit)
    {
        using var windows = ZipFile.OpenRead(windowsPath);
        using var apple = ZipFile.OpenRead(applePath);
        var windowsSpec = ReadManifest(windows, version, commit);
        var appleSpec = ReadManifest(apple, version, commit);
        var metadata = appleSpec.Root!.Elements().Single(element => element.Name.LocalName == "metadata");
        var windowsMetadata = windowsSpec.Root!.Elements().Single(element => element.Name.LocalName == "metadata");
        foreach (var name in new[] { "dependencies", "frameworkReferences" })
        {
            var source = windowsMetadata.Elements().SingleOrDefault(element => element.Name.LocalName == name);
            var target = metadata.Elements().SingleOrDefault(element => element.Name.LocalName == name);
            foreach (var group in source?.Elements().Where(IsWindowsGroup) ?? [])
            {
                target ??= AddCollection(metadata, source!.Name);
                if (target.Elements().Any(IsWindowsGroup))
                {
                    throw new InvalidDataException("Apple package already contains a Windows dependency group.");
                }

                target.Add(new XElement(group));
            }
        }

        var entries = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        long actualBytes = 0;
        foreach (var entry in apple.Entries)
        {
            var data = ReadEntry(entry);
            actualBytes = AddEntryBytes(actualBytes, data.Length);
            entries.Add(entry.FullName, data);
        }

        var imported = 0;
        foreach (var entry in windows.Entries)
        {
            if (!IsWindowsAsset(entry.FullName))
            {
                continue;
            }

            var data = ReadEntry(entry);
            actualBytes = AddEntryBytes(actualBytes, data.Length);
            if (!entries.TryAdd(entry.FullName, data))
            {
                throw new InvalidDataException("Native package contains duplicate platform assets.");
            }

            imported++;
        }

        if (imported == 0)
        {
            throw new InvalidDataException("Windows native assets are missing.");
        }

        MergeContentTypes(entries, windows);
        var manifestName = apple.Entries.Single(entry => entry.FullName.EndsWith(".nuspec", StringComparison.Ordinal)).FullName;
        using (var manifest = new MemoryStream())
        {
            appleSpec.Save(manifest);
            entries[manifestName] = manifest.ToArray();
        }

        using var result = new MemoryStream();
        using (var zip = new ZipArchive(result, ZipArchiveMode.Create, leaveOpen: true))
        {
            foreach (var entry in entries)
            {
                var created = zip.CreateEntry(entry.Key, CompressionLevel.Optimal);
                created.LastWriteTime = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
                using var stream = created.Open();
                stream.Write(entry.Value);
            }
        }

        return result.ToArray();
    }

    private static XDocument ReadManifest(ZipArchive archive, string version, string commit)
    {
        long bytes = 0;
        foreach (var entry in archive.Entries)
        {
            bytes = checked(bytes + entry.Length);
        }

        if (archive.Entries.Count > MaximumEntries || bytes > MaximumUncompressedBytes)
        {
            throw new InvalidDataException("Native artifact exceeds package composition limits.");
        }

        if (archive.Entries.Any(entry => entry.FullName == ".signature.p7s"))
        {
            throw new InvalidDataException("Native composition requires unsigned input artifacts.");
        }

        var manifest = archive.Entries.Single(entry => entry.FullName.EndsWith(".nuspec", StringComparison.Ordinal));
        using var stream = manifest.Open();
        var document = XDocument.Load(stream);
        var metadata = document.Root!.Elements().Single(element => element.Name.LocalName == "metadata");
        var id = metadata.Elements().Single(element => element.Name.LocalName == "id").Value;
        var actualVersion = metadata.Elements().Single(element => element.Name.LocalName == "version").Value;
        var repository = metadata.Elements().Single(element => element.Name.LocalName == "repository");
        if (id != PackageId || actualVersion != version || (string?)repository.Attribute("commit") != commit)
        {
            throw new InvalidDataException("Native artifact package identity, version or source commit does not match.");
        }

        return document;
    }

    private static XElement AddCollection(XElement metadata, XName name)
    {
        var collection = new XElement(name);
        metadata.Add(collection);
        return collection;
    }

    private static void MergeContentTypes(SortedDictionary<string, byte[]> entries, ZipArchive windows)
    {
        const string path = "[Content_Types].xml";
        if (!entries.TryGetValue(path, out var current))
        {
            throw new InvalidDataException("Apple native artifact has no package content types.");
        }

        using var currentStream = new MemoryStream(current);
        using var windowsStream = windows.GetEntry(path)?.Open()
            ?? throw new InvalidDataException("Windows native artifact has no package content types.");
        var target = XDocument.Load(currentStream);
        var source = XDocument.Load(windowsStream);
        foreach (var contentType in source.Root!.Elements())
        {
            var key = contentType.Name.LocalName == "Default" ? "Extension" : "PartName";
            var value = (string?)contentType.Attribute(key)
                ?? throw new InvalidDataException("Native artifact contains an invalid package content type.");
            if (key == "PartName" && !IsWindowsAsset(value.TrimStart('/')))
            {
                continue;
            }

            var existing = target.Root!.Elements().SingleOrDefault(element =>
                element.Name == contentType.Name && (string?)element.Attribute(key) == value);
            if (existing is null)
            {
                target.Root.Add(new XElement(contentType));
            }
            else if (!XNode.DeepEquals(existing, contentType))
            {
                throw new InvalidDataException("Host artifacts disagree on a package content type.");
            }
        }

        using var output = new MemoryStream();
        target.Save(output);
        entries[path] = output.ToArray();
    }

    private static bool IsWindowsGroup(XElement element) =>
        ((string?)element.Attribute("targetFramework"))?.Contains("windows", StringComparison.OrdinalIgnoreCase) == true;

    private static bool IsWindowsAsset(string path) =>
        path.StartsWith($"lib/{WindowsFramework}/", StringComparison.Ordinal)
        || path.StartsWith($"ref/{WindowsFramework}/", StringComparison.Ordinal);

    private static byte[] ReadEntry(ZipArchiveEntry entry)
    {
        using var stream = entry.Open();
        using var buffer = new MemoryStream();
        var block = new byte[16 * 1_024];
        int read;
        while ((read = stream.Read(block)) != 0)
        {
            if (buffer.Length + read > MaximumUncompressedBytes)
            {
                throw new InvalidDataException("Native artifact entry exceeds composition limits.");
            }

            buffer.Write(block.AsSpan(0, read));
        }

        return buffer.ToArray();
    }

    private static long AddEntryBytes(long current, int length)
    {
        var total = checked(current + length);
        if (total > MaximumUncompressedBytes)
        {
            throw new InvalidDataException("Complete native artifact exceeds composition limits.");
        }

        return total;
    }
}
