// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text;
using System.Xml.Linq;
using OccasionallyConnected.Ci;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests host-specific native package composition.</summary>
public sealed class MobileNativePackageTests
{
    /// <summary>The exact fixture package version.</summary>
    private const string Version = "0.1.0-native-test";

    /// <summary>The exact fixture source commit.</summary>
    private const string Commit = "1234567890123456789012345678901234567890";

    /// <summary>The complete native package identity.</summary>
    private const string PackageId = "ReactiveUI.Primitives.OccasionallyConnected.Mobile";

    /// <summary>The Windows asset folder.</summary>
    private const string WindowsFramework = "net10.0-windows10.0.19041";

    /// <summary>The package extension.</summary>
    private const string PackageExtension = "nupkg";

    /// <summary>The symbol extension.</summary>
    private const string SymbolsExtension = "snupkg";

    /// <summary>Verifies Windows assets and dependency groups are combined with Apple assets and symbols.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MergePreservesHostAssetsAndExactPackageIdentity()
    {
        using var files = new MergeFiles();
        files.CreateInputs();
        MobileNativePackage.Merge(files.Windows, files.Apple, files.Output, Version, Commit);
        foreach (var extension in new[] { PackageExtension, SymbolsExtension })
        {
            var fileName = extension == SymbolsExtension ? "Mobile.pdb" : "Mobile.dll";
            var archive = ReadArchive(await File.ReadAllBytesAsync(MergeFiles.Package(files.Output, extension)));
            await Assert.That(archive.Assets).Contains($"lib/{WindowsFramework}/{fileName}");
            await Assert.That(archive.Assets).Contains($"lib/net10.0-ios/{fileName}");
            await Assert.That(archive.Assets).Contains($"lib/net10.0-maccatalyst/{fileName}");
            await Assert.That(archive.Assets).Contains($"lib/net10.0-android/{fileName}");
            var groups = archive.Manifest.Descendants("group").ToArray();
            await Assert.That(Array.Exists(groups, static group => ((string?)group.Attribute("targetFramework")) == WindowsFramework)).IsTrue();
            if (extension != PackageExtension)
            {
                continue;
            }

            await Assert.That(archive.Assets).Contains($"lib/{WindowsFramework}/Mobile.pri");
            await Assert.That(archive.ContentTypes.Descendants("Default").Any(
                static element => (string?)element.Attribute("Extension") == "pri")).IsTrue();
        }
    }

    /// <summary>Verifies an independently produced artifact cannot inject another release's code.</summary>
    /// <param name="wrongVersion">Whether the package version differs.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task MergeRejectsMismatchedReleaseIdentity(bool wrongVersion)
    {
        using var files = new MergeFiles();
        files.CreateInputs(wrongVersion ? "0.2.0" : Version, wrongVersion ? Commit : "different-source");

        await Assert.That(() => MobileNativePackage.Merge(files.Windows, files.Apple, files.Output, Version, Commit))
            .ThrowsExactly<InvalidDataException>();
        await Assert.That(Directory.Exists(files.Output)).IsFalse();
    }

    /// <summary>Verifies missing host output fails before any complete artifact appears.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MergeRequiresBothPackageAndSymbolArtifacts()
    {
        using var files = new MergeFiles();
        files.CreateInputs();
        File.Delete(MergeFiles.Package(files.Windows, SymbolsExtension));

        await Assert.That(() => MobileNativePackage.Merge(files.Windows, files.Apple, files.Output, Version, Commit))
            .ThrowsExactly<FileNotFoundException>();
        await Assert.That(Directory.Exists(files.Output)).IsFalse();
    }

    /// <summary>Verifies identical inputs produce identical combined artifacts.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MergeIsDeterministic()
    {
        using var files = new MergeFiles();
        files.CreateInputs();
        MobileNativePackage.Merge(files.Windows, files.Apple, files.Output, Version, Commit);
        var other = $"{files.Output}-second";
        MobileNativePackage.Merge(files.Windows, files.Apple, other, Version, Commit);
        var first = await File.ReadAllBytesAsync(MergeFiles.Package(files.Output, PackageExtension));
        var second = await File.ReadAllBytesAsync(MergeFiles.Package(other, PackageExtension));

        await Assert.That(first.SequenceEqual(second)).IsTrue();
    }

    /// <summary>Verifies signed host input cannot be rewritten into a misleading combined signature.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MergeRejectsAlreadySignedInput()
    {
        using var files = new MergeFiles();
        files.CreateInputs();
        AddSignature(MergeFiles.Package(files.Windows, PackageExtension));

        await Assert.That(() => MobileNativePackage.Merge(files.Windows, files.Apple, files.Output, Version, Commit))
            .ThrowsExactly<InvalidDataException>();
        await Assert.That(Directory.Exists(files.Output)).IsFalse();
    }

    /// <summary>Adds a synthetic existing package signature.</summary>
    /// <param name="path">The host package.</param>
    private static void AddSignature(string path)
    {
        using var zip = ZipFile.Open(path, ZipArchiveMode.Update);
        using var signature = zip.CreateEntry(".signature.p7s").Open();
        signature.Write("signed"u8);
    }

    /// <summary>Inspects in-memory archive data without synchronous filesystem operations.</summary>
    /// <param name="bytes">The archive bytes.</param>
    /// <returns>The asset names and manifest.</returns>
    private static (string[] Assets, XDocument Manifest, XDocument ContentTypes) ReadArchive(byte[] bytes)
    {
        using var buffer = new MemoryStream(bytes);
        using var zip = new ZipArchive(buffer, ZipArchiveMode.Read);
        using var stream = zip.GetEntry($"{PackageId}.nuspec")!.Open();
        using var contentTypes = zip.GetEntry("[Content_Types].xml")!.Open();
        return (zip.Entries.Select(static entry => entry.FullName).ToArray(), XDocument.Load(stream), XDocument.Load(contentTypes));
    }

    /// <summary>Owns temporary unsigned package fixtures.</summary>
    private sealed class MergeFiles : IDisposable
    {
        /// <summary>The owned temporary directory.</summary>
        private readonly string _root = Path.Combine(Path.GetTempPath(), $"rxui-native-merge-{Guid.NewGuid():N}");

        /// <summary>Initializes a new instance of the <see cref="MergeFiles"/> class.</summary>
        internal MergeFiles()
        {
            _ = Directory.CreateDirectory(Windows);
            _ = Directory.CreateDirectory(Apple);
        }

        /// <summary>Gets Windows artifacts.</summary>
        internal string Windows => Path.Combine(_root, "windows");

        /// <summary>Gets Apple artifacts.</summary>
        internal string Apple => Path.Combine(_root, "apple");

        /// <summary>Gets the new complete output path.</summary>
        internal string Output => Path.Combine(_root, "complete");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Directory.Delete(_root, recursive: true);

        /// <summary>Gets a fixture package path.</summary>
        /// <param name="directory">The package directory.</param>
        /// <param name="extension">The package extension.</param>
        /// <returns>The full path.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static string Package(string directory, string extension) => Path.Combine(directory, $"{PackageId}.{Version}.{extension}");

        /// <summary>Creates both host package and symbol inputs.</summary>
        /// <param name="appleVersion">The Apple manifest version.</param>
        /// <param name="appleCommit">The Apple source commit.</param>
        internal void CreateInputs(string appleVersion = Version, string appleCommit = Commit)
        {
            foreach (var extension in new[] { PackageExtension, SymbolsExtension })
            {
                WriteArchive(Package(Windows, extension), Version, Commit, [WindowsFramework]);
                WriteArchive(Package(Apple, extension), appleVersion, appleCommit, ["net10.0-android", "net10.0-ios", "net10.0-maccatalyst"]);
            }
        }

        /// <summary>Writes a minimal unsigned host artifact.</summary>
        /// <param name="path">The output path.</param>
        /// <param name="version">The manifest version.</param>
        /// <param name="commit">The source commit.</param>
        /// <param name="frameworks">The platform assets.</param>
        private static void WriteArchive(string path, string version, string commit, string[] frameworks)
        {
            using var zip = ZipFile.Open(path, ZipArchiveMode.Create);
            var manifest = new XElement(
                "package",
                new XElement(
                    "metadata",
                    new XElement("id", PackageId),
                    new XElement(nameof(version), version),
                    new XElement("repository", new XAttribute(nameof(commit), commit)),
                    new XElement("dependencies", frameworks.Select(static framework => new XElement("group", new XAttribute("targetFramework", framework))))));
            using (var stream = zip.CreateEntry($"{PackageId}.nuspec").Open())
            {
                var bytes = Encoding.UTF8.GetBytes(manifest.ToString());
                stream.Write(bytes);
            }

            foreach (var framework in frameworks)
            {
                var fileName = Path.GetExtension(path) == $".{SymbolsExtension}" ? "Mobile.pdb" : "Mobile.dll";
                using var stream = zip.CreateEntry($"lib/{framework}/{fileName}").Open();
                stream.Write(Encoding.UTF8.GetBytes(framework));
            }

            var includesWindowsResources = Path.GetExtension(path) == $".{PackageExtension}" && Array.Exists(
                frameworks,
                static framework => framework == WindowsFramework);
            if (includesWindowsResources)
            {
                using var resource = zip.CreateEntry($"lib/{WindowsFramework}/Mobile.pri").Open();
                resource.Write("windows-resource"u8);
            }

            using var contentStream = zip.CreateEntry("[Content_Types].xml").Open();
            if (includesWindowsResources)
            {
                contentStream.Write("<Types><Default Extension=\"dll\" ContentType=\"application/octet\" /><Default Extension=\"pri\" ContentType=\"application/octet\" /></Types>"u8);
            }
            else
            {
                contentStream.Write("<Types><Default Extension=\"dll\" ContentType=\"application/octet\" /></Types>"u8);
            }
        }
    }
}
