// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO.Compression;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Xml.Linq;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Tests native-head declarations and CI-produced package assets.</summary>
public sealed partial class MobileSqliteStorageTests
{
    /// <summary>The Windows platform revision omitted from NuGet folder names.</summary>
    private const string ZeroRevisionSuffix = ".0";

    /// <summary>The Mobile NuGet and assembly identifier.</summary>
    private const string MobilePackageId = "ReactiveUI.Primitives.OccasionallyConnected.Mobile";

    /// <summary>The supported native package heads.</summary>
    private static readonly string[] NativeHeads =
    [
        "net10.0-android", "net10.0-windows10.0.19041.0", "net10.0-ios", "net10.0-maccatalyst",
    ];

    /// <summary>Checks supported hosts include stable native heads without an opt-in property.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task DefaultNativeHeadsHaveMatchingPublicApiBaselines()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "CLAUDE.md")))
        {
            directory = directory.Parent;
        }

        await Assert.That(directory).IsNotNull();
        var projectRoot = Path.Combine(directory!.FullName, "src", "ReactiveUI.Primitives.OccasionallyConnected.Mobile");
        var project = XDocument.Load(Path.Combine(projectRoot, "ReactiveUI.Primitives.OccasionallyConnected.Mobile.csproj"));
        await Assert.That(project.Descendants("TargetFrameworks").First().Value).IsEqualTo("$(MauiTargetFrameworks)");
        var defaults = project.Descendants("MobilePlatformTargetFrameworks").Select(static property => property.Value).ToArray();
        var frameworks = defaults.SelectMany(static value => value.Split(';')).ToArray();
        await Assert.That(frameworks.SequenceEqual(NativeHeads)).IsTrue();
        foreach (var framework in frameworks)
        {
            var api = await File.ReadAllTextAsync(Path.Combine(projectRoot, "PublicAPI", framework, "PublicAPI.txt"));
            await Assert.That(api).Contains("public static class MauiMobileServices");
        }
    }

    /// <summary>Checks native package libraries really contain the native convenience entry point.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task PackedNativeHeadsContainPlatformConvenienceApi()
    {
        var package = Environment.GetEnvironmentVariable("RXUI_MOBILE_NATIVE_PACKAGE");
        if (string.IsNullOrWhiteSpace(package))
        {
            Skip.Test("The native CI job supplies its freshly built package for this asset check.");
            return;
        }

        var expected = Environment.GetEnvironmentVariable("RXUI_MOBILE_NATIVE_HEADS");
        await Assert.That(expected).IsNotNull();
        await using var archive = await ZipFile.OpenReadAsync(package);
        var heads = expected!.Split(';');
        var libraries = archive.Entries.Where(static entry =>
            entry.FullName.StartsWith("lib/", StringComparison.Ordinal)
            && entry.FullName.EndsWith($"/{MobilePackageId}.dll", StringComparison.Ordinal)).ToArray();
        await Assert.That(libraries.Length).IsEqualTo(heads.Length + 1);
        foreach (var framework in heads)
        {
            var packageFramework = framework.EndsWith(ZeroRevisionSuffix, StringComparison.Ordinal)
                && framework.Contains("-windows", StringComparison.Ordinal)
                ? framework[..^ZeroRevisionSuffix.Length]
                : framework;
            var library = libraries.Single(entry =>
                entry.FullName.StartsWith($"lib/{packageFramework}", StringComparison.Ordinal)
                && entry.FullName.EndsWith($"/{MobilePackageId}.dll", StringComparison.Ordinal));
            await using var content = await library.OpenAsync();
            await using var bytes = new MemoryStream();
            await content.CopyToAsync(bytes);
            bytes.Position = 0;
            using var reader = new PEReader(bytes, PEStreamOptions.LeaveOpen);
            var metadata = reader.GetMetadataReader();
            var services = metadata.TypeDefinitions.Select(metadata.GetTypeDefinition).Single(type =>
                metadata.GetString(type.Name) == "MauiMobileServices"
                && metadata.GetString(type.Namespace) == typeof(MobileSqliteStorage).Namespace);
            var methods = services.GetMethods().Select(metadata.GetMethodDefinition).ToArray();
            await Assert.That(Array.Exists(methods, method =>
                metadata.GetString(method.Name) == "CreateSqliteStorageAsync"
                && (method.Attributes & (MethodAttributes.Public | MethodAttributes.Static))
                    == (MethodAttributes.Public | MethodAttributes.Static))).IsTrue();
            await Assert.That(Array.Exists(methods, method =>
                metadata.GetString(method.Name) == "CreateConnectivityHint"
                && (method.Attributes & (MethodAttributes.Public | MethodAttributes.Static))
                    == (MethodAttributes.Public | MethodAttributes.Static))).IsTrue();
        }

        await Assert.That(archive.Entries.Any(static entry =>
            entry.FullName == "lib/net10.0/ReactiveUI.Primitives.OccasionallyConnected.Mobile.dll")).IsTrue();
        await VerifyPackageIdentityAsync(archive);
    }

    /// <summary>Checks the native package has the release version and source commit used by the job.</summary>
    /// <param name="archive">The freshly packed native package.</param>
    /// <returns>The identity check completion.</returns>
    private static async Task VerifyPackageIdentityAsync(ZipArchive archive)
    {
        var entry = archive.Entries.Single(static item => item.FullName.EndsWith(".nuspec", StringComparison.Ordinal));
        await using var content = await entry.OpenAsync();
        var manifest = await XDocument.LoadAsync(content, LoadOptions.None, CancellationToken.None);
        await Assert.That(manifest.Descendants().Single(static element => element.Name.LocalName == "id").Value)
            .IsEqualTo(MobilePackageId);
        var expectedVersion = Environment.GetEnvironmentVariable("RXUI_MOBILE_PACKAGE_VERSION");
        if (!string.IsNullOrWhiteSpace(expectedVersion))
        {
            await Assert.That(manifest.Descendants().Single(static element => element.Name.LocalName == "version").Value)
                .IsEqualTo(expectedVersion);
        }

        var expectedCommit = Environment.GetEnvironmentVariable("RXUI_MOBILE_PACKAGE_COMMIT");
        if (!string.IsNullOrWhiteSpace(expectedCommit))
        {
            var repository = manifest.Descendants().Single(static element => element.Name.LocalName == "repository");
            await Assert.That((string?)repository.Attribute("commit")).IsEqualTo(expectedCommit);
        }
    }
}
