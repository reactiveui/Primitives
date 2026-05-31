// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;
using PublicApiGenerator;

namespace ReactiveUI.Primitives.ApiApproval;

/// <summary>
/// A helper for doing public API approvals via PublicApiGenerator and Verify. Linked into each
/// API-approval test project so the public surface is snapshotted per runtime and target framework.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class ApiApprovalExtensions
{
    /// <summary>
    /// Checks that the public API of the assembly matches the approved snapshot.
    /// </summary>
    /// <param name="assembly">The assembly that is being checked.</param>
    /// <param name="filePath">The caller file path.</param>
    /// <returns>A task to monitor the process.</returns>
    public static async Task CheckApproval(
        this Assembly assembly,
        [CallerFilePath] string filePath = "")
    {
        var generatorOptions = new ApiGeneratorOptions();
        var apiText = assembly.GeneratePublicApi(generatorOptions);
        _ = await Verifier.Verify(apiText, null, filePath)
            .UniqueForRuntimeAndVersion()
            .ScrubEmptyLines()
            .ScrubLines(static l =>
                l.StartsWith("[assembly: AssemblyVersion(", StringComparison.InvariantCulture) ||
                l.StartsWith("[assembly: AssemblyFileVersion(", StringComparison.InvariantCulture) ||
                l.StartsWith("[assembly: AssemblyInformationalVersion(", StringComparison.InvariantCulture) ||
                l.StartsWith("[assembly: System.Reflection.AssemblyMetadata(", StringComparison.InvariantCulture) ||
                l.StartsWith("[assembly: System.Runtime.Versioning.SupportedOSPlatform(", StringComparison.InvariantCulture) ||
                l.StartsWith("[assembly: System.Runtime.Versioning.TargetFramework(", StringComparison.InvariantCulture) ||
                l.StartsWith("[assembly: System.Runtime.Versioning.TargetPlatform(", StringComparison.InvariantCulture));
    }
}
