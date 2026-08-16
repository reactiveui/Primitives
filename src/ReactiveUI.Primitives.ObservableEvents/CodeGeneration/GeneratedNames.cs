// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.ObservableEvents.CodeGeneration;

/// <summary>Builds the wrapper class and generated file names, from the identity of the host they belong to.</summary>
/// <remarks>
/// A readable name alone is not enough to key generated output on. Sanitizing punctuation out of a fully qualified
/// name maps distinct hosts onto the same identifier - <c>Samples.A_B.C</c> and <c>Samples.A.B_C</c> both flatten to
/// <c>Samples_A_B_C</c> - so a hash of the unflattened identity is appended to keep them apart, while the readable
/// half is kept so a generated file is still recognisable in a build log.
/// </remarks>
internal static class GeneratedNames
{
    /// <summary>The prefix of a generated wrapper class name.</summary>
    private const string WrapperPrefix = "Rx";

    /// <summary>The suffix of a generated wrapper class name.</summary>
    private const string WrapperSuffix = "Events";

    /// <summary>The prefix shared by every generated file name.</summary>
    private const string HintPrefix = "ObservableEvents.";

    /// <summary>The suffix of a generated instance wrapper file name.</summary>
    private const string InstanceHintSuffix = ".Instance.g.cs";

    /// <summary>The suffix of a generated static wrapper file name.</summary>
    private const string StaticHintSuffix = ".Static.g.cs";

    /// <summary>The offset basis of the FNV-1a hash that keeps sanitized names apart.</summary>
    private const ulong HashOffsetBasis = 14_695_981_039_346_656_037;

    /// <summary>The prime of the FNV-1a hash that keeps sanitized names apart.</summary>
    private const ulong HashPrime = 1_099_511_628_211;

    /// <summary>Builds the wrapper class name for a host.</summary>
    /// <param name="identity">The host's fully qualified name.</param>
    /// <returns>The wrapper class name.</returns>
    internal static string WrapperName(string identity)
    {
        var builder = new PooledStringBuilder(identity.Length + WrapperPrefix.Length + WrapperSuffix.Length);
        _ = builder.Append(WrapperPrefix);
        AppendUniqueComponent(builder, identity);
        return builder.Append(WrapperSuffix).ToStringAndReturn();
    }

    /// <summary>Builds the generated file name for an instance wrapper.</summary>
    /// <param name="identity">The host's fully qualified name.</param>
    /// <returns>The generated file name.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string InstanceHintName(string identity) => HintName(identity, InstanceHintSuffix);

    /// <summary>Builds the generated file name for one namespace's static wrappers.</summary>
    /// <param name="namespaceName">The namespace, or an empty string for the global namespace.</param>
    /// <returns>The generated file name.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string StaticHintName(string namespaceName) => HintName(namespaceName, StaticHintSuffix);

    /// <summary>Appends the readable, collision-resistant component of a generated name.</summary>
    /// <param name="builder">The destination builder.</param>
    /// <param name="identity">The identity to render.</param>
    private static void AppendUniqueComponent(PooledStringBuilder builder, string identity)
    {
        AppendSanitized(builder, identity);
        _ = builder.Append('_').Append(StableHash(identity));
    }

    /// <summary>Appends an identity with everything that cannot appear in an identifier folded to underscores.</summary>
    /// <param name="builder">The destination builder.</param>
    /// <param name="identity">The identity to sanitize.</param>
    private static void AppendSanitized(PooledStringBuilder builder, string identity)
    {
        var start = 0;
        var end = identity.Length;

        // Leading and trailing punctuation would sanitize to underscores that carry no information, and a
        // generated name reads better without them; the hash still separates identities that differ only there.
        while (start < end && !char.IsLetterOrDigit(identity[start]))
        {
            start++;
        }

        while (end > start && !char.IsLetterOrDigit(identity[end - 1]))
        {
            end--;
        }

        for (var index = start; index < end; index++)
        {
            var character = identity[index];
            _ = builder.Append(char.IsLetterOrDigit(character) ? character : '_');
        }
    }

    /// <summary>Builds a generated file name from an identity and a category suffix.</summary>
    /// <param name="identity">The identity the file is keyed on.</param>
    /// <param name="suffix">The category suffix.</param>
    /// <returns>The generated file name.</returns>
    private static string HintName(string identity, string suffix)
    {
        var builder = new PooledStringBuilder(identity.Length + HintPrefix.Length + suffix.Length);
        _ = builder.Append(HintPrefix);
        AppendUniqueComponent(builder, identity);
        return builder.Append(suffix).ToStringAndReturn();
    }

    /// <summary>Computes a deterministic FNV-1a hash of an identity.</summary>
    /// <param name="identity">The identity to hash.</param>
    /// <returns>The invariant uppercase hexadecimal hash.</returns>
    private static string StableHash(string identity)
    {
        var hash = HashOffsetBasis;
        foreach (var character in identity)
        {
            hash ^= character;
            hash *= HashPrime;
        }

        return hash.ToString("X16", CultureInfo.InvariantCulture);
    }
}
