// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies every spelling of the static request attribute is recognised.</summary>
/// <remarks>
/// The attribute is matched on how it is written rather than on the symbol it binds to, because the type it names
/// is this generator's own output and output is invisible to the pipeline that produced it. That puts the burden of
/// understanding C#'s naming forms on this generator, so each one a consumer may reasonably write is pinned here.
/// </remarks>
public sealed partial class EventGeneratorTests
{
    /// <summary>The host every spelling in these tests points at.</summary>
    private const string StaticRequestHost = """

        namespace Samples
        {
            public static class EventSource
            {
                public static event System.Action<int>? GlobalChanged;
            }
        }
        """;

    /// <summary>Verifies the attribute written out in full is recognised.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorAcceptsAFullyQualifiedStaticRequest()
    {
        const string Source = """
            [assembly: ReactiveUI.Primitives.ObservableEvents.GenerateStaticEventObservables(
                typeof(Samples.EventSource))]
            """ + StaticRequestHost;

        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(Array.Exists(result.GeneratedHintNames, IsStaticHint)).IsTrue();
    }

    /// <summary>Verifies the attribute written with the suffix the language lets you omit is recognised.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorAcceptsTheSuffixedStaticRequestSpelling()
    {
        const string Source = """
            using ReactiveUI.Primitives.ObservableEvents;

            [assembly: GenerateStaticEventObservablesAttribute(typeof(Samples.EventSource))]
            """ + StaticRequestHost;

        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(Array.Exists(result.GeneratedHintNames, IsStaticHint)).IsTrue();
    }

    /// <summary>Verifies the attribute reached through a namespace alias is recognised.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorAcceptsAnAliasQualifiedStaticRequest()
    {
        const string Source = """
            using Events = ReactiveUI.Primitives.ObservableEvents;

            [assembly: Events::GenerateStaticEventObservables(typeof(Samples.EventSource))]
            """ + StaticRequestHost;

        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(Array.Exists(result.GeneratedHintNames, IsStaticHint)).IsTrue();
    }

    /// <summary>Verifies an unrelated generic assembly attribute is ignored rather than mistaken for a request.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorIgnoresAGenericAssemblyAttribute()
    {
        const string Source = """
            [assembly: Samples.Tagged<int>]

            namespace Samples
            {
                public sealed class TaggedAttribute<T> : System.Attribute
                {
                }

                public static class EventSource
                {
                    public static event System.Action<int>? GlobalChanged;
                }
            }
            """;

        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedHintNames.Any(IsEventWrapperHint)).IsFalse();
    }

    /// <summary>Determines whether a generated-source hint identifies a static wrapper.</summary>
    /// <param name="hintName">The hint name.</param>
    /// <returns><see langword="true"/> for a static wrapper hint.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsStaticHint(string hintName) =>
        hintName.EndsWith(StaticHintSuffix, StringComparison.Ordinal);
}
