// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.ObservableEvents.CodeGeneration;

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies the naming that keeps generated wrappers and files from colliding.</summary>
public sealed class GeneratedNamesTests
{
    /// <summary>Verifies punctuation is folded to underscores and a hash keeps the identity.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GeneratedNamesFoldsPunctuationAndKeepsAHash()
    {
        var name = GeneratedNames.WrapperName("global::Samples.EventSource");

        await Assert.That(name).StartsWith("Rxglobal__Samples_EventSource_");
        await Assert.That(name).EndsWith("Events");
    }

    /// <summary>Verifies leading and trailing punctuation is dropped rather than left as underscores.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GeneratedNamesTrimsLeadingAndTrailingPunctuation()
    {
        var trimmed = GeneratedNames.WrapperName("::Samples.EventSource.");
        var bare = GeneratedNames.WrapperName("Samples.EventSource");

        await Assert.That(trimmed).StartsWith("RxSamples_EventSource_");

        // Same readable component, different identity: only the hash may distinguish them.
        await Assert.That(trimmed).IsNotEqualTo(bare);
    }

    /// <summary>Verifies an identity with nothing nameable still produces a usable identifier.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GeneratedNamesHandlesAnIdentityWithNothingNameable()
    {
        var empty = GeneratedNames.StaticHintName(string.Empty);
        var punctuationOnly = GeneratedNames.StaticHintName("::");

        await Assert.That(empty).StartsWith("ObservableEvents._");
        await Assert.That(empty).EndsWith(".Static.g.cs");
        await Assert.That(punctuationOnly).StartsWith("ObservableEvents._");
        await Assert.That(punctuationOnly).IsNotEqualTo(empty);
    }

    /// <summary>Verifies sanitized-alike identities keep distinct file names, which duplicate hints would break.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GeneratedNamesKeepsSanitizedAlikeIdentitiesApart()
    {
        var first = GeneratedNames.InstanceHintName("global::Samples.A_B.C");
        var second = GeneratedNames.InstanceHintName("global::Samples.A.B_C");

        await Assert.That(first).IsNotEqualTo(second);
        await Assert.That(first).EndsWith(".Instance.g.cs");
        await Assert.That(second).EndsWith(".Instance.g.cs");
    }

    /// <summary>Verifies the same identity always produces the same name, so rebuilds stay deterministic.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GeneratedNamesAreDeterministic()
    {
        const string Identity = "global::Samples.EventSource";

        await Assert.That(GeneratedNames.InstanceHintName(Identity))
            .IsEqualTo(GeneratedNames.InstanceHintName(Identity));
        await Assert.That(GeneratedNames.WrapperName(Identity)).IsEqualTo(GeneratedNames.WrapperName(Identity));
    }
}
