// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies the reduced location a diagnostic carries through the pipeline.</summary>
public sealed class LocationInfoTests
{
    /// <summary>The source every compilation in these tests is built from.</summary>
    private const string SampleSource = "class Sample { }";

    /// <summary>The path that source is attributed to.</summary>
    private const string SamplePath = "Sample.cs";

    /// <summary>Verifies a source location survives the round trip to values and back.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LocationInfoRoundTripsASourceLocation()
    {
        var original = (await ParseSampleAsync()).GetLocation();

        var info = LocationInfo.From(original);

        await Assert.That(info).IsNotNull();
        var rebuilt = info!.ToLocation();
        await Assert.That(rebuilt.SourceSpan).IsEqualTo(original.SourceSpan);
        await Assert.That(rebuilt.GetLineSpan().Path).IsEqualTo(original.GetLineSpan().Path);
        await Assert.That(rebuilt.GetLineSpan().Span).IsEqualTo(original.GetLineSpan().Span);
    }

    /// <summary>Verifies a location with no source is reduced to nothing rather than a bogus span.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LocationInfoDiscardsALocationWithNoSource()
    {
        await Assert.That(LocationInfo.From(null)).IsNull();
        await Assert.That(LocationInfo.From(Location.None)).IsNull();
    }

    /// <summary>Verifies a syntax reference is reduced through the same path as a bare location.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LocationInfoReducesASyntaxReference()
    {
        var reference = (await ParseSampleAsync()).GetReference();

        var info = LocationInfo.From(reference, CancellationToken.None);

        await Assert.That(info).IsNotNull();
        await Assert.That(info!.FilePath).IsEqualTo(SamplePath);
    }

    /// <summary>Verifies an absent reference, as an attribute read from metadata has, reduces to nothing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LocationInfoDiscardsAnAbsentSyntaxReference() =>
        await Assert.That(LocationInfo.From(null, CancellationToken.None)).IsNull();

    /// <summary>Verifies two reductions of the same location compare equal, so a model stays cached.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LocationInfoComparesByValue()
    {
        var location = (await ParseSampleAsync()).GetLocation();

        await Assert.That(LocationInfo.From(location)).IsEqualTo(LocationInfo.From(location));
    }

    /// <summary>Parses the shared sample source.</summary>
    /// <returns>The parsed root node.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<SyntaxNode> ParseSampleAsync() =>
        CSharpSyntaxTree.ParseText(SampleSource, path: SamplePath).GetRootAsync();
}
