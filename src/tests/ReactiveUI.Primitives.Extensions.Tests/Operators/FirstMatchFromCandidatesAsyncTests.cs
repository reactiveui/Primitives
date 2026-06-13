// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests covering the async-sink path of
/// <c>FirstMatchFromCandidates</c> (when projections do not complete synchronously)
/// and edge cases not exercised by the sync fast-path tests.</summary>
public class FirstMatchFromCandidatesAsyncTests
{
    /// <summary>Fallback value emitted when no candidate satisfies the predicate.</summary>
    private const string Fallback = "fallback";

    /// <summary>Sentinel raw value used by tests that drive an async projection to a matching emission.</summary>
    private const string MatchRaw = "match";

    /// <summary>Verifies that the empty-candidate fast path emits the fallback and completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCandidatesEmpty_ThenEmitsFallback()
    {
        var results = new List<string>();
        var completed = false;
        using var sub = Array.Empty<string>().FirstMatchFromCandidates(static _ => Observable.Empty<string>(), static raw => raw, static _ => true, Fallback).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([Fallback]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that when the projection is asynchronous and emits a matching value,
    /// the downstream receives that transformed value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncProjectionEmitsMatch_ThenEmitsTransformedValue()
    {
        using var s = new Subject<string>();
        var results = new List<string>();
        var keys = new[]
        {
            "k"
        };
        using var sub = keys.FirstMatchFromCandidates(_ => s, static raw => raw.ToUpperInvariant(), static transformed => transformed.StartsWith('M'), Fallback).Subscribe(results.Add);
        s.OnNext("match");
        await Assert.That(results).IsCollectionEqualTo(["MATCH"]);
    }

    /// <summary>Verifies that when every async projection completes without a match the fallback is emitted.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncProjectionsCompleteWithoutMatch_ThenEmitsFallback()
    {
        var s1 = new Subject<string>();
        var s2 = new Subject<string>();
        var subjects = new[]
        {
            s1,
            s2
        };
        var results = new List<string>();
        var completed = false;
        var keys = new[]
        {
            0,
            1
        };
        using var sub = keys.FirstMatchFromCandidates(i => subjects[i], static raw => raw, static _ => false, Fallback).Subscribe(results.Add, () => completed = true);
        s1.OnCompleted();
        s2.OnCompleted();
        await Assert.That(results).IsCollectionEqualTo([Fallback]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that an async projection's error skips the candidate and the next is tried.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncProjectionErrors_ThenSkipsAndContinues()
    {
        var s1 = new Subject<string>();
        var s2 = new Subject<string>();
        var subjects = new[]
        {
            s1,
            s2
        };
        var results = new List<string>();
        var keys = new[]
        {
            0,
            1
        };
        using var sub = keys.FirstMatchFromCandidates(i => subjects[i], static raw => raw, static raw => raw == MatchRaw, Fallback).Subscribe(results.Add);
        s1.OnError(new InvalidOperationException("ignored"));
        s2.OnNext(MatchRaw);
        await Assert.That(results).IsCollectionEqualTo([MatchRaw]);
    }

    /// <summary>Verifies that when the transform throws on the async path the candidate is skipped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncTransformThrows_ThenValueIsIgnoredAndPipelineContinues()
    {
        using var s = new Subject<string>();
        var results = new List<string>();
        var keys = new[]
        {
            "k"
        };
        using var sub = keys.FirstMatchFromCandidates(
            _ => s,
            static raw =>
        {
            if (raw == "bad")
            {
                throw new InvalidOperationException("transform failed");
            }

            return raw.ToUpperInvariant();
        },
            static t => t.StartsWith('G'),
            Fallback).Subscribe(results.Add);
        s.OnNext("bad");
        s.OnNext("good");
        await Assert.That(results).IsCollectionEqualTo(["GOOD"]);
    }

    /// <summary>Verifies that disposing the async sink before the projection emits prevents any downstream notifications.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncSinkDisposedBeforeEmission_ThenNoDownstreamNotifications()
    {
        using var s = new Subject<string>();
        var results = new List<string>();
        var completed = false;
        var keys = new[]
        {
            "k"
        };
        var sub = keys.FirstMatchFromCandidates(_ => s, static raw => raw, static _ => true, Fallback).Subscribe(results.Add, () => completed = true);
        sub.Dispose();
        s.OnNext("ignored");
        s.OnCompleted();
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies that a thrown projection on the async path is skipped and the loop continues to the next candidate.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncProjectionFactoryThrows_ThenSkipsAndContinues()
    {
        using var asyncFirst = new Subject<string>();
        var results = new List<string>();
        var keys = new[]
        {
            "async-open",
            "throw-key",
            "match"
        };
        using var sub = keys.FirstMatchFromCandidates(
            key => key switch
        {
            "async-open" => asyncFirst,
            "throw-key" => throw new InvalidOperationException("project failed"),
            _ => Observable.Return(key),
        },
            static raw => raw.ToUpperInvariant(),
            static t => t == "MATCH",
            Fallback).Subscribe(results.Add);

        // Trigger async sink: complete the first projection so AsyncSink.TryNext walks the rest.
        asyncFirst.OnCompleted();
        await Assert.That(results).IsCollectionEqualTo(["MATCH"]);
    }
}
