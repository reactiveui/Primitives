// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests asynchronous candidate matching, fallback, errors, and cancellation.</summary>
public class FirstMatchFromCandidatesAsyncPathTests
{
    /// <summary>Fallback value emitted when no candidate matches.</summary>
    private const string Fallback = "fallback";

    /// <summary>Candidate key whose projection is an async (never-sync-completing) subject.</summary>
    private const string AsyncKey = "async";

    /// <summary>Candidate key whose projection is a synchronously-erroring observable.</summary>
    private const string SyncErrorKey = "sync-error";

    /// <summary>Candidate key whose projection is a synchronously-completing empty observable.</summary>
    private const string SyncCompleteKey = "sync-complete";

    /// <summary>Candidate key whose projection emits the match value.</summary>
    private const string HitKey = "hit";

    /// <summary>Verifies that an empty candidate list emits the fallback and completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCandidatesEmpty_ThenEmitsFallbackAndCompletes()
    {
        List<string> results = [];
        var completed = false;
        using var sub = Array.Empty<string>()
            .FirstMatchFromCandidates(
                static _ => Observable.Empty<string>(),
                static raw => raw,
                static value => value.Length > 0,
                Fallback).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([Fallback]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies an async projection whose value matches the predicate emits it and completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncProjectionMatches_ThenEmitsMatch()
    {
        string[] keys = ["miss", HitKey];
        Subject<string> emissionGate = new();
        List<string> results = [];
        TaskCompletionSource<bool> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = ((IReadOnlyList<string>)keys)
            .FirstMatchFromCandidates(
                key => key == HitKey ? emissionGate : Observable.Empty<string>(),
                static raw => raw,
                static value => value == HitKey,
                Fallback).Subscribe(results.Add, () => completed.TrySetResult(true));
        emissionGate.OnNext(HitKey);
        emissionGate.OnCompleted();
        var done = await completed.Task;
        await Assert.That(done).IsTrue();
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
    }

    /// <summary>Verifies an async projection that completes without a match falls through to the fallback.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncProjectionNeverMatches_ThenFallback()
    {
        string[] keys = ["only"];
        Subject<string> subject = new();
        List<string> results = [];
        TaskCompletionSource<bool> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = ((IReadOnlyList<string>)keys)
            .FirstMatchFromCandidates(
                _ => subject,
                static raw => raw,
                static value => value == "match-impossible",
                Fallback).Subscribe(results.Add, () => completed.TrySetResult(true));
        subject.OnNext("nope");
        subject.OnCompleted();
        await completed.Task;
        await Assert.That(results).IsCollectionEqualTo([Fallback]);
    }

    /// <summary>Verifies an async projection error is swallowed and the walk moves to the next candidate.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncProjectionErrors_ThenSkipsToNextCandidate()
    {
        string[] keys = ["bad", "good"];
        Subject<string> badSubject = new();
        Subject<string> goodSubject = new();
        List<string> results = [];
        TaskCompletionSource<bool> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = ((IReadOnlyList<string>)keys)
            .FirstMatchFromCandidates(
                key => key == "bad" ? badSubject : goodSubject,
                static raw => raw,
                static value => value == "good",
                Fallback).Subscribe(results.Add, () => completed.TrySetResult(true));
        badSubject.OnError(new InvalidOperationException("bad failed"));
        goodSubject.OnNext("good");
        goodSubject.OnCompleted();
        await completed.Task;
        await Assert.That(results).IsCollectionEqualTo(["good"]);
    }

    /// <summary>Verifies that disposing during the async walk stops further candidate processing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedDuringAsyncWalk_ThenStops()
    {
        string[] keys = ["k1", "k2"];
        Subject<string> firstSubject = new();
        List<string> results = [];
        var completed = false;
        var sub = ((IReadOnlyList<string>)keys)
            .FirstMatchFromCandidates(_ => firstSubject, static raw => raw, static _ => true, Fallback)
            .Subscribe(results.Add, () => completed = true);
        sub.Dispose();

        sub.Dispose();
        firstSubject.OnNext("late");
        firstSubject.OnCompleted();
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies a synchronous transform that throws for one candidate lets the next be tried.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncTransformThrows_ThenContinuesToNextCandidate()
    {
        string[] keys = ["throw", HitKey];
        List<string> results = [];
        var completed = false;
        using var sub = ((IReadOnlyList<string>)keys).FirstMatchFromCandidates(
            Observable.Return,
            static raw => raw == "throw" ? throw new InvalidOperationException("transform-throws") : raw,
            static value => value == HitKey,
            Fallback).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies a candidate that errors from inside its own <c>Subscribe</c> gives way to the next.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncCandidateProjectionSyncErrors_ThenLoopingGuardSkipsToNextCandidate()
    {
        string[] keys = [SyncErrorKey, HitKey];
        List<string> results = [];
        var completed = false;
        using var sub = ((IReadOnlyList<string>)keys)
            .FirstMatchFromCandidates(
                static key => key switch
                {
                    SyncErrorKey => new SyncErroringObservable<string>(new InvalidOperationException(SyncErrorKey)),
                    _ => Observable.Return(key)
                },
                static raw => raw,
                static value => value == HitKey,
                Fallback).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies a candidate that completes from inside its own <c>Subscribe</c> gives way to the next.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncCandidateProjectionSyncCompletes_ThenLoopingGuardSkipsToNextCandidate()
    {
        string[] keys = [SyncCompleteKey, HitKey];
        List<string> results = [];
        var completed = false;
        using var sub = ((IReadOnlyList<string>)keys)
            .FirstMatchFromCandidates(
                static key => key == SyncCompleteKey ? new SyncCompletingObservable<string>() : Observable.Return(key),
                static raw => raw,
                static value => value == HitKey,
                Fallback).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies candidate notifications arriving after the match has fired are silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncCandidateEmitsAfterMatch_ThenDroppedByDoneGuard()
    {
        string[] keys = [HitKey];
        Subject<string> subject = new();
        List<string> results = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = ((IReadOnlyList<string>)keys)
            .FirstMatchFromCandidates(_ => subject, static raw => raw, static value => value == HitKey, Fallback)
            .Subscribe(results.Add, () => completed.TrySetResult());
        subject.OnNext(HitKey);
        await completed.Task;
        subject.OnNext("ignored-late");
        subject.OnError(new InvalidOperationException("ignored-late"));
        subject.OnCompleted();
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
    }

    /// <summary>Verifies a mid-walk candidate that errors from inside <c>Subscribe</c> lets the walk reach the match.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncSinkWalkHitsSyncErroringCandidate_ThenLoopingGuardSkipsAhead()
    {
        string[] keys = [AsyncKey, SyncErrorKey, HitKey];
        Subject<string> asyncSubject = new();
        List<string> results = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = ((IReadOnlyList<string>)keys).FirstMatchFromCandidates(
            key => key switch
            {
                AsyncKey => asyncSubject,
                SyncErrorKey => new SyncErroringObservable<string>(new InvalidOperationException("sync")),
                _ => Observable.Return(key)
            },
            static raw => raw,
            static value => value == HitKey,
            Fallback).Subscribe(results.Add, () => completed.TrySetResult());

        // Completing the first candidate is what advances the walk onto the re-entrant one.
        asyncSubject.OnCompleted();
        await completed.Task;
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
    }

    /// <summary>Verifies a mid-walk candidate that completes from inside <c>Subscribe</c> lets the walk reach the match.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncSinkWalkHitsSyncCompletingCandidate_ThenLoopingGuardSkipsAhead()
    {
        string[] keys = [AsyncKey, SyncCompleteKey, HitKey];
        Subject<string> asyncSubject = new();
        List<string> results = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = ((IReadOnlyList<string>)keys).FirstMatchFromCandidates(
            key => key switch
            {
                AsyncKey => asyncSubject,
                SyncCompleteKey => new SyncCompletingObservable<string>(),
                _ => Observable.Return(key)
            },
            static raw => raw,
            static value => value == HitKey,
            Fallback).Subscribe(results.Add, () => completed.TrySetResult());
        asyncSubject.OnCompleted();
        await completed.Task;
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
    }

    /// <summary>Observable that calls <c>OnError</c> on its subscriber from inside <c>Subscribe</c>.</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    /// <param name = "error">The exception to deliver to the subscriber.</param>
    private sealed class SyncErroringObservable<T>(Exception error) : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnError(error);
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Observable that calls <c>OnCompleted</c> on its subscriber from inside <c>Subscribe</c>.</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    private sealed class SyncCompletingObservable<T> : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }
    }
}
