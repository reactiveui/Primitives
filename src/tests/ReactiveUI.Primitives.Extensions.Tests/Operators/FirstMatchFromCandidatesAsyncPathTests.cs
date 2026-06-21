// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Coverage for the asynchronous-projection path of
/// <c>FirstMatchFromCandidates</c> backed by <c>FirstMatchFromCandidatesObservable</c>
/// — empty candidate list, async-projection match, async-projection no-match falls back,
/// async-projection error skips, and dispose during the async walk.</summary>
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

    /// <summary>Verifies that an async projection whose value matches the predicate emits the
    /// matching value and completes — exercises the <c>AsyncSink.OnNext</c> match path.</summary>
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
        var done = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(done).IsTrue();
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
    }

    /// <summary>Verifies that an async projection that never matches falls through to the
    /// fallback when its source completes — exercises the async <c>OnCompleted</c> path.</summary>
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
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(results).IsCollectionEqualTo([Fallback]);
    }

    /// <summary>Verifies that an async projection error is swallowed and the walk continues
    /// to the next candidate — exercises the async <c>OnError</c> path.</summary>
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
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
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

        // Second dispose hits the Interlocked.Exchange null-loser branch in AsyncSink.Dispose
        // — the first call swapped in null and disposed the previous subscription, so the
        // second call sees null and the `?.Dispose()` no-op fires.
        sub.Dispose();
        firstSubject.OnNext("late");
        firstSubject.OnCompleted();
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies that when the synchronous transform throws for one candidate the next
    /// candidate is tried — exercises the <c>catch { continue; }</c> path in the sync fast path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncTransformThrows_ThenContinuesToNextCandidate()
    {
        string[] keys = ["throw", HitKey];
        List<string> results = [];
        var completed = false;
        using var sub = ((IReadOnlyList<string>)keys).FirstMatchFromCandidates(
            static key => Observable.Return(key),
            static raw => raw == "throw" ? throw new InvalidOperationException("transform-throws") : raw,
            static value => value == HitKey,
            Fallback).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that a candidate whose projected observable synchronously calls
    /// <c>OnError</c> on the sink during its <c>Subscribe</c> call hits the
    /// <c>if (_looping) return;</c> re-entrancy guard in <c>AsyncSink.OnError</c> and
    /// proceeds to the next candidate.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncCandidateProjectionSyncErrors_ThenLoopingGuardSkipsToNextCandidate()
    {
        string[] keys = [SyncErrorKey, HitKey];
        List<string> results = [];
        var completed = false;
        using var sub = ((IReadOnlyList<string>)keys)
            .FirstMatchFromCandidates(
            static key => key == SyncErrorKey
                ? new SyncErroringObservable<string>(new InvalidOperationException(SyncErrorKey))
                : Observable.Return(key),
            static raw => raw,
            static value => value == HitKey,
            Fallback).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that a candidate whose projected observable synchronously calls
    /// <c>OnCompleted</c> on the sink during its <c>Subscribe</c> call hits the
    /// <c>if (_looping) return;</c> re-entrancy guard in <c>AsyncSink.OnCompleted</c> and
    /// proceeds to the next candidate.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncCandidateProjectionSyncCompletes_ThenLoopingGuardSkipsToNextCandidate()
    {
        string[] keys = [SyncCompleteKey, HitKey];
        List<string> results = [];
        var completed = false;
        using var sub = ((IReadOnlyList<string>)keys)
            .FirstMatchFromCandidates(
            key => key == SyncCompleteKey ? new SyncCompletingObservable<string>() : Observable.Return(key),
            static raw => raw,
            static value => value == HitKey,
            Fallback).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that a second async candidate emission arriving after a match has already
    /// fired is silently dropped via the <c>_done</c> guard in <c>AsyncSink.OnNext</c>.</summary>
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
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        subject.OnNext("ignored-late");
        subject.OnError(new InvalidOperationException("ignored-late"));
        subject.OnCompleted();
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
    }

    /// <summary>Drives <c>AsyncSink.TryNext</c> through a candidate whose projection
    /// synchronously errors — that path enters <c>AsyncSink.OnError</c> while <c>_looping == true</c>
    /// (inside <c>TryNext</c>), exercising the <c>if (_looping) return;</c> guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncSinkWalkHitsSyncErroringCandidate_ThenLoopingGuardSkipsAhead()
    {
        // First candidate's projection is async (never completes during Subscribe), forcing
        // TrySyncLoop to hand off to AsyncSink. Second candidate's projection synchronously
        // errors during AsyncSink.TryNext's loop iteration, hitting AsyncSink.OnError with
        // _looping == true.
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

        // Complete the async subject — AsyncSink.OnCompleted runs (outside TryNext, so _looping
        // is false), which invokes TryNext. The next iteration projects SyncErrorKey whose
        // SyncErroringObservable.Subscribe calls observer.OnError synchronously, re-entering
        // AsyncSink.OnError while _looping is still true — hitting the looping-guard return.
        asyncSubject.OnCompleted();
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
    }

    /// <summary>Same shape as the looping-error case but the intermediate candidate
    /// synchronously completes instead of erroring, exercising
    /// <c>AsyncSink.OnCompleted</c>'s <c>_looping</c> guard.</summary>
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
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(results).IsCollectionEqualTo([HitKey]);
    }

    /// <summary>Observable that synchronously calls <c>OnError</c> on the subscriber from inside
    /// its <c>Subscribe</c> method — used to exercise the re-entrancy <c>_looping</c> guard.</summary>
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

    /// <summary>Observable that synchronously calls <c>OnCompleted</c> on the subscriber from
    /// inside its <c>Subscribe</c> method — used to exercise the re-entrancy <c>_looping</c>
    /// guard.</summary>
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
