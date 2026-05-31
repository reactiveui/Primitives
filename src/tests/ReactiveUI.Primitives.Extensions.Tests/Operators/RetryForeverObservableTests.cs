// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <see cref="RetryForeverObservable{T}"/> — exercises the resubscribe-on-error
/// loop, the dispose-after-error short-circuit that prevents a runaway resubscribe, and the
/// null-observer subscribe guard.</summary>
public class RetryForeverObservableTests
{
    /// <summary>Sentinel for the first source emission.</summary>
    private const int FirstAttempt = 1;

    /// <summary>Sentinel for the second source emission.</summary>
    private const int SecondAttempt = 2;

    /// <summary>Number of attempts the resubscribe loop is configured to make.</summary>
    private const int FinalAttempt = 3;

    /// <summary>Verifies the resubscribe loop replays values from a fresh subscription on every error and finally forwards completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryForeverSourceErrorsThenCompletes_ThenResubscribesAndForwards()
    {
        var attempts = 0;
        var values = new List<int>();
        var completed = false;
        var source = Observable.Create<int>(observer =>
        {
            var attempt = Interlocked.Increment(ref attempts);
            observer.OnNext(attempt);
            if (attempt < FinalAttempt)
            {
                observer.OnError(new InvalidOperationException("retry"));
            }
            else
            {
                observer.OnCompleted();
            }

            return System.Reactive.Disposables.Disposable.Empty;
        });

        using var sub = source.OnErrorRetry().Subscribe(values.Add, () => completed = true);

        await Assert.That(values).IsCollectionEqualTo([FirstAttempt, SecondAttempt, FinalAttempt]);
        await Assert.That(completed).IsTrue();
        await Assert.That(attempts).IsEqualTo(FinalAttempt);
    }

    /// <summary>Verifies that disposing the subscription suppresses resubscription on a subsequent error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryForeverDisposedAfterError_ThenDoesNotResubscribe()
    {
        var subscribeCount = 0;
        IObserver<int>? captured = null;
        var source = Observable.Create<int>(observer =>
        {
            Interlocked.Increment(ref subscribeCount);
            captured = observer;
            return System.Reactive.Disposables.Disposable.Empty;
        });

        var sub = source.OnErrorRetry().Subscribe(static _ => { });
        await Assert.That(subscribeCount).IsEqualTo(1);

        sub.Dispose();
        captured!.OnError(new InvalidOperationException("after-dispose"));

        await Assert.That(subscribeCount).IsEqualTo(1);
    }

    /// <summary>Verifies completion after disposal is ignored.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryForeverDisposedBeforeCompletion_ThenCompletionDropped()
    {
        var source = new SyncDirectSource<int>();
        var completedCount = 0;

        var sub = source.OnErrorRetry().Subscribe(static _ => { }, () => completedCount++);

        sub.Dispose();
        source.Observer.OnCompleted();

        await Assert.That(completedCount).IsEqualTo(0);
    }

    /// <summary>Verifies subscribing with a null observer throws.</summary>
    [Test]
    public void WhenRetryForeverObserverNull_ThenSubscribeThrows()
    {
        var observable = new RetryForeverObservable<int>(new Subject<int>());

        Assert.Throws<ArgumentNullException>(() => observable.Subscribe(null!));
    }
}
