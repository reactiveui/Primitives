// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="ReattemptCoordinator{T}"/>.</summary>
public sealed class ReattemptCoordinatorTests
{
    /// <summary>Iteration count for deep synchronous retry stack-safety verification.</summary>
    private const int StackSafetyDepth = 10_000;

    /// <summary>Recovered value emitted after retry exhaustion.</summary>
    private const int RecoveredValue = 42;

    /// <summary>Retry count used for range reduction checks.</summary>
    private const int RetryCount = 2;

    /// <summary>Range length used for reduction checks.</summary>
    private const int RangeLength = 4;

    /// <summary>Completion arriving from a misbehaving source after disposal is suppressed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompleted_AfterDispose_DoesNotForwardCompletion()
    {
        IObserver<int>? captured = null;
        var source = Signal.Create<int>(obs =>
        {
            captured = obs;
            return EmptyDisposable.Instance;
        });

        RecordingWitness<int> downstream = new();
        ReattemptCoordinator<int> coordinator = new(source, RetryCount, downstream);
        var sub = coordinator.Run();
        sub.Dispose();

        captured!.OnCompleted();

        await Assert.That(downstream.Completed).IsEqualTo(0);
    }

    /// <summary>Duplicate completion from a misbehaving source reaches downstream once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompleted_RepeatedBySource_ForwardsSingleCompletion()
    {
        IObserver<int>? captured = null;
        var source = Signal.Create<int>(obs =>
        {
            captured = obs;
            return EmptyDisposable.Instance;
        });

        RecordingWitness<int> downstream = new();
        using var sub = new ReattemptCoordinator<int>(source, RetryCount, downstream).Run();
        captured!.OnCompleted();
        captured.OnCompleted();

        await Assert.That(downstream.Completed).IsEqualTo(1);
    }

    /// <summary>Ten thousand synchronous retries execute iteratively without overflowing the call stack.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_TenThousandSynchronousRetries_RecoversWithoutStackOverflow()
    {
        var attempts = 0;
        var result = 0;
        var flaky = Signal.Create<int>(obs =>
        {
            attempts++;
            if (attempts < StackSafetyDepth)
            {
                obs.OnError(new InvalidOperationException("retry"));
            }
            else
            {
                obs.OnNext(RecoveredValue);
                obs.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });

        _ = flaky.Reattempt(StackSafetyDepth).Subscribe(x => result = x);

        await Assert.That(result).IsEqualTo(RecoveredValue);
        await Assert.That(attempts).IsEqualTo(StackSafetyDepth);
    }

    /// <summary>Reattempt on an infallible range signal returns the original range unchanged.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Reattempt_OnRangeSignal_ReturnsSourceUnchanged()
    {
        var range = Signal.Sequence(1, RangeLength);
        await Assert.That(range.Reattempt(RetryCount)).IsSameReferenceAs(range);
        await Assert.That(ReattemptSignal<int>.CreateTotalRuns(range, 0)).IsSameReferenceAs(Signal.Empty<int>());
    }

    /// <summary>Asynchronous errors after subscribe trigger iterative re-subscription, and post-disposal callbacks are ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_AsynchronousRetryAndDisposedGuards_BehaveAsExpected()
    {
        List<Signal<int>> attempts = [];
        var flaky = Signal.Create<int>(obs =>
        {
            Signal<int> inner = new();
            attempts.Add(inner);
            return inner.Subscribe(obs);
        });

        RecordingWitness<int> downstream = new();
        using (new ReattemptCoordinator<int>(flaky, RetryCount, downstream).Run())
        {
            attempts[0].OnError(new InvalidOperationException("async-1"));
            attempts[1].OnNext(RecoveredValue);
            attempts[1].OnCompleted();
        }

        await Assert.That(downstream.Values.SequenceEqual([RecoveredValue])).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(1);

        RecordingWitness<int> disposedDownstream = new();
        ReattemptCoordinator<int> disposedCoordinator = new(Signal.Silent<int>(), RetryCount, disposedDownstream);
        disposedCoordinator.Dispose();
        _ = disposedCoordinator.Run();
        await Assert.That(disposedCoordinator.BeginAttempt()).IsFalse();
        disposedCoordinator.OnNext(1);
        disposedCoordinator.OnCompleted();
        disposedCoordinator.OnError(new InvalidOperationException("ignored"));
        await Assert.That(disposedDownstream.Values).IsEmpty();
        await Assert.That(disposedDownstream.Completed).IsEqualTo(0);
        await Assert.That(disposedDownstream.Errors).IsEmpty();
    }
}
