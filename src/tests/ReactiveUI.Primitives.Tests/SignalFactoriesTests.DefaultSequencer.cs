// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the factory overloads that name no sequencer and therefore run on the default one, together with
/// the degenerate inputs those factories have to fold away: an empty range and an uncancellable token.
/// </summary>
public partial class SignalFactoriesTests
{
    /// <summary>The time allowed for a default-sequencer factory to produce its notification.</summary>
    private static readonly TimeSpan DefaultSequencerTimeout = TimeSpan.FromSeconds(5);

    /// <summary>The timeout used by the expiry factory test.</summary>
    private static readonly TimeSpan ShortExpiry = TimeSpan.FromMilliseconds(20);

    /// <summary>Verifies an empty range completes immediately without emitting.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnEmptySequenceCompletesWithoutEmitting()
    {
        List<int> values = [];
        var completions = 0;
        using var subscription = Signal.Sequence(One, 0)
            .Subscribe(values.Add, static _ => { }, () => completions++);
        await Assert.That(values.Count).IsEqualTo(0);
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>Verifies the cancellable enumerable factory stops when its token is already cancelled.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEnumerableHonorsACancellableTokenAndIgnoresAnUncancellableOne()
    {
        using CancellationTokenSource cancelled = new();
        await cancelled.CancelAsync();
        List<int> cancelledValues = [];
        List<Exception> cancelledErrors = [];
        var cancelledCompletions = 0;
        using var cancelledSubscription = Signal.FromEnumerable([One, Two, Three], cancelled.Token)
            .Subscribe(cancelledValues.Add, cancelledErrors.Add, () => cancelledCompletions++);
        await Assert.That(cancelledValues.Count).IsEqualTo(0);
        await Assert.That(cancelledErrors.Count).IsEqualTo(0);
        await Assert.That(cancelledCompletions).IsEqualTo(0);
        List<int> plainValues = [];
        using var plainSubscription = Signal.FromEnumerable([One, Two, Three], CancellationToken.None)
            .Subscribe(plainValues.Add);
        await Assert.That(plainValues.SequenceEqual([One, Two, Three])).IsTrue();
    }

    /// <summary>Verifies the sequencer-free expiry factory fails a sequence that never terminates.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ExpireWithoutASequencerFailsASilentSequence()
    {
        List<Exception> errors = [];
        using var subscription = Signal.Expire(Signal.Silent<int>(), ShortExpiry)
            .Subscribe(static _ => { }, errors.Add);
        await TestPolling.SpinUntil(() => errors.Count == 1, DefaultSequencerTimeout);
        await Assert.That(errors[0]).IsTypeOf<TimeoutException>();
    }

    /// <summary>Verifies the sequencer-free <c>Start</c> factories run their work and emit its outcome.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartWithoutASequencerRunsTheWorkOnTheDefaultSequencer()
    {
        List<int> functionValues = [];
        using var functionSubscription = Signal.Start(static () => Two).Subscribe(functionValues.Add);
        await TestPolling.SpinUntil(() => functionValues.Count == 1, DefaultSequencerTimeout);
        await Assert.That(functionValues.SequenceEqual([Two])).IsTrue();
        var actionRuns = 0;
        List<RxVoid> actionValues = [];

        // A void method group is what selects Start(Action); a lambda over 'actionRuns++' is a
        // Func<int> and would bind to the generic Start<T> overload instead.
        void RunAction() => actionRuns++;

        using var actionSubscription = Signal.Start(RunAction).Subscribe(actionValues.Add);
        await TestPolling.SpinUntil(() => actionValues.Count == 1, DefaultSequencerTimeout);
        await Assert.That(actionRuns).IsEqualTo(1);
        await Assert.That(actionValues[0]).IsEqualTo(RxVoid.Default);
    }

    /// <summary>Verifies the sequencer-free <c>Every</c> factory ticks on the default sequencer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EveryWithoutASequencerTicksOnTheDefaultSequencer()
    {
        List<long> ticks = [];
        using (Signal.Every(ShortExpiry).Subscribe(ticks.Add))
        {
            await TestPolling.SpinUntil(() => ticks.Count >= Two, DefaultSequencerTimeout);
        }

        await Assert.That(ticks[1]).IsGreaterThan(ticks[0]);
    }
}
