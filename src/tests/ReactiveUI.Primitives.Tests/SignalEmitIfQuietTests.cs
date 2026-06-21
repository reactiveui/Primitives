// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Signal"/> emit-if-quiet debounce contracts.</summary>
public sealed class SignalEmitIfQuietTests
{
    /// <summary>Verifies the EmitIfQuiet method covers immediate, scheduled, completion, stale emission, and error paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EmitIfQuietCoversImmediateScheduledCompletionStaleAndErrorPaths()
    {
        const int First = 1;
        const int Second = 2;
        const int Third = 3;
        List<int> immediateValues = [];
        _ = Signal.FromEnumerable([First, Second]).EmitIfQuiet(TimeSpan.Zero).Subscribe(immediateValues.Add);
        await Assert.That(immediateValues.SequenceEqual([First, Second])).IsTrue();
        VirtualClock clock = new();
        Signal<int> source = new();
        List<int> delayedValues = [];
        var completed = 0;
        _ = source.EmitIfQuiet(TimeSpan.FromTicks(Third), clock)
            .Subscribe(delayedValues.Add, ex => throw ex, () => completed++);
        source.OnNext(First);
        clock.AdvanceBy(TimeSpan.FromTicks(Second));
        source.OnNext(Second);
        clock.AdvanceBy(TimeSpan.FromTicks(First));
        clock.AdvanceBy(TimeSpan.FromTicks(Second));
        source.OnNext(Third);
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(Third));
        await Assert.That(delayedValues.SequenceEqual([Second, Third])).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        var emptyCompletion = 0;
        Signal<int> emptySource = new();
        _ = emptySource.EmitIfQuiet(TimeSpan.FromTicks(First), new VirtualClock())
            .Subscribe(_ => { }, ex => throw ex, () => emptyCompletion++);
        emptySource.OnCompleted();
        await Assert.That(emptyCompletion).IsEqualTo(1);
        VirtualClock errorClock = new();
        Signal<int> errorSource = new();
        InvalidOperationException expected = new("quiet");
        Exception? observed = null;
        _ = errorSource.EmitIfQuiet(TimeSpan.FromTicks(First), errorClock).Subscribe(_ => { }, ex => observed = ex);
        errorSource.OnNext(First);
        errorSource.OnError(expected);
        errorClock.AdvanceBy(TimeSpan.FromTicks(First));
        await Assert.That(observed!).IsSameReferenceAs(expected);
        _ = Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).EmitIfQuiet(TimeSpan.FromTicks(First)));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Emit(First).EmitIfQuiet(TimeSpan.FromTicks(First), null!));
        var stoppedGuardCompleted = 0;
        _ = new ScriptedObservable<int>(observer =>
            {
                observer.OnCompleted();
                observer.OnNext(First);
            }).EmitIfQuiet(TimeSpan.FromTicks(First), new VirtualClock())
            .Subscribe(_ => { }, ex => throw ex, () => stoppedGuardCompleted++);
        await Assert.That(stoppedGuardCompleted).IsEqualTo(1);
    }
}
