// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the System.Reactive alias overloads that take only a subset of the observer callbacks, an explicit
/// comparer, or an explicit gate — the shapes the data-driven parity rows do not reach.
/// </summary>
public partial class RxNamesTests
{
    /// <summary>Verifies the two-callback <c>Do</c> overloads run only the callbacks they were given.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DoWithTwoCallbacksInvokesOnlyTheSuppliedCallbacks()
    {
        List<int> erroringValues = [];
        List<Exception> observedErrors = [];
        InvalidOperationException expected = new("do-fault");
        Signal<int> erroring = new();
        using (erroring.Do(erroringValues.Add, observedErrors.Add).Subscribe(static _ => { }, static _ => { }))
        {
            erroring.OnNext(One);
            erroring.OnError(expected);
        }

        await Assert.That(erroringValues.SequenceEqual([One])).IsTrue();
        await Assert.That(observedErrors.Count).IsEqualTo(1);
        await Assert.That(observedErrors[0]).IsSameReferenceAs(expected);
        List<int> completingValues = [];
        var sideEffectCompletions = 0;
        Signal<int> completing = new();
        using (completing.Do(completingValues.Add, () => sideEffectCompletions++).Subscribe(static _ => { }))
        {
            completing.OnNext(Three);
            completing.OnCompleted();
        }

        await Assert.That(completingValues.SequenceEqual([Three])).IsTrue();
        await Assert.That(sideEffectCompletions).IsEqualTo(1);
    }

    /// <summary>Verifies the comparer overloads of the adjacent-duplicate operators honor the supplied comparer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DistinctUntilChangedWithAComparerSuppressesAdjacentValuesTheComparerCallsEqual()
    {
        List<string> values = [];
        _ = Signal.FromEnumerable(["a", "A", "b"])
            .DistinctUntilChanged(StringComparer.OrdinalIgnoreCase)
            .Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(["a", "b"])).IsTrue();
        List<string> defaultedValues = [];
        _ = Signal.FromEnumerable(["a", "A", "b"])
            .DistinctUntilChanged((IEqualityComparer<string>?)null)
            .Subscribe(defaultedValues.Add);
        await Assert.That(defaultedValues.SequenceEqual(["a", "A", "b"])).IsTrue();
        List<string> keyed = [];
        _ = Signal.FromEnumerable(["ax", "Ay", "bz"])
            .DistinctUntilChangedBy(static value => value[..1], StringComparer.OrdinalIgnoreCase)
            .Subscribe(keyed.Add);
        await Assert.That(keyed.SequenceEqual(["ax", "bz"])).IsTrue();
        List<string> defaultedKeys = [];
        _ = Signal.FromEnumerable(["ax", "Ay", "bz"])
            .DistinctUntilChangedBy(static value => value[..1], (IEqualityComparer<string>?)null)
            .Subscribe(defaultedKeys.Add);
        await Assert.That(defaultedKeys.SequenceEqual(["ax", "Ay", "bz"])).IsTrue();
    }

    /// <summary>Verifies <c>StartWith</c> returns the source untouched when no values are prepended.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartWithNoValuesReturnsTheSourceItself()
    {
        var source = Signal.FromEnumerable([One, Three]);
        var started = source.StartWith();
        await Assert.That(started).IsSameReferenceAs(source);
        List<int> values = [];
        _ = started.Subscribe(values.Add);
        await Assert.That(values.SequenceEqual([One, Three])).IsTrue();
    }

#if NET9_0_OR_GREATER
    /// <summary>Verifies the object-gate <c>Synchronize</c> overload forwards the source under the shared gate.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SynchronizeWithAnObjectGateForwardsEverySourceNotification()
    {
        var gate = new object();
        RecordingWitness<int> witness = new();
        Signal<int> source = new();
        using (source.Synchronize(gate).Subscribe(witness))
        {
            source.OnNext(One);
            source.OnNext(Three);
            source.OnCompleted();
        }

        await Assert.That(witness.Values.SequenceEqual([One, Three])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Emit(One).Synchronize((object)null!));
        _ = Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Synchronize(gate));
    }
#endif
}
