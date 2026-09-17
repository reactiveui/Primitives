// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the pull-mode delivery that reads a value again each time it changes, through its struct and class forms.</summary>
public sealed class CurrentValueDeliveryTests
{
    /// <summary>The second value a test reads.</summary>
    private const int Second = 2;

    /// <summary>The third value a test reads.</summary>
    private const int Third = 3;

    /// <summary>Starting reads and delivers the initial value, even when it is the default value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StartDeliversTheInitialValueEvenWhenItIsTheDefault()
    {
        RecordingWitness<int> downstream = new();
        using CurrentValueWitness<int> witness = new(downstream, new MutableReader(), EqualityComparer<int>.Default);

        witness.Start();

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("0");
        await Assert.That(witness.IsTerminated).IsFalse();
    }

    /// <summary>A change raised before starting is ignored, because the initial read covers it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ChangeRaisedBeforeStartIsIgnored()
    {
        RecordingWitness<int> downstream = new();
        MutableReader reader = new() { Value = 1 };
        using CurrentValueWitness<int> witness = new(downstream, reader);

        witness.Changed();
        reader.Value = Second;
        witness.Start();

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("2");
    }

    /// <summary>With a comparer, a read equal to the last delivered value is skipped; without one every read is delivered.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ComparerSkipsReadsEqualToTheLastDeliveredValue()
    {
        RecordingWitness<int> distinct = new();
        RecordingWitness<int> every = new();
        MutableReader reader = new() { Value = 1 };
        using CurrentValueWitness<int> distinctWitness = new(distinct, reader, EqualityComparer<int>.Default);
        using CurrentValueWitness<int> everyWitness = new(every, reader);

        distinctWitness.Start();
        everyWitness.Start();
        distinctWitness.Changed();
        everyWitness.Changed();
        reader.Value = Second;
        distinctWitness.Changed();
        everyWitness.Changed();

        await Assert.That(string.Join(",", distinct.Values)).IsEqualTo("1,2");
        await Assert.That(string.Join(",", every.Values)).IsEqualTo("1,1,2");
    }

    /// <summary>The boolean constructor turns distinct filtering on and off.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DistinctUntilChangedFlagControlsFiltering()
    {
        RecordingWitness<int> distinct = new();
        RecordingWitness<int> every = new();
        DeliveryOwner distinctOwner = new(distinct, distinctUntilChanged: true) { Value = 1 };
        DeliveryOwner everyOwner = new(every, distinctUntilChanged: false) { Value = 1 };

        distinctOwner.Start();
        everyOwner.Start();
        distinctOwner.Changed();
        everyOwner.Changed();

        await Assert.That(string.Join(",", distinct.Values)).IsEqualTo("1");
        await Assert.That(string.Join(",", every.Values)).IsEqualTo("1,1");
    }

    /// <summary>A terminal recorded before starting is delivered after the initial value, and the first terminal wins.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TerminalRecordedBeforeStartFollowsTheInitialValue()
    {
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new("first");
        using CurrentValueWitness<int> witness = new(downstream, new MutableReader { Value = 1 });

        witness.Fault(expected);
        witness.Complete();
        var terminatedBeforeStart = witness.IsTerminated;
        witness.Start();
        witness.Changed();

        await Assert.That(terminatedBeforeStart).IsFalse();
        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("1");
        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Completed).IsEqualTo(0);
        await Assert.That(witness.IsTerminated).IsTrue();
    }

    /// <summary>Completion after starting is delivered at once, and a drain afterwards reports nothing further.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompletionAfterStartIsDeliveredOnce()
    {
        RecordingWitness<int> downstream = new();
        DeliveryOwner owner = new(downstream, distinctUntilChanged: false) { Value = 1 };

        owner.Start();
        owner.Complete();
        var drainedAgain = owner.DrainNow();

        await Assert.That(downstream.Completed).IsEqualTo(1);
        await Assert.That(drainedAgain).IsFalse();
    }

    /// <summary>After disposal no change is delivered.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposalStopsDelivery()
    {
        RecordingWitness<int> downstream = new();
        MutableReader reader = new() { Value = 1 };
        CurrentValueWitness<int> witness = new(downstream, reader);

        witness.Start();
        witness.Dispose();
        reader.Value = Second;
        witness.Changed();
        witness.Complete();

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("1");
        await Assert.That(downstream.Completed).IsEqualTo(0);
        await Assert.That(witness.IsTerminated).IsTrue();
    }

    /// <summary>Stopping while the value is being read drops that value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StopDuringTheReadDropsTheValue()
    {
        RecordingWitness<int> downstream = new();
        MutableReader reader = new() { Value = 1 };
        CurrentValueWitness<int> witness = new(downstream, reader);
        reader.OnRead = () => witness.Dispose();

        witness.Start();

        await Assert.That(downstream.Values.Count).IsEqualTo(0);
    }

    /// <summary>Stopping from the observer of a pending change drops the terminal recorded behind it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StopFromTheObserverDropsTheRecordedTerminal()
    {
        CurrentValueWitness<int>? witness = null;
        CallbackRecordingWitness<int> downstream = new(_ => witness!.Dispose());
        witness = new(downstream, new MutableReader { Value = 1 });

        witness.Complete();
        witness.Start();

        await Assert.That(downstream.IsCompleted).IsFalse();
    }

    /// <summary>A reader that throws propagates to the caller, and the next change is still delivered.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ThrowingReaderPropagatesAndTheNextChangeStillEmits()
    {
        RecordingWitness<int> downstream = new();
        MutableReader reader = new() { Value = 1 };
        using CurrentValueWitness<int> witness = new(downstream, reader);
        witness.Start();

        reader.OnRead = static () => throw new InvalidOperationException("read");
        await Assert.That(witness.Changed).Throws<InvalidOperationException>();
        reader.OnRead = null;
        reader.Value = Second;
        witness.Changed();

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("1,2");
    }

    /// <summary>A change raised by the observer on the delivering thread is read and delivered after the observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ChangeRaisedByTheObserverIsDeliveredAfterItReturns()
    {
        List<string> log = [];
        MutableReader reader = new() { Value = 1 };
        CurrentValueWitness<int>? witness = null;
        witness = new(
            new CallbackRecordingWitness<int>(value =>
            {
                log.Add($"start{value}");
                if (value == 1)
                {
                    reader.Value = Second;
                    witness!.Changed();
                }

                log.Add($"end{value}");
            }),
            reader);

        witness.Start();

        await Assert.That(string.Join(",", log)).IsEqualTo("start1,end1,start2,end2");
    }

    /// <summary>An observer that marshals to a thread which raises a change is not deadlocked, and the change is delivered.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserverMarshallingToAChangingThreadDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        ConcurrentQueue<int> values = new();
        MutableReader reader = new() { Value = 1 };
        CurrentValueWitness<int>? witness = null;
        witness = new(
            new CallbackRecordingWitness<int>(value =>
            {
                values.Enqueue(value);
                if (value != 1)
                {
                    return;
                }

                dispatcher.Invoke(() =>
                {
                    reader.Value = Third;
                    witness!.Changed();
                });
            }),
            reader);

        var worker = BackgroundThread.Start(witness.Start);

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        await Assert.That(string.Join(",", values)).IsEqualTo("1,3");
    }

    /// <summary>The class form rejects null arguments.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NullArgumentsAreRejected()
    {
        using CurrentValueWitness<int> witness = new(new RecordingWitness<int>(), new MutableReader());

        await Assert.That(static () => new CurrentValueWitness<int>(null!, new MutableReader())).Throws<ArgumentNullException>();
        await Assert.That(static () => new CurrentValueWitness<int>(new RecordingWitness<int>(), null!)).Throws<ArgumentNullException>();
        await Assert.That(() => witness.Fault(null!)).Throws<ArgumentNullException>();
    }

    /// <summary>A reader over a mutable value that can run an action on each read.</summary>
    private sealed class MutableReader : ICurrentValueReader<int>
    {
        /// <summary>Gets or sets the value returned by <see cref="Read"/>.</summary>
        public int Value { get; set; }

        /// <summary>Gets or sets the action run at the start of each read.</summary>
        public Action? OnRead { get; set; }

        /// <inheritdoc/>
        public int Read()
        {
            OnRead?.Invoke();
            return Value;
        }
    }

    /// <summary>An owner embedding the struct form, as a subscription would.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="distinctUntilChanged">Whether equal reads are skipped.</param>
    private sealed class DeliveryOwner(IObserver<int> observer, bool distinctUntilChanged) : ICurrentValueReader<int>
    {
        /// <summary>The embedded delivery.</summary>
        private CurrentValueDelivery<int> _delivery = new(distinctUntilChanged);

        /// <summary>Gets or sets the value returned by <see cref="Read"/>.</summary>
        public int Value { get; set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read() => Value;

        /// <summary>Starts the delivery.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Start() => _delivery.Start(new OwnerDrain(this));

        /// <summary>Records a change.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Changed() => _delivery.Changed(new OwnerDrain(this));

        /// <summary>Records completion.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Complete() => _delivery.Complete(new OwnerDrain(this));

        /// <summary>Drains the recorded work on the calling thread.</summary>
        /// <returns>Whether the terminal notification was delivered.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool DrainNow() => _delivery.DrainTo(observer, this);

        /// <summary>Drains the owner's recorded work.</summary>
        /// <param name="Owner">The owner.</param>
        private readonly record struct OwnerDrain(DeliveryOwner Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner.DrainNow();
        }
    }
}
