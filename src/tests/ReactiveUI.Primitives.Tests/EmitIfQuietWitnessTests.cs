// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests argument validation and emission guards of <see cref="EmitIfQuietWitness{T}"/>.</summary>
public sealed class EmitIfQuietWitnessTests
{
    /// <summary>The value pushed through the witness.</summary>
    private const int Value = 5;

    /// <summary>A null observer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullObserver_ThrowsArgumentNull() =>
        await Assert.That(static () => new EmitIfQuietWitness<int>(null!, TimeSpan.Zero, Sequencer.Immediate))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A null sequencer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSequencer_ThrowsArgumentNull() =>
        await Assert.That(static () => new EmitIfQuietWitness<int>(new IgnoringObserver(), TimeSpan.Zero, null!))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A scheduled emission that runs again for the same version does not emit the value twice.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ReplayedEmissionForTheSameVersionEmitsOnce()
    {
        ManualSequencer sequencer = new();
        RecordingWitness<int> observer = new();
        using EmitIfQuietWitness<int> witness = new(observer, TimeSpan.Zero, sequencer);

        witness.OnNext(Value);
        sequencer.RunPending();
        sequencer.RunStaleTick();

        await Assert.That(observer.Values.SequenceEqual([Value])).IsTrue();
    }

    /// <summary>An emission that runs after completion delivered the pending value emits nothing more.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EmissionRunningAfterCompletionEmitsNothingMore()
    {
        InlineSequencer sequencer = new();
        RecordingWitness<int> observer = new();
        using EmitIfQuietWitness<int> witness = new(observer, TimeSpan.Zero, sequencer);
        sequencer.BeforeRun = witness.OnCompleted;

        witness.OnNext(Value);

        await Assert.That(observer.Values.SequenceEqual([Value])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>Observer that ignores every notification.</summary>
    private sealed class IgnoringObserver : IObserver<int>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(int value)
        {
        }
    }

    /// <summary>Runs each work item inline, after a callback the test installs for the next schedule.</summary>
    private sealed class InlineSequencer : ISequencer
    {
        /// <inheritdoc/>
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <inheritdoc/>
        public long Timestamp => 0;

        /// <summary>Gets or sets the callback run before the next scheduled item.</summary>
        internal Action? BeforeRun { get; set; }

        /// <inheritdoc/>
        public void Schedule(IWorkItem item)
        {
            var beforeRun = BeforeRun;
            BeforeRun = null;
            beforeRun?.Invoke();
            item.Execute();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);
    }
}
