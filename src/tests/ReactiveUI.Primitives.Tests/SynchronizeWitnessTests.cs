// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the <see cref = "SynchronizeWitness{T}"/> gate and the <c>Synchronize</c> operator.</summary>
public class SynchronizeWitnessTests
{
    /// <summary>The literal two.</summary>
    private const int Second = 2;

    /// <summary>The literal three.</summary>
    private const int Third = 3;

    /// <summary>The gate forwards every notification to the downstream observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForwardsEachNotificationToTheDownstreamObserver()
    {
        Recorder<int> recorder = new();
        SynchronizeWitness<int> sink = new(recorder);
        InvalidOperationException error = new("boom");
        sink.OnNext(1);
        sink.OnNext(Second);
        sink.OnError(error);
        sink.OnCompleted();
        await Assert.That(recorder.Values.SequenceEqual([1, Second])).IsTrue();
        await Assert.That(recorder.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(recorder.Completed).IsEqualTo(1);
    }

    /// <summary>The <c>Synchronize</c> operator forwards the source sequence unchanged.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SynchronizeOperatorForwardsTheSourceSequence()
    {
        List<int> received = [];
        var completed = false;
        _ = new ImmediateSource<int>(1, Second, Third)
            .Synchronize()
            .Subscribe(new DelegateWitness<int>(
                received.Add,
                static _ => { },
                () => completed = true));
        await Assert.That(received.SequenceEqual([1, Second, Third])).IsTrue();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>The operator validates its source argument.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void SynchronizeOnNullSourceThrows() =>
        Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).Synchronize());

    /// <summary>The shared-gate operator validates its gate argument.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void SynchronizeOnNullGateThrows() =>
        Assert.Throws<ArgumentNullException>(static () => new ImmediateSource<int>(1).Synchronize(null!));

    /// <summary>Two witnesses sharing one gate are serialized relative to each other, never overlapping on the shared downstream.</summary>
    /// <returns>A task that completes when the assertions have run.</returns>
    [Test]
    public async Task SharedGateSerializesAcrossTwoWitnesses()
{
        Lock gate = new();
        List<int> values = [];
        var held = true;
        var observer = new DelegateWitness<int>(
            value =>
        {
            held &= IsHeld(gate);
            values.Add(value);
        },
            static _ => { },
            static () => { });
        using SynchronizeWitness<int> first = new(observer, gate);
        using SynchronizeWitness<int> second = new(observer, gate);
        first.OnNext(1);
        second.OnNext(Second);
        await Assert.That(held).IsTrue();
        await Assert.That(values.SequenceEqual([1, Second])).IsTrue();
    }

    /// <summary>Every downstream notification runs while the witness owns its gate.</summary>
    /// <returns>A task that completes when the assertions have run.</returns>
    [Test]
    public async Task HoldsTheGateAcrossEveryDownstreamNotification()
{
        var held = true;
        List<int> values = [];
        SynchronizeWitness<int>? sink = null;
        var observer = new DelegateWitness<int>(
            value =>
        {
            held &= IsHeld(sink!.Gate);
            values.Add(value);
        },
            _ => held &= IsHeld(sink!.Gate),
            () => held &= IsHeld(sink!.Gate));
        sink = new(observer);
        sink.OnNext(1);
        sink.OnNext(Second);
        sink.OnError(new InvalidOperationException("failure"));
        sink.OnCompleted();
        await Assert.That(held).IsTrue();
        await Assert.That(values.SequenceEqual([1, Second])).IsTrue();
        sink.Dispose();
    }

    /// <summary>The object-gated sequence forwards every source value and its completion downstream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ObjectGatedSynchronizeForwardsTheSourceSequence()
    {
        Recorder<int> recorder = new();
        SynchronizeObjectSignal<int> signal = new(new ImmediateSource<int>(1, Second, Third), new object());

        using var subscription = signal.Subscribe(recorder);

        await Assert.That(recorder.Values.SequenceEqual([1, Second, Third])).IsTrue();
        await Assert.That(recorder.Completed).IsEqualTo(1);
        await Assert.That(recorder.Errors.Count).IsEqualTo(0);
    }

    /// <summary>The object-gated sequence forwards a source error and validates its observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ObjectGatedSynchronizeForwardsErrorsAndRejectsANullObserver()
    {
        InvalidOperationException expected = new("object-gate");
        Recorder<int> recorder = new();
        SynchronizeObjectSignal<int> signal = new(
            new ScriptedObservable<int>(observer =>
            {
                observer.OnNext(1);
                observer.OnError(expected);
            }),
            new object());

        using var subscription = signal.Subscribe(recorder);

        await Assert.That(recorder.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(recorder.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(recorder.Completed).IsEqualTo(0);
        _ = Assert.Throws<ArgumentNullException>(() => signal.Subscribe(null!));
    }

    /// <summary>Disposing an object-gated subscription releases the upstream subscription exactly once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ObjectGatedSynchronizeDisposesTheUpstreamSubscriptionOnce()
    {
        RecordingDisposable upstream = new();
        SynchronizeObjectWitness<int> witness = new(new Recorder<int>(), new object());
        witness.SetSubscription(upstream);

        witness.Dispose();
        witness.Dispose();

        await Assert.That(upstream.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Two object-gated witnesses sharing one gate never overlap on the shared downstream.</summary>
    /// <returns>A task that completes when the assertions have run.</returns>
    [Test]
    public async Task ObjectGatedWitnessesSharingOneGateAreSerialized()
{
        var gate = new object();
        List<int> values = [];
        var held = true;
        var observer = new DelegateWitness<int>(
            value =>
        {
            held &= Monitor.IsEntered(gate);
            values.Add(value);
        },
            static _ => { },
            static () => { });
        using SynchronizeObjectWitness<int> first = new(observer, gate);
        using SynchronizeObjectWitness<int> second = new(observer, gate);
        first.OnNext(1);
        second.OnNext(Second);
        await Assert.That(held).IsTrue();
        await Assert.That(values.SequenceEqual([1, Second])).IsTrue();
    }

#if NET9_0_OR_GREATER
    /// <summary>The object-gate <c>Synchronize</c> overload forwards the source sequence unchanged.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SynchronizeWithAnObjectGateForwardsTheSourceSequence()
    {
        Recorder<int> recorder = new();

        using var subscription = new ImmediateSource<int>(1, Second, Third)
            .Synchronize(new object())
            .Subscribe(recorder);

        await Assert.That(recorder.Values.SequenceEqual([1, Second, Third])).IsTrue();
        await Assert.That(recorder.Completed).IsEqualTo(1);
    }
#endif

    /// <summary>Reports ownership of the platform synchronization gate.</summary>
    /// <param name="gate">The gate under test.</param>
    /// <returns>Whether the calling thread owns the gate.</returns>
    private static bool IsHeld(Lock gate)
    {
#if NET9_0_OR_GREATER
        return gate.IsHeldByCurrentThread;
#else
        return Monitor.IsEntered(gate);
#endif
    }

    /// <summary>An observer that records all values, errors, and completion counts.</summary>
    /// <typeparam name = "T">The type of the observed values.</typeparam>
    private sealed class Recorder<T> : IObserver<T>
    {
        /// <summary>Gets the recorded values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the recorded errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of completion callbacks observed.</summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        /// <param name = "error">The error to record.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        /// <param name = "value">The value to record.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => Values.Add(value);
    }

    /// <summary>A synchronous observable that emits a fixed set of values then completes.</summary>
    /// <typeparam name = "T">The value type.</typeparam>
    /// <param name = "values">The values to emit on subscription.</param>
    private sealed class ImmediateSource<T>(params T[] values) : IObservable<T>, IDisposable
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            foreach (var value in values)
            {
                observer.OnNext(value);
            }

            observer.OnCompleted();
            return this;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }
}
