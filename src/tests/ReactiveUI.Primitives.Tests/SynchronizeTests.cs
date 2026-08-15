// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the <see cref = "SynchronizeWitness{T}"/> gate and the <c>Synchronize</c> operator.</summary>
public class SynchronizeTests
{
    /// <summary>The number of producer threads used by stress tests.</summary>
    private const int Threads = 8;

    /// <summary>The number of values sent by each producer thread.</summary>
    private const int PerThread = 500;

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
        ConcurrencyProbe probe = new();
        Lock gate = new();
        SynchronizeWitness<int> first = new(probe, gate);
        SynchronizeWitness<int> second = new(probe, gate);
        var tasks = new Task[Threads];
        for (var t = 0; t < Threads; t++)
        {
            var sink = t % Second == 0 ? first : second;
            tasks[t] = Task.Run(() =>
            {
                for (var i = 0; i < PerThread; i++)
                {
                    sink.OnNext(i);
                }
            });
        }

        await Task.WhenAll(tasks);
        await Assert.That(probe.OverlapDetected).IsFalse();
        await Assert.That(probe.Count).IsEqualTo(Threads * PerThread);
    }

    /// <summary>Concurrent <c>OnNext</c> calls are serialized: the downstream is never entered re-entrantly and sees every value.</summary>
    /// <returns>A task that completes when the assertions have run.</returns>
    [Test]
    public async Task SerializesConcurrentOnNextSoTheDownstreamNeverOverlaps()
    {
        ConcurrencyProbe probe = new();
        SynchronizeWitness<int> sink = new(probe);
        var tasks = new Task[Threads];
        for (var t = 0; t < Threads; t++)
        {
            tasks[t] = Task.Run(() =>
            {
                for (var i = 0; i < PerThread; i++)
                {
                    sink.OnNext(i);
                }
            });
        }

        await Task.WhenAll(tasks);
        await Assert.That(probe.OverlapDetected).IsFalse();
        await Assert.That(probe.Count).IsEqualTo(Threads * PerThread);
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
        ConcurrencyProbe probe = new();
        var gate = new object();
        SynchronizeObjectWitness<int> first = new(probe, gate);
        SynchronizeObjectWitness<int> second = new(probe, gate);
        var tasks = new Task[Threads];
        for (var t = 0; t < Threads; t++)
        {
            var sink = t % Second == 0 ? first : second;
            tasks[t] = Task.Run(() =>
            {
                for (var i = 0; i < PerThread; i++)
                {
                    sink.OnNext(i);
                }
            });
        }

        await Task.WhenAll(tasks);
        await Assert.That(probe.OverlapDetected).IsFalse();
        await Assert.That(probe.Count).IsEqualTo(Threads * PerThread);
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

    /// <summary>A downstream observer that flags any re-entrant (overlapping) notification and counts deliveries.</summary>
    private sealed class ConcurrencyProbe : IObserver<int>
    {
        /// <summary>The number of wait spin iterations used to widen the re-entrancy detection window.</summary>
        private const int SpinIterations = 50;

        /// <summary>Non-zero while a notification is in flight, used to detect re-entrancy.</summary>
        private int _inside;

        /// <summary>Gets the number of values delivered.</summary>
        public int Count { get; private set; }

        /// <summary>Gets a value indicating whether two notifications were ever observed to overlap.</summary>
        public bool OverlapDetected { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        /// <param name = "error">The forwarded error (ignored).</param>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        /// <param name = "value">The forwarded value.</param>
        public void OnNext(int value)
        {
            if (Interlocked.Exchange(ref _inside, 1) != 0)
            {
                OverlapDetected = true;
            }

            // Non-atomic on purpose: the gate must serialize callers for this to stay exact.
            Count++;
            Thread.SpinWait(SpinIterations);
            _ = Interlocked.Exchange(ref _inside, 0);
        }
    }
}
