// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Signal"/> resource-scoped use contracts.</summary>
public class UseSignalTests
{
    /// <summary>The first value pushed through a resource-scoped signal.</summary>
    private const int FirstValue = 1;

    /// <summary>A value pushed after the signal has terminated; it must never be delivered.</summary>
    private const int LateValue = 2;

    /// <summary>Covers resource disposal when the subscription forwards a null error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UseValidatesSubscriptionError()
    {
        RecordingDisposable resource = new();
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Use(
                () => resource,
                static _ => new ScriptedObservable<int>(static observer => observer.OnError(null!)))
            .Subscribe(new Recorder<int>()));
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Covers resource disposal when the inner subscription is null.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UseDisposesResourceWhenSubscriptionIsNull()
    {
        RecordingDisposable resource = new();
        _ = Assert.Throws<ArgumentNullException>(() => Signal
            .Use(() => resource, static _ => new NullSubscriptionObservable<int>())
            .Subscribe(new Recorder<int>()));
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>
    /// A fault from the scoped source reaches the observer and releases the resource. The whole point of the
    /// operator is that the resource does not outlive the sequence, whichever way the sequence ends.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UseForwardsSourceFaultsAndReleasesTheResource()
    {
        RecordingDisposable resource = new();
        Recorder<int> observer = new();
        InvalidOperationException error = new("use-fault");

        _ = Signal.Use(() => resource, _ => new ScriptedObservable<int>(inner => inner.OnError(error)))
            .Subscribe(observer);

        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>
    /// With a hot source, the subscription is handed to the sink while the sequence is still live: the resource
    /// stays alive until completion, and every notification the source keeps sending afterwards is dropped
    /// rather than re-delivered or re-releasing the resource.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UseHoldsTheResourceUntilTheHotSourceCompletesThenIgnoresLateSignals()
    {
        RecordingDisposable resource = new();
        CapturingObservable<int> source = new();
        Recorder<int> observer = new();

        _ = Signal.Use(() => resource, _ => source).Subscribe(observer);
        var sink = source.Observer!;

        sink.OnNext(FirstValue);
        await Assert.That(resource.DisposeCount).IsEqualTo(0);

        sink.OnCompleted();

        // Everything below arrives after the terminal and must be swallowed.
        sink.OnCompleted();
        sink.OnError(new InvalidOperationException("late"));
        sink.OnNext(LateValue);

        await Assert.That(observer.Values.SequenceEqual([FirstValue])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
        await Assert.That(source.DisposeCount).IsEqualTo(1);
    }

    /// <summary>
    /// When the observer throws from its value callback, the sink tears itself down — unsubscribing upstream
    /// and releasing the resource — and rethrows. A second value must then be dropped, not fed back into the
    /// observer that already failed.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UseReleasesTheResourceWhenTheObserverThrows()
    {
        RecordingDisposable resource = new();
        CapturingObservable<int> source = new();

        _ = Signal.Use(() => resource, _ => source).Subscribe(new ThrowingWitness<int>(throwOnNext: true));
        var sink = source.Observer!;

        var thrown = Assert.Throws<InvalidOperationException>(() => sink.OnNext(FirstValue));

        // The sink has stopped, so this must be a silent no-op rather than a second trip into the observer.
        sink.OnNext(LateValue);

        await Assert.That(thrown!.Message).IsEqualTo("observer-next");
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
        await Assert.That(source.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposing the subscription releases the resource, and disposing it again does not release it twice.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UseReleasesTheResourceExactlyOnceWhenTheSubscriptionIsDisposedTwice()
    {
        RecordingDisposable resource = new();
        CapturingObservable<int> source = new();
        Recorder<int> observer = new();

        var subscription = Signal.Use(() => resource, _ => source).Subscribe(observer);
        subscription.Dispose();
        subscription.Dispose();

        // The source is unaware of the disposal and keeps pushing; the sink must drop it.
        source.Observer!.OnNext(LateValue);

        await Assert.That(resource.DisposeCount).IsEqualTo(1);
        await Assert.That(source.DisposeCount).IsEqualTo(1);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
    }

    /// <summary>Observable that hands back the observer it was subscribed with and counts its own disposals.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class CapturingObservable<T> : IObservable<T>
    {
        /// <summary>Gets the observer captured on subscription.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <summary>Gets the number of times the subscription handed to the subscriber was disposed.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return new ActionDisposable(() => DisposeCount++);
        }
    }

    /// <summary>Observable that runs a supplied subscription script synchronously.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="script">The subscription script.</param>
    private sealed class ScriptedObservable<T>(Action<IObserver<T>> script) : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            script(observer);
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Observable that returns a null subscription.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class NullSubscriptionObservable<T> : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnNext(default!);
            return null!;
        }
    }

    /// <summary>Records observer notifications.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class Recorder<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets observed errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of completion notifications.</summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => Values.Add(value);
    }
}
