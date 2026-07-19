// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Repeat operator parity tests for System.Reactive-compatible names.</summary>
public partial class RxNamesTests
{
    /// <summary>The value four, used to bound the infinite repeat test.</summary>
    private const int Four = 4;

    /// <summary>The expected values when a two-value source is repeated twice.</summary>
    private static readonly int[] _oneTwoOneTwo = [One, Two, One, Two];

    /// <summary>The expected values when a single-value source is repeated and bounded by Take.</summary>
    private static readonly int[] _threeRepeatedFourTimes = [Three, Three, Three, Three];

    /// <summary>Verifies that finite repeat resubscribes to a synchronous source and completes once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatCountRepeatsSynchronousSourceThenCompletes()
    {
        RecordingWitness<int> observer = new();

        using var subscription = Signal.FromEnumerable([One, Two]).Repeat(Two).Subscribe(observer);

        await Assert.That(observer.Values.SequenceEqual(_oneTwoOneTwo)).IsTrue();
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies that a zero repeat count completes without subscribing to the source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatCountZeroCompletesWithoutSubscribingToSource()
    {
        var subscriptions = 0;
        ScriptedObservable<int> source = new(observer =>
        {
            subscriptions++;
            observer.OnNext(One);
            observer.OnCompleted();
        });
        RecordingWitness<int> observer = new();

        using var subscription = source.Repeat(0).Subscribe(observer);

        await Assert.That(subscriptions).IsEqualTo(0);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies that indefinite repeat can be bounded by downstream disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatWithoutCountCanBeBoundedByTake()
    {
        RecordingWitness<int> observer = new();

        using var subscription = Signal.Emit(Three).Repeat().Take(Four).Subscribe(observer);

        await Assert.That(observer.Values.SequenceEqual(_threeRepeatedFourTimes)).IsTrue();
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies that repeat propagates a source error and does not resubscribe after it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatPropagatesSourceErrorWithoutResubscribing()
    {
        var subscriptions = 0;
        InvalidOperationException error = new(Boom);
        ScriptedObservable<int> source = new(observer =>
        {
            subscriptions++;
            observer.OnNext(One);
            observer.OnError(error);
        });
        RecordingWitness<int> observer = new();

        using var subscription = source.Repeat(Two).Subscribe(observer);

        await Assert.That(subscriptions).IsEqualTo(1);
        await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies a source subscribe exception is converted to one downstream error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatConvertsSourceSubscribeExceptionToSingleErrorWithoutResubscribing()
    {
        var subscriptions = 0;
        InvalidOperationException error = new(Boom);
        ScriptedObservable<int> source = new(_ =>
        {
            subscriptions++;
            throw error;
        });
        RecordingWitness<int> observer = new();

        using var subscription = source.Repeat(Two).Subscribe(observer);

        await Assert.That(subscriptions).IsEqualTo(1);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies duplicate source errors are ignored after the first terminal notification.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatIgnoresDuplicateSourceError()
    {
        InvalidOperationException error = new(Boom);
        InvalidOperationException duplicate = new("duplicate");
        ScriptedObservable<int> source = new(observer =>
        {
            observer.OnError(error);
            observer.OnError(duplicate);
        });
        RecordingWitness<int> observer = new();

        using var subscription = source.Repeat(Two).Subscribe(observer);

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies duplicate source completions do not schedule extra repetitions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatIgnoresDuplicateSourceCompletion()
    {
        var subscriptions = 0;
        ScriptedObservable<int> source = new(observer =>
        {
            subscriptions++;
            observer.OnCompleted();
            observer.OnCompleted();
        });
        RecordingWitness<int> observer = new();

        using var subscription = source.Repeat(Two).Subscribe(observer);

        await Assert.That(subscriptions).IsEqualTo(Two);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies a stale terminal callback from an earlier repeat attempt is ignored.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatIgnoresLateTerminalFromPreviousSubscription()
    {
        ManualRepeatSource<int> source = new();
        RecordingWitness<int> observer = new();

        using var subscription = source.Repeat(Two).Subscribe(observer);

        await Assert.That(source.SubscriptionCount).IsEqualTo(1);

        source.Complete(0);

        await Assert.That(source.SubscriptionCount).IsEqualTo(Two);
        await Assert.That(observer.Completed).IsEqualTo(0);

        source.Complete(0);
        source.Emit(0, Three);
        source.Error(0, new InvalidOperationException(Boom));

        await Assert.That(source.SubscriptionCount).IsEqualTo(Two);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Values.Count).IsEqualTo(0);

        source.Emit(1, Two);
        source.Complete(1);

        await Assert.That(observer.Values.SequenceEqual([Two])).IsTrue();
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies values produced after disposal are ignored by the active repeat attempt.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatIgnoresValueAfterDisposal()
    {
        ManualRepeatSource<int> source = new();
        RecordingWitness<int> observer = new();
        var subscription = source.Repeat(Two).Subscribe(observer);

        source.Emit(0, One);
        subscription.Dispose();
        source.Emit(0, Two);

        await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies errors produced after disposal are ignored by the active repeat attempt.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatIgnoresErrorAfterDisposal()
    {
        ManualRepeatSource<int> source = new();
        RecordingWitness<int> observer = new();
        var subscription = source.Repeat(Two).Subscribe(observer);
        InvalidOperationException error = new(Boom);

        subscription.Dispose();
        source.Error(0, error);

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies completion produced after disposal does not resubscribe or complete downstream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatIgnoresCompletionAfterDisposal()
    {
        ManualRepeatSource<int> source = new();
        RecordingWitness<int> observer = new();
        var subscription = source.Repeat(Two).Subscribe(observer);

        subscription.Dispose();
        source.Complete(0);

        await Assert.That(source.SubscriptionCount).IsEqualTo(1);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies the coordinator ignores completion for a stale repeat generation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatCoordinatorIgnoresStaleCompletionGeneration()
    {
        RecordingWitness<int> observer = new();
        RepeatSourceCoordinator<int> coordinator = new(new ScriptedObservable<int>(static _ => { }), Two, observer);

        using var subscription = coordinator.Run();
        coordinator.OnCompleted(0);

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies the repeat coordinator race guards return without signalling after disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatCoordinatorRaceGuardsReturnAfterDisposal()
    {
        RecordingWitness<int> observer = new();
        RepeatSourceCoordinator<int> coordinator = new(new ScriptedObservable<int>(static _ => { }), One, observer);

        coordinator.Dispose();
        coordinator.Complete();
        coordinator.ScheduleNext();
        coordinator.SubscribeNext();

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies Repeat validates null sources and negative counts.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RepeatRejectsInvalidArguments()
    {
        await Assert.That(static () => ((IObservable<int>)null!).Repeat()).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => ((IObservable<int>)null!).Repeat(One)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => Signal.Emit(One).Repeat(NegativeOne)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Observable that exposes repeat attempt observers for manual notification.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class ManualRepeatSource<T> : IObservable<T>
    {
        /// <summary>The subscribed observers.</summary>
        private readonly List<IObserver<T>> _observers = [];

        /// <summary>Gets the number of subscriptions created.</summary>
        internal int SubscriptionCount => _observers.Count;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observers.Add(observer);
            return EmptyDisposable.Instance;
        }

        /// <summary>Emits a value from the selected subscription.</summary>
        /// <param name="subscriptionIndex">The subscription index.</param>
        /// <param name="value">The value to emit.</param>
        internal void Emit(int subscriptionIndex, T value) =>
            _observers[subscriptionIndex].OnNext(value);

        /// <summary>Completes the selected subscription.</summary>
        /// <param name="subscriptionIndex">The subscription index.</param>
        internal void Complete(int subscriptionIndex) =>
            _observers[subscriptionIndex].OnCompleted();

        /// <summary>Fails the selected subscription.</summary>
        /// <param name="subscriptionIndex">The subscription index.</param>
        /// <param name="failure">The error to emit.</param>
        internal void Error(int subscriptionIndex, Exception failure) =>
            _observers[subscriptionIndex].OnError(failure);
    }
}
