// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies disposed subscriptions stop receiving notifications immediately, including reentrant sibling disposal.</summary>
public sealed class DisposedSubscriptionDeliveryTests
{
    /// <summary>First sample item.</summary>
    private const int FirstItem = 1;

    /// <summary>Second sample item.</summary>
    private const int SecondItem = 2;

    /// <summary>Third sample item.</summary>
    private const int ThirdItem = 3;

    /// <summary>Reproduces issue 251 when the upstream source faults and tears down sibling subscriptions inside <c>Signal.Create</c>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SignalCreate_WhenUpstreamFaults_DoesNotDeliverOnErrorToDisposedSibling()
    {
        using Signal<int> source = new();
        List<string> events = [];

        var created = Signal.Create<int>(observer =>
        {
            var published = source.Publish();

            var first = published.SubscribeSafe(observer);

            var second = published.Subscribe(
                value => events.Add($"sub2.OnNext({value})"),
                error => events.Add($"sub2.OnError({error.Message})"),
                () => events.Add("sub2.OnCompleted()"));

            var connection = published.Connect();

            return new ActionDisposable(() =>
            {
                events.Add("connection.Dispose()");
                connection.Dispose();
                events.Add("sub1.Dispose()");
                first.Dispose();
                events.Add("sub2.Dispose()");
                second.Dispose();
            });
        });

        using var outer = created.Subscribe(
            value => events.Add($"outer.OnNext({value})"),
            error => events.Add($"outer.OnError({error.Message})"),
            () => events.Add("outer.OnCompleted()"));

        source.OnNext(FirstItem);
        source.OnNext(SecondItem);
        source.OnNext(ThirdItem);
        source.OnError(new InvalidOperationException("Test"));

        await Assert.That(events.Contains("sub2.OnError(Test)")).IsFalse();
        var secondDisposeIndex = events.IndexOf("sub2.Dispose()");
        await Assert.That(secondDisposeIndex).IsGreaterThan(0);
        await Assert.That(events.Skip(secondDisposeIndex + 1).Any(static item => item.StartsWith("sub2.", StringComparison.Ordinal))).IsFalse();
    }

    /// <summary>Reproduces issue 251 for completion when <c>Signal.Create</c> disposes sibling subscriptions during <c>OnCompleted</c>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SignalCreate_WhenUpstreamCompletes_DoesNotDeliverOnCompletedToDisposedSibling()
    {
        using Signal<int> source = new();
        List<string> events = [];

        var created = Signal.Create<int>(observer =>
        {
            var published = source.Publish();
            var forward = published.SubscribeSafe(observer);
            var sibling = published.Subscribe(
                value => events.Add($"sibling.OnNext({value})"),
                error => events.Add($"sibling.OnError({error.Message})"),
                () => events.Add("sibling.OnCompleted()"));
            var connection = published.Connect();

            return new ActionDisposable(() =>
            {
                connection.Dispose();
                events.Add("sibling.Dispose()");
                sibling.Dispose();
                forward.Dispose();
            });
        });

        using var outer = created.Subscribe(
            value => events.Add($"outer.OnNext({value})"),
            error => events.Add($"outer.OnError({error.Message})"),
            () => events.Add("outer.OnCompleted()"));

        source.OnNext(FirstItem);
        source.OnCompleted();

        await Assert.That(events.Contains("sibling.OnCompleted()")).IsFalse();
    }

    /// <summary>Direct <see cref="IObserver{T}"/> and action subscriptions on <see cref="Signal{T}"/> stop receiving when disposed mid-broadcast.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Signal_ReentrantSiblingDispose_SuppressesObserverAndActionDelivery()
    {
        using Signal<int> nextSignal = new();
        IDisposable secondObserverSub = EmptyDisposable.Instance;
        IDisposable secondActionSub = EmptyDisposable.Instance;
        RecordingWitness<int> secondObserver = new();
        List<int> actionValues = [];

        using var firstSub = nextSignal.Subscribe(_ =>
        {
            secondObserverSub.Dispose();
            secondObserverSub.Dispose();
            secondActionSub.Dispose();
        });
        secondObserverSub = nextSignal.Subscribe(secondObserver);
        secondActionSub = nextSignal.Subscribe(actionValues.Add);

        nextSignal.OnNext(FirstItem);
        ((Signal<int>.SignalSubscription)secondActionSub).OnNext(SecondItem);

        await Assert.That(secondObserver.Values).IsEmpty();
        await Assert.That(actionValues).IsEmpty();

        using Signal<int> errorSignal = new();
        IDisposable errorObserverSub = EmptyDisposable.Instance;
        IDisposable errorActionSub = EmptyDisposable.Instance;
        RecordingWitness<int> errorSecond = new();
        using var errorFirst = errorSignal.Subscribe(
            static _ => { },
            _ =>
            {
                errorObserverSub.Dispose();
                errorActionSub.Dispose();
            });
        errorObserverSub = errorSignal.Subscribe(errorSecond);
        errorActionSub = errorSignal.Subscribe(static _ => { });

        errorSignal.OnError(new InvalidOperationException("handled"));
        await Assert.That(errorSecond.Errors).IsEmpty();

        using Signal<int> completeSignal = new();
        IDisposable completeObserverSub = EmptyDisposable.Instance;
        RecordingWitness<int> completeSecond = new();
        using var completeFirst = completeSignal.Subscribe(
            static _ => { },
            () => completeObserverSub.Dispose());
        completeObserverSub = completeSignal.Subscribe(completeSecond);

        completeSignal.OnCompleted();
        await Assert.That(completeSecond.Completed).IsEqualTo(0);
    }

    /// <summary><see cref="Witness.Subscribe{T}(IObservable{T}, Action{T}, Action{Exception}, Action)"/> suppresses post-disposal and duplicate terminal callbacks.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WitnessSubscribe_MisbehavingObservable_SuppressesPostDisposalAndDuplicateTerminals()
    {
        RecordingDisposable disposedHandle = new();
        ManualObservable<int> disposedSource = new(disposedHandle);
        List<int> values = [];
        List<Exception> errors = [];
        var completions = 0;

        var subscription = Witness.Subscribe(
            disposedSource,
            values.Add,
            errors.Add,
            () => completions++);

        disposedSource.PushNext(FirstItem);
        subscription.Dispose();
        disposedSource.PushNext(SecondItem);
        disposedSource.PushError(new InvalidOperationException("after-dispose"));
        disposedSource.PushCompleted();

        await Assert.That(values.SequenceEqual([FirstItem])).IsTrue();
        await Assert.That(errors).IsEmpty();
        await Assert.That(completions).IsEqualTo(0);
        await Assert.That(disposedHandle.DisposeCount).IsEqualTo(1);

        RecordingDisposable duplicateErrorHandle = new();
        ManualObservable<int> duplicateErrorSource = new(duplicateErrorHandle);
        List<Exception> singleErrors = [];
        using var errorSub = Witness.Subscribe(
            duplicateErrorSource,
            static _ => { },
            singleErrors.Add,
            static () => { });

        InvalidOperationException boom = new("boom");
        duplicateErrorSource.PushError(boom);
        duplicateErrorSource.PushError(new InvalidOperationException("second"));
        duplicateErrorSource.PushNext(FirstItem);

        await Assert.That(singleErrors.SequenceEqual([boom])).IsTrue();
        await Assert.That(duplicateErrorHandle.DisposeCount).IsEqualTo(1);

        RecordingDisposable syncReturnedHandle = new();
        SynchronousCompletingObservable<int> syncComplete = new(syncReturnedHandle);
        var syncCompletions = 0;
        var syncHandle = Witness.Subscribe(
            syncComplete,
            static _ => { },
            static _ => { },
            () => syncCompletions++);

        await Assert.That(syncCompletions).IsEqualTo(1);
        await Assert.That(syncReturnedHandle.DisposeCount).IsEqualTo(1);
        await Assert.That(syncHandle).IsSameReferenceAs(EmptyDisposable.Instance);
    }

    /// <summary><see cref="ReplaySignal{T}"/>, <see cref="BehaviorSignal{T}"/>, and <see cref="StateSignal{T}"/> stop delivering to a sibling disposed mid-broadcast.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SerializedSubjects_ReentrantSiblingDispose_SuppressesRemainingBroadcast()
    {
        using ReplaySignal<int> replay = new();
        IDisposable replaySiblingSub = EmptyDisposable.Instance;
        RecordingWitness<int> replaySibling = new();
        using var replayFirst = replay.Subscribe(
            _ =>
            {
                replaySiblingSub.Dispose();
                replaySiblingSub.Dispose();
            },
            _ => replaySiblingSub.Dispose(),
            () => replaySiblingSub.Dispose());
        replaySiblingSub = replay.Subscribe(replaySibling);
        replay.OnNext(FirstItem);
        await Assert.That(replaySibling.Values).IsEmpty();

        replaySiblingSub = replay.Subscribe(replaySibling);
        replaySibling.Values.Clear();
        replay.OnError(new InvalidOperationException("replay-err"));
        await Assert.That(replaySibling.Errors).IsEmpty();

        using BehaviorSignal<int> behavior = new(0);
        IDisposable behaviorSiblingSub = EmptyDisposable.Instance;
        RecordingWitness<int> behaviorSibling = new();
        using var behaviorFirst = behavior.Subscribe(
            value =>
            {
                if (value == FirstItem)
                {
                    behaviorSiblingSub.Dispose();
                }
            },
            static _ => { },
            () => behaviorSiblingSub.Dispose());
        behaviorSiblingSub = behavior.Subscribe(behaviorSibling);
        behaviorSibling.Values.Clear();
        behavior.OnNext(FirstItem);
        await Assert.That(behaviorSibling.Values).IsEmpty();

        behaviorSiblingSub = behavior.Subscribe(behaviorSibling);
        behaviorSibling.Values.Clear();
        behavior.OnCompleted();
        await Assert.That(behaviorSibling.Completed).IsEqualTo(0);

        using StateSignal<int> state = new(0);
        IDisposable stateSiblingSub = EmptyDisposable.Instance;
        RecordingWitness<int> stateSibling = new();
        using var stateFirst = state.Subscribe(value =>
        {
            if (value == FirstItem)
            {
                stateSiblingSub.Dispose();
            }
        });
        stateSiblingSub = state.Subscribe(stateSibling);
        stateSibling.Values.Clear();
        state.OnNext(FirstItem);
        await Assert.That(stateSibling.Values).IsEmpty();
    }

    /// <summary>
    /// <see cref="DelayableNotificationSignal{T}"/>, <see cref="ProjectedReadOnlyState{TSource, TResult}"/>, <see cref="SliceWindow{T}"/>,
    /// and <see cref="AsyncSignal{T}"/> stop delivering when disposed mid-broadcast.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DetachableObserverOwners_ReentrantSiblingDispose_SuppressesDelivery()
    {
        await VerifyDelayableAndProjected();
        await VerifySliceWindowAndAsyncSignal();
    }

    /// <summary>Verifies <see cref="DelayableNotificationSignal{T}"/> and <see cref="ProjectedReadOnlyState{TSource, TResult}"/> sibling disposal.</summary>
    /// <returns>A task representing the asynchronous assertions.</returns>
    private static async Task VerifyDelayableAndProjected()
    {
        using DelayableNotificationSignal<int> delayable = new(static () => false, static items => items);
        IDisposable siblingSub = EmptyDisposable.Instance;
        RecordingWitness<int> sibling = new();
        using var first = delayable.Subscribe(
            _ => siblingSub.Dispose(),
            _ => siblingSub.Dispose(),
            () => siblingSub.Dispose());
        siblingSub = delayable.Subscribe(sibling);
        delayable.OnNext(FirstItem);
        await Assert.That(sibling.Values).IsEmpty();

        siblingSub = delayable.Subscribe(sibling);
        delayable.OnError(new InvalidOperationException("delay-err"));
        await Assert.That(sibling.Errors).IsEmpty();

        using StateSignal<int> sourceState = new(0);
        using var projected = ProjectedReadOnlyState<int, int>.Create(sourceState, static x => x);
        IDisposable projectedSiblingSub = EmptyDisposable.Instance;
        RecordingWitness<int> projectedSibling = new();
        using var projectedFirst = projected.Subscribe(
            value =>
            {
                if (value == FirstItem)
                {
                    projectedSiblingSub.Dispose();
                }
            },
            static _ => { },
            () => projectedSiblingSub.Dispose());
        projectedSiblingSub = projected.Subscribe(projectedSibling);
        projectedSibling.Values.Clear();
        sourceState.OnNext(FirstItem);
        await Assert.That(projectedSibling.Values).IsEmpty();

        projectedSiblingSub = projected.Subscribe(projectedSibling);
        projectedSibling.Values.Clear();
        sourceState.OnCompleted();
        await Assert.That(projectedSibling.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies <see cref="SliceWindow{T}"/> and <see cref="AsyncSignal{T}"/> sibling disposal.</summary>
    /// <returns>A task representing the asynchronous assertions.</returns>
    private static async Task VerifySliceWindowAndAsyncSignal()
    {
        SliceWindow<int> window = new(new());
        IDisposable windowSiblingSub = EmptyDisposable.Instance;
        RecordingWitness<int> windowSibling = new();
        using var windowFirst = window.Subscribe(
            _ => windowSiblingSub.Dispose(),
            static _ => { },
            () => windowSiblingSub.Dispose());
        windowSiblingSub = window.Subscribe(windowSibling);
        window.Publish(FirstItem);
        await Assert.That(windowSibling.Values).IsEmpty();

        windowSiblingSub = window.Subscribe(windowSibling);
        window.Complete();
        await Assert.That(windowSibling.Completed).IsEqualTo(0);

        using AsyncSignal<int> asyncSignal = new();
        IDisposable asyncSiblingSub = EmptyDisposable.Instance;
        RecordingWitness<int> asyncSibling = new();
        using var asyncFirst = asyncSignal.Subscribe(_ => asyncSiblingSub.Dispose());
        asyncSiblingSub = asyncSignal.Subscribe(asyncSibling);
        asyncSignal.OnNext(FirstItem);
        asyncSignal.OnCompleted();
        await Assert.That(asyncSibling.Values).IsEmpty();
        await Assert.That(asyncSibling.Completed).IsEqualTo(0);
    }

    /// <summary>Test observable that retains its observer even after its subscription handle is disposed.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="subscription">The subscription handle returned from <see cref="Subscribe"/>.</param>
    private sealed class ManualObservable<T>(IDisposable subscription) : IObservable<T>
    {
        /// <summary>The subscription handle returned to subscribers.</summary>
        private readonly IDisposable _subscription = subscription;

        /// <summary>The most recently subscribed observer.</summary>
        private IObserver<T>? _observer;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observer = observer;
            return _subscription;
        }

        /// <summary>Pushes a value directly to the retained observer.</summary>
        /// <param name="value">The value to push.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PushNext(T value) => _observer?.OnNext(value);

        /// <summary>Pushes an error directly to the retained observer.</summary>
        /// <param name="error">The error to push.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PushError(Exception error) => _observer?.OnError(error);

        /// <summary>Pushes completion directly to the retained observer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PushCompleted() => _observer?.OnCompleted();
    }

    /// <summary>Test observable that completes synchronously inside <see cref="Subscribe"/> before returning a disposable handle.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="subscription">The disposable returned from <see cref="Subscribe"/>.</param>
    private sealed class SynchronousCompletingObservable<T>(IDisposable subscription) : IObservable<T>
    {
        /// <summary>The disposable returned from <see cref="Subscribe"/>.</summary>
        private readonly IDisposable _subscription = subscription;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnCompleted();
            return _subscription;
        }
    }
}
