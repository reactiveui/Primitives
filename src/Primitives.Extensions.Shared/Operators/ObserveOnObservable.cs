// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Delivers source notifications in order on the supplied sequencer, scheduling one drain per burst.</summary>
/// <typeparam name="T">The element type of the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="scheduler">The scheduler every notification is delivered on.</param>
public sealed class ObserveOnObservable<T>(IObservable<T> source, ISequencer scheduler) : IObservable<T>
{
    /// <summary>Returns <paramref name="source"/> unchanged when <paramref name="scheduler"/> is <see langword="null"/>; otherwise schedules callbacks on <paramref name="scheduler"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">Optional scheduler to notify observers on.</param>
    /// <returns>The scheduled or original observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> CreateSafe(IObservable<T> source, ISequencer? scheduler) =>
        scheduler is null ? source : new ObserveOnObservable<T>(source, scheduler);

    /// <summary>Schedules callbacks on <paramref name="scheduler"/> when <paramref name="condition"/> holds; otherwise returns <paramref name="source"/> unchanged.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="condition">Whether to apply <paramref name="scheduler"/>.</param>
    /// <param name="scheduler">The scheduler used when <paramref name="condition"/> is <see langword="true"/>.</param>
    /// <returns>The scheduled or original observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> CreateIf(IObservable<T> source, bool condition, ISequencer scheduler) =>
        condition ? new ObserveOnObservable<T>(source, scheduler) : source;

    /// <summary>Schedules callbacks on <paramref name="trueScheduler"/> when <paramref name="condition"/> holds and on <paramref name="falseScheduler"/> otherwise.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="condition">Whether to select <paramref name="trueScheduler"/>.</param>
    /// <param name="trueScheduler">The scheduler used when <paramref name="condition"/> is <see langword="true"/>.</param>
    /// <param name="falseScheduler">The scheduler used when <paramref name="condition"/> is <see langword="false"/>.</param>
    /// <returns>The scheduled observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ObserveOnObservable<T> CreateIf(
        IObservable<T> source,
        bool condition,
        ISequencer trueScheduler,
        ISequencer falseScheduler) =>
        condition ? new(source, trueScheduler) : new(source, falseScheduler);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (scheduler.IsImmediate)
        {
            return source.Subscribe(observer);
        }

        ObserveOnSink sink = new(observer, scheduler);
        sink.AttachSourceSubscription(source.Subscribe(sink));
        return sink;
    }

    /// <summary>Queues values and terminal notifications together for ordered delivery on the scheduler.</summary>
    private sealed class ObserveOnSink : IObserver<T>, IDisposable, IDrainTarget
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _downstream;

        /// <summary>The gate protecting the queue and terminal state.</summary>
        private readonly Lock _gate = new();

        /// <summary>The notification queue and scheduled-drain bookkeeping shared with the drain loop.</summary>
        private readonly ScheduledDrainState<T> _state;

        /// <summary>Initializes a new instance of the <see cref="ObserveOnSink"/> class.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="scheduler">The scheduler notifications are delivered on.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Correctness",
            "SST2403:Do not let 'this' escape from a constructor",
            Justification =
                "_state is owned solely by this sink and only stores the back-reference, so 'this' never escapes construction.")]
        public ObserveOnSink(IObserver<T> downstream, ISequencer scheduler)
        {
            _downstream = downstream;
            _state = new(scheduler, this, _gate);
        }

        /// <summary>Records the upstream subscription so <see cref="Dispose"/> can tear it down.</summary>
        /// <param name="subscription">The upstream subscription handle.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AttachSourceSubscription(IDisposable subscription) => _state.Attach(subscription);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _state.EnqueueNext(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _state.EnqueueError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _state.EnqueueCompleted();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _state.Dispose();

        /// <inheritdoc/>
        void IDrainTarget.Drain()
        {
            while (_state.TryDequeue(out var notification))
            {
                switch (notification.Kind)
                {
                    case DrainNotificationKind.Next:
                        {
                            try
                            {
                                _downstream.OnNext(notification.Value);
                            }
                            catch
                            {
                                Dispose();
                                throw;
                            }

                            break;
                        }

                    case DrainNotificationKind.Error:
                        {
                            var subscription = _state.BeginDispose();
                            try
                            {
                                _downstream.OnError(notification.Error!);
                            }
                            finally
                            {
                                subscription?.Dispose();
                            }

                            return;
                        }

                    default:
                        {
                            var subscription = _state.BeginDispose();
                            try
                            {
                                _downstream.OnCompleted();
                            }
                            finally
                            {
                                subscription?.Dispose();
                            }

                            return;
                        }
                }
            }
        }
    }
}
