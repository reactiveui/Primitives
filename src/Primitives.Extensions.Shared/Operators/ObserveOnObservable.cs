// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>
/// Marshals every source notification onto the supplied <see cref="ISequencer"/>, preserving order.
/// Replaces the <c>System.Reactive.Linq.Observable.ObserveOn</c> delegation behind the sync
/// <c>ObserveOnSafe</c> / <c>ObserveOnIf</c> helpers with our own queue-and-single-drain marshaller:
/// notifications are enqueued and a single drain pass is scheduled per burst (rather than one
/// scheduled action per item). The shared queue / gate / drain machinery lives in
/// <see cref="ScheduledDrainState{T}"/>; this sink only carries the forward-everything drain handling.
/// </summary>
/// <typeparam name="T">The element type of the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="scheduler">The scheduler every notification is delivered on.</param>
internal sealed class ObserveOnObservable<T>(IObservable<T> source, ISequencer scheduler) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        // The immediate scheduler runs scheduled work inline on the calling thread, so the
        // queue-and-drain machinery would be pure overhead: forward straight through.
        if (ReferenceEquals(scheduler, Sequencer.Immediate))
        {
            return source.Subscribe(observer);
        }

        ObserveOnSink sink = new(observer, scheduler);
        sink.AttachSourceSubscription(source.Subscribe(sink));
        return sink;
    }

    /// <summary>
    /// Single observer that queues upstream notifications and drains them on the scheduler thread in
    /// FIFO order. Terminal notifications travel through the same queue so they never overtake
    /// still-queued values.
    /// </summary>
    private sealed class ObserveOnSink : IObserver<T>, IDisposable, IDrainTarget
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _downstream;

        /// <summary>The gate protecting the queue and terminal state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Shared queue / scheduled-drain machinery.</summary>
        private readonly ScheduledDrainState<T> _state;

        /// <summary>Initializes a new instance of the <see cref="ObserveOnSink"/> class.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="scheduler">The scheduler notifications are delivered on.</param>
        public ObserveOnSink(IObserver<T> downstream, ISequencer scheduler)
        {
            _downstream = downstream;
            _state = new(scheduler, this, _gate);
        }

        /// <summary>Records the upstream subscription so <see cref="Dispose"/> can tear it down.</summary>
        /// <param name="subscription">The upstream subscription handle.</param>
        public void AttachSourceSubscription(IDisposable subscription) => _state.Attach(subscription);

        /// <inheritdoc/>
        public void OnNext(T value) => _state.EnqueueNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error) => _state.EnqueueError(error);

        /// <inheritdoc/>
        public void OnCompleted() => _state.EnqueueCompleted();

        /// <inheritdoc/>
        public void Dispose() => _state.BeginDispose()?.Dispose();

        /// <inheritdoc/>
        void IDrainTarget.Drain()
        {
            while (_state.TryDequeue(out var notification))
            {
                switch (notification.Kind)
                {
                    case DrainNotificationKind.Next:
                        {
                            _downstream.OnNext(notification.Value);
                            break;
                        }

                    case DrainNotificationKind.Error:
                        {
                            _state.Terminate();
                            _downstream.OnError(notification.Error!);
                            return;
                        }

                    default:
                        {
                            // DrainNotificationKind has only three values; the discard arm absorbs Completed.
                            _state.Terminate();
                            _downstream.OnCompleted();
                            return;
                        }
                }
            }
        }
    }
}
