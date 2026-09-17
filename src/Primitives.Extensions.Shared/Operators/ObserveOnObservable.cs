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
internal sealed class ObserveOnObservable<T>(IObservable<T> source, ISequencer scheduler) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (ReferenceEquals(scheduler, Sequencer.Immediate))
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
                            _state.Terminate();
                            _downstream.OnCompleted();
                            return;
                        }
                }
            }
        }
    }
}
