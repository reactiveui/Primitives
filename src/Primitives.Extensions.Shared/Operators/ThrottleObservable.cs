// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Emits the latest value after the configured quiet period.</summary>
/// <typeparam name="T">The element type of the source observable.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="dueTime">The quiescence duration required before emission.</param>
/// <param name="scheduler">The scheduler used to time emissions.</param>
public sealed class ThrottleObservable<T>(
    IObservable<T> source,
    TimeSpan dueTime,
    ISequencer scheduler) : IObservable<T>
{
    /// <summary>Creates a throttled sequence on <paramref name="scheduler"/>, validating <paramref name="scheduler"/> eagerly.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The quiescence duration required before emission.</param>
    /// <param name="scheduler">The scheduler used to time emissions.</param>
    /// <returns>A throttled observable sequence.</returns>
    public static ThrottleObservable<T> Create(
        IObservable<T> source,
        TimeSpan dueTime,
        ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);
        return new(source, dueTime, scheduler);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ThrottleSink sink = new(observer, dueTime, scheduler);
        sink.Attach(source.Subscribe(sink));
        return sink;
    }

    /// <summary>Sink that holds the most-recent value and a scheduled emission that fires after the configured quiescence interval.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="dueTime">The quiescence duration.</param>
    /// <param name="scheduler">The scheduler used to time emissions.</param>
    internal sealed class ThrottleSink(
        IObserver<T> downstream,
        TimeSpan dueTime,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The synchronization gate.</summary>
        private readonly Lock _gate = new();

        /// <summary>The pending scheduled emission.</summary>
        private readonly SwapDisposable _pending = new();

        /// <summary>Upstream subscription handle.</summary>
        private IDisposable? _subscription;

        /// <summary>The most-recent value waiting to be emitted.</summary>
        private T _latest = default!;

        /// <summary>Whether a value is currently pending emission.</summary>
        private bool _hasValue;

        /// <summary>Monotonic id of the latest scheduled emission.</summary>
        private long _emissionId;

        /// <summary>Whether the sequence is terminally done.</summary>
        private bool _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            long id;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _latest = value;
                _hasValue = true;
                id = ++_emissionId;
            }

            _pending.Disposable = scheduler.Schedule((this, id), dueTime, static (_, state) =>
            {
                state.Item1.Emit(state.id);
                return EmptyDisposable.Instance;
            });
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
            }

            _pending.Dispose();
            SinkSubscription.Dispose(ref _subscription);
            downstream.OnError(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            T pending;
            bool flush;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                flush = _hasValue;
                pending = _latest;
                _latest = default!;
                _hasValue = false;
            }

            _pending.Dispose();
            SinkSubscription.Dispose(ref _subscription);

            if (flush)
            {
                downstream.OnNext(pending);
            }

            downstream.OnCompleted();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
                _latest = default!;
                _hasValue = false;
            }

            _pending.Dispose();
            SinkSubscription.Dispose(ref _subscription);
        }

        /// <summary>Attaches the upstream subscription handle.</summary>
        /// <param name="subscription">The upstream subscription handle.</param>
        internal void Attach(IDisposable? subscription)
        {
            if (subscription is not null)
            {
                SinkSubscription.Set(ref _subscription, subscription);
            }
        }

        /// <summary>Emits the buffered value when it is still current, meaning no newer <see cref="OnNext"/> arrived after this emission was scheduled.</summary>
        /// <param name="id">The emission id this callback was scheduled for.</param>
        internal void Emit(long id)
        {
            T value;
            lock (_gate)
            {
                if (_done || id != _emissionId || !_hasValue)
                {
                    return;
                }

                value = _latest;
                _latest = default!;
                _hasValue = false;
            }

            try
            {
                downstream.OnNext(value);
            }
            catch
            {
                Dispose();
                throw;
            }
        }
    }
}
