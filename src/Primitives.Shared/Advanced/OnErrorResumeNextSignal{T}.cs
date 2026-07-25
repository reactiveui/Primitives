// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Continues through observable sources after either completion or error.</summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class OnErrorResumeNextSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The sources to subscribe in order.</summary>
    private readonly IEnumerable<IObservable<T>> _sources;

    /// <summary>Initializes a new instance of the <see cref="OnErrorResumeNextSignal{T}"/> class.</summary>
    /// <param name="sources">The sources to subscribe in order.</param>
    internal OnErrorResumeNextSignal(IEnumerable<IObservable<T>> sources) => _sources = sources;

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new Coordinator(observer).Run(_sources);
    }

    /// <summary>Coordinates ordered subscriptions while swallowing source errors.</summary>
    private sealed class Coordinator : IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Serializes enumeration, terminal rescheduling, and disposal.</summary>
        private readonly Lock _gate = new();

        /// <summary>The active source subscription.</summary>
        private readonly SwapDisposable _subscription = new();

        /// <summary>The active source enumerator.</summary>
        private IEnumerator<IObservable<T>>? _enumerator;

        /// <summary>The recursive current-thread rescheduler.</summary>
        private Action? _nextSelf;

        /// <summary>The active recursive schedule.</summary>
        private IDisposable _schedule = EmptyDisposable.Instance;

        /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="Coordinator"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal Coordinator(IObserver<T> observer) => _observer = observer;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            Dispose(true);
        }

        /// <summary>Starts iterating and subscribing to sources.</summary>
        /// <param name="sources">The sources to subscribe in order.</param>
        /// <returns>The coordinator that owns the subscriptions.</returns>
        internal Coordinator Run(IEnumerable<IObservable<T>> sources)
        {
            try
            {
                _enumerator = sources.GetEnumerator();
            }
            catch (Exception error)
            {
                Fail(error);
                return this;
            }

            var schedule = Sequencer.CurrentThread.Schedule(SubscribeNext);
            _schedule = schedule;
            if (Volatile.Read(ref _disposed) != 0)
            {
                schedule.Dispose();
            }

            return this;
        }

        /// <summary>Subscribes to the next source or completes when the sequence is exhausted.</summary>
        /// <param name="self">The current-thread recursive scheduler.</param>
        private void SubscribeNext(Action self)
        {
            IObservable<T>? source = null;
            Exception? error = null;
            var completed = false;
            var disposed = false;

            lock (_gate)
            {
                _nextSelf = self;
                disposed = Volatile.Read(ref _disposed) != 0;
                if (!disposed)
                {
                    ReadNextSource(out source, out error, out completed);
                }
            }

            if (disposed)
            {
                return;
            }

            if (error is not null)
            {
                Fail(error);
                return;
            }

            if (completed)
            {
                Complete();
                return;
            }

            _subscription.Disposable = source!.Subscribe(_observer.OnNext, _ => ScheduleNext(), ScheduleNext);
        }

        /// <summary>Reads the next source from the active enumerator.</summary>
        /// <param name="source">The next source when available.</param>
        /// <param name="error">The enumeration error when reading fails.</param>
        /// <param name="completed">Whether enumeration has completed.</param>
        private void ReadNextSource(out IObservable<T>? source, out Exception? error, out bool completed)
        {
            source = null;
            error = null;
            completed = false;

            try
            {
                var enumerator = _enumerator;
                if (enumerator?.MoveNext() != true)
                {
                    completed = true;
                    return;
                }

                source = enumerator.Current
                         ?? throw new InvalidOperationException("OnErrorResumeNext source contained null.");
            }
            catch (Exception exception)
            {
                error = exception;
            }
        }

        /// <summary>Schedules the next source without re-entering the current source terminal callback.</summary>
        private void ScheduleNext()
        {
            Action? next;
            lock (_gate)
            {
                if (Volatile.Read(ref _disposed) != 0)
                {
                    return;
                }

                next = _nextSelf;
            }

            next?.Invoke();
        }

        /// <summary>Forwards an error and releases owned resources.</summary>
        /// <param name="error">The error to forward.</param>
        private void Fail(Exception error)
        {
            switch (Interlocked.Exchange(ref _disposed, 1))
            {
                case 0:
                    {
                        try
                        {
                            _observer.OnError(error);
                        }
                        finally
                        {
                            Dispose(true);
                        }

                        break;
                    }
            }
        }

        /// <summary>Forwards completion and releases owned resources.</summary>
        private void Complete()
        {
            switch (Interlocked.Exchange(ref _disposed, 1))
            {
                case 0:
                    {
                        try
                        {
                            _observer.OnCompleted();
                        }
                        finally
                        {
                            Dispose(true);
                        }

                        break;
                    }
            }
        }

        /// <summary>Releases managed resources.</summary>
        /// <param name="disposing">Whether managed resources should be released.</param>
        private void Dispose(bool disposing)
        {
            _ = disposing;
            _schedule.Dispose();
            _subscription.Dispose();
            _enumerator?.Dispose();
            _enumerator = null;
        }
    }
}
