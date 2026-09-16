// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Retries the source observable sequence upon error, with a delay selected by a function.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="retryCount">The maximum number of retries.</param>
/// <param name="delaySelector">A function to select the delay for each retry attempt.</param>
/// <param name="sequencer">The sequencer timing retry delays; <c>null</c> uses the default sequencer.</param>
internal sealed class RetryWithDelayObservable<T>(
    IObservable<T> source,
    int retryCount,
    Func<int, TimeSpan> delaySelector,
    ISequencer? sequencer = null) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(delaySelector);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        RetryWithDelaySink sink = new(observer, source, retryCount, delaySelector, sequencer ?? Sequencer.Default);
        sink.Run();
        return sink;
    }

    /// <summary>Sink that manages retries with a custom delay selector.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="source">The source observable.</param>
    /// <param name="maxRetries">The maximum number of retries.</param>
    /// <param name="delaySelector">The delay selector.</param>
    /// <param name="scheduler">The scheduler used to time retry delays.</param>
    /// <remarks>
    /// The retry count is advanced only by the source's serialized notifications, so no lock is needed; the observer, the
    /// delay selector and the source subscription all run without one.
    /// </remarks>
    private sealed class RetryWithDelaySink(
        IObserver<T> downstream,
        IObservable<T> source,
        int maxRetries,
        Func<int, TimeSpan> delaySelector,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The subscription to the source sequence or the pending retry; an assignment after disposal is disposed at once.</summary>
        private readonly MutableDisposable _subscription = new();

        /// <summary>The number of retries already attempted.</summary>
        private int _retries;

        /// <summary>Whether the sink has been disposed.</summary>
        private bool _disposed;

        /// <summary>Starts the retry process.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Run() => SubscribeToSource();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => downstream.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Volatile.Read(ref _disposed))
            {
                return;
            }

            if (_retries < maxRetries)
            {
                var delay = delaySelector(_retries);
                _retries++;

                if (delay == TimeSpan.Zero)
                {
                    SubscribeToSource();
                }
                else
                {
                    _subscription.Disposable = scheduler.Schedule(this, delay, static (_, self) =>
                    {
                        self.SubscribeToSource();
                        return EmptyDisposable.Instance;
                    });
                }
            }
            else
            {
                downstream.OnError(error);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();

        /// <inheritdoc/>
        public void Dispose()
        {
            Volatile.Write(ref _disposed, true);
            _subscription.Dispose();
        }

        /// <summary>Subscribes to the source sequence unless the sink has been disposed.</summary>
        private void SubscribeToSource()
        {
            if (Volatile.Read(ref _disposed))
            {
                return;
            }

            _subscription.Disposable = source.Subscribe(this);
        }
    }
}
