// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Operator that drops source elements while an asynchronous action is in progress.
/// Replaces the closure-based implementation in ReactiveExtensions.DropIfBusy.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="asyncAction">The asynchronous action to execute for each forwarded element.</param>
internal sealed class DropIfBusyObservable<T>(
    IObservable<T> source,
    Func<T, ValueTask> asyncAction) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(asyncAction);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        var sink = new DropIfBusySink(observer, asyncAction);
        var sub = source.Subscribe(sink);
        return new DisposableBag(sub, sink);
    }

    /// <summary>Sink that manages the busy state and executes the async action.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="asyncAction">The async action to run.</param>
    private sealed class DropIfBusySink(
        IObserver<T> downstream,
        Func<T, ValueTask> asyncAction) : IObserver<T>, IDisposable
    {
        /// <summary>0 = idle, 1 = busy.</summary>
        private int _isBusy;

        /// <summary>Whether the sink is terminal.</summary>
        private bool _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (_done)
            {
                return;
            }

            // If we can transition from 0 to 1, we handle this value.
            if (Interlocked.CompareExchange(ref _isBusy, 1, 0) != 0)
            {
                return;
            }

            _ = ProcessAsync(value);
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            downstream.OnError(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            _done = true;
            downstream.OnCompleted();
        }

        /// <inheritdoc/>
        public void Dispose() => _done = true;

        /// <summary>Executes the async action and manages the busy state transition.</summary>
        /// <param name="value">The value to process.</param>
        /// <returns>A task representing the async operation.</returns>
        private async Task ProcessAsync(T value)
        {
            try
            {
                await asyncAction(value).ConfigureAwait(false);
                if (_done)
                {
                    return;
                }

                downstream.OnNext(value);
            }
            catch (Exception ex)
            {
                if (_done)
                {
                    return;
                }

                downstream.OnError(ex);
            }
            finally
            {
                Volatile.Write(ref _isBusy, 0);
            }
        }
    }
}
