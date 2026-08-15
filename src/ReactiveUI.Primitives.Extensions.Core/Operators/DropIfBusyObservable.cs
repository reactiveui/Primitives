// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Operator that drops source elements while an asynchronous action is in progress.
/// Replaces the closure-based implementation in ReactiveExtensions.DropIfBusy.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="asyncAction">The asynchronous action to execute for each forwarded element.</param>
public sealed class DropIfBusyObservable<T>(
    IObservable<T> source,
    Func<T, ValueTask> asyncAction) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(asyncAction);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        DropIfBusySink sink = new(observer, asyncAction);
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

        /// <summary>Non-zero once the sink is terminal.</summary>
        private int _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _done) != 0)
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
            if (Interlocked.Exchange(ref _done, 1) != 0)
            {
                return;
            }

            downstream.OnError(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            Volatile.Write(ref _done, 1);
            downstream.OnCompleted();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Volatile.Write(ref _done, 1);

        /// <summary>Executes the async action and manages the busy state transition.</summary>
        /// <param name="value">The value to process.</param>
        /// <returns>A task representing the async operation.</returns>
        private async Task ProcessAsync(T value)
        {
            try
            {
                await asyncAction(value).ConfigureAwait(false);
                if (Volatile.Read(ref _done) != 0)
                {
                    return;
                }

                downstream.OnNext(value);
            }
            catch (Exception ex)
            {
                if (Volatile.Read(ref _done) != 0)
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
