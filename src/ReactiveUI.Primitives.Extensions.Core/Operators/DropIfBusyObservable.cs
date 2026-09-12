// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Runs <paramref name="asyncAction"/> for each source value and forwards the value once it finishes, dropping every
/// value that arrives while an action is in flight. An exception from the action terminates the sequence.
/// </summary>
/// <typeparam name = "T">The element type.</typeparam>
/// <param name = "source">The source observable.</param>
/// <param name = "asyncAction">The asynchronous action to execute for each forwarded element.</param>
public sealed class DropIfBusyObservable<T>(IObservable<T> source, Func<T, ValueTask> asyncAction) : IObservable<T>
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

    /// <summary>Processes source values and owns the subscription state.</summary>
    /// <param name = "downstream">The downstream observer.</param>
    /// <param name = "asyncAction">The asynchronous operation.</param>
    internal sealed class DropIfBusySink(IObserver<T> downstream, Func<T, ValueTask> asyncAction) : IObserver<T>, IDisposable
    {
        /// <summary>0 = idle, 1 = busy.</summary>
        private int _isBusy;

        /// <summary>Non-zero once the sink is terminal.</summary>
        private int _done;

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _ = OnNextAsync(value);

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

        /// <summary>Starts the action for the value when the sink is idle, and drops the value otherwise.</summary>
        /// <param name = "value">The source value.</param>
        /// <returns>The processing task, or a completed task when no work starts.</returns>
        internal Task OnNextAsync(T value)
        {
            if (Volatile.Read(ref _done) != 0)
            {
                return Task.CompletedTask;
            }

            return Interlocked.CompareExchange(ref _isBusy, 1, 0) != 0
                ? Task.CompletedTask
                : ProcessAsync(value);
        }

        /// <summary>Awaits the action, forwards the value unless the sink has terminated, and clears the busy flag.</summary>
        /// <param name = "value">The value to process.</param>
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
