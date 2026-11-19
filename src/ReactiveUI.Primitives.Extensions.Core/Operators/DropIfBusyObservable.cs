// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Runs the action before forwarding each value, dropping values received while the action is running.</summary>
/// <typeparam name = "T">The element type.</typeparam>
/// <param name = "source">The source observable.</param>
/// <param name = "asyncAction">The asynchronous action to execute for each forwarded element.</param>
/// <remarks>An action failure terminates the sequence.</remarks>
public sealed class DropIfBusyObservable<T>(IObservable<T> source, Func<T, ValueTask> asyncAction) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(asyncAction);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        DropIfBusySink sink = new(observer, asyncAction);
        sink.Attach(source.Subscribe(sink));
        return sink;
    }

    /// <summary>Processes source values and owns the subscription state.</summary>
    /// <param name = "downstream">The downstream observer.</param>
    /// <param name = "asyncAction">The asynchronous operation.</param>
    internal sealed class DropIfBusySink(IObserver<T> downstream, Func<T, ValueTask> asyncAction) : IObserver<T>, IDisposable
    {
        /// <summary>Upstream subscription handle.</summary>
        private IDisposable? _subscription;

        /// <summary>0 = idle, 1 = busy.</summary>
        private int _isBusy;

        /// <summary>Non-zero once the sink is terminal.</summary>
        private int _done;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _ = OnNextAsync(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _done, 1) != 0)
            {
                return;
            }

            SinkSubscription.Dispose(ref _subscription);
            downstream.OnError(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _done, 1) != 0)
            {
                return;
            }

            SinkSubscription.Dispose(ref _subscription);
            downstream.OnCompleted();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Volatile.Write(ref _done, 1);
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
            catch (Exception ex) when (Volatile.Read(ref _done) != 0 || ForwardFault(ex))
            {
                // Fault handled or suppressed after disposal/completion.
            }
            finally
            {
                Volatile.Write(ref _isBusy, 0);
            }
        }

        /// <summary>Latches termination, disposes upstream, and forwards a handler fault downstream.</summary>
        /// <param name="error">The handler exception.</param>
        /// <returns>Always <see langword="true"/> once handled.</returns>
        private bool ForwardFault(Exception error)
        {
            if (Interlocked.Exchange(ref _done, 1) == 0)
            {
                SinkSubscription.Dispose(ref _subscription);
                downstream.OnError(error);
            }

            return true;
        }
    }
}
