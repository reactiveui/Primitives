// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Queues timer callbacks until the test explicitly advances them.</summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    /// <summary>Pending timer registrations in creation order.</summary>
    private readonly Channel<ManualTimer> _timers = Channel.CreateUnbounded<ManualTimer>();

    /// <summary>Gets the number of registrations not yet consumed by the test.</summary>
    internal int PendingTimerCount => _timers.Reader.Count;

    /// <inheritdoc/>
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ManualTimer timer = new(callback, state, dueTime);
        _ = _timers.Writer.TryWrite(timer);
        return timer;
    }

    /// <summary>Waits for the next timer registration without advancing it.</summary>
    /// <returns>The registered timer.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<ManualTimer> NextTimerAsync() => _timers.Reader.ReadAsync();

    /// <summary>Fires the next registered timer.</summary>
    /// <returns>A task representing the callback invocation.</returns>
    internal async Task FireNextAsync()
    {
        var timer = await NextTimerAsync();
        timer.Fire();
    }

    /// <summary>Drives queued timers until the supplied operation completes.</summary>
    /// <param name="operation">The operation whose timers should run.</param>
    /// <returns>A task representing the operation.</returns>
    internal async Task RunAsync(Task operation)
    {
        while (!operation.IsCompleted)
        {
            var ready = _timers.Reader.WaitToReadAsync().AsTask();
            if (await Task.WhenAny(operation, ready) == operation)
            {
                break;
            }

            if (_timers.Reader.TryRead(out var timer))
            {
                timer.Fire();
            }
        }

        await operation;
    }

    /// <summary>Drives queued timers until the supplied result is available.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="operation">The operation whose timers should run.</param>
    /// <returns>The completed result.</returns>
    internal async Task<T> RunAsync<T>(Task<T> operation)
    {
        await RunAsync((Task)operation);
        return await operation;
    }

    /// <summary>A timer whose callback is invoked explicitly by its owner.</summary>
    /// <param name="callback">The timer callback.</param>
    /// <param name="state">The callback state.</param>
    /// <param name="dueTime">The initial due time.</param>
    internal sealed class ManualTimer(TimerCallback callback, object? state, TimeSpan dueTime) : ITimer
    {
        /// <summary>Whether the timer has been disposed.</summary>
        private bool _disposed;

        /// <summary>Gets the most recently requested due time.</summary>
        internal TimeSpan DueTime { get; private set; } = dueTime;

        /// <inheritdoc/>
        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            DueTime = dueTime;
            return !_disposed;
        }

        /// <inheritdoc/>
        public void Dispose() => _disposed = true;

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            Dispose();
            return default;
        }

        /// <summary>Fires an armed callback once; a changed timer can be fired again.</summary>
        internal void Fire()
        {
            if (_disposed || DueTime == Timeout.InfiniteTimeSpan)
            {
                return;
            }

            DueTime = Timeout.InfiniteTimeSpan;
            callback(state);
        }
    }
}
