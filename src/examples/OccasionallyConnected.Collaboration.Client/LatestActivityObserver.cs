// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Tracks the latest activity view and lets callers wait for one matching view.</summary>
internal sealed class LatestActivityObserver : IObserver<ActivityView>
{
    /// <summary>Protects observer state.</summary>
    private readonly object _gate = new();

    /// <summary>Stores the single publish confirmation waiter.</summary>
    private TaskCompletionSource<ActivityView>? _waiter;

    /// <summary>Stores the predicate for the active waiter.</summary>
    private Func<ActivityView, bool>? _predicate;

    /// <summary>Stores the latest view.</summary>
    private ActivityView? _latest;

    /// <summary>Stores the terminal observer error.</summary>
    private Exception? _terminalError;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => OnError(new InvalidOperationException("Activity observation completed before the publish was confirmed."));

    /// <inheritdoc />
    public void OnError(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        TaskCompletionSource<ActivityView>? waiter;
        lock (_gate)
        {
            if (_terminalError is not null)
            {
                return;
            }

            _terminalError = error;
            waiter = ClearWaiter();
        }

        waiter?.TrySetException(error);
    }

    /// <inheritdoc />
    public void OnNext(ActivityView value)
    {
        TaskCompletionSource<ActivityView>? matched = null;
        lock (_gate)
        {
            if (_terminalError is not null)
            {
                return;
            }

            _latest = value;
            if (_predicate is { } predicate && predicate(value))
            {
                matched = ClearWaiter();
            }
        }

        matched?.TrySetResult(value);
    }

    /// <summary>Waits for a matching activity view.</summary>
    /// <param name="predicate">The predicate.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The matching view.</returns>
    /// <exception cref="InvalidOperationException">A wait is already pending or the observer has ended.</exception>
    internal async Task<ActivityView> WaitForAsync(Func<ActivityView, bool> predicate, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        TaskCompletionSource<ActivityView> waiter;
        lock (_gate)
        {
            ThrowIfTerminal();
            if (_latest is { } latest && predicate(latest))
            {
                return latest;
            }

            if (_waiter is not null)
            {
                throw new InvalidOperationException("Only one pending activity confirmation wait is supported.");
            }

            _predicate = predicate;
            waiter = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiter = waiter;
        }

        await using var registration = cancellationToken.UnsafeRegister(_ => CancelWaiter(waiter, cancellationToken), null);
        return await waiter.Task.ConfigureAwait(false);
    }

    /// <summary>Clears a waiter if it still owns the active wait slot.</summary>
    /// <param name="waiter">The waiter requesting cancellation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private void CancelWaiter(TaskCompletionSource<ActivityView> waiter, CancellationToken cancellationToken)
    {
        var canceled = false;
        lock (_gate)
        {
            if (ReferenceEquals(_waiter, waiter))
            {
                _ = ClearWaiter();
                canceled = true;
            }
        }

        if (canceled)
        {
            _ = waiter.TrySetCanceled(cancellationToken);
        }
    }

    /// <summary>Clears the pending waiter state.</summary>
    /// <returns>The pending waiter.</returns>
    private TaskCompletionSource<ActivityView>? ClearWaiter()
    {
        var waiter = _waiter;
        _waiter = null;
        _predicate = null;
        return waiter;
    }

    /// <summary>Throws the stored terminal error when the observer has ended.</summary>
    private void ThrowIfTerminal()
    {
        if (_terminalError is null)
        {
            return;
        }

        ExceptionDispatchInfo.Capture(_terminalError).Throw();
    }
}
