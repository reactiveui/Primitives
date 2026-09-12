// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Schedules observer notification drain work on the thread pool.</summary>
internal sealed class ThreadPoolObserverNotificationScheduler : IObserverNotificationScheduler
{
    /// <summary>Stores the default scheduler instance.</summary>
    internal static readonly ThreadPoolObserverNotificationScheduler Instance = new(QueueThreadPoolWorkItem);

    /// <summary>Stores the thread pool callback.</summary>
    private static readonly WaitCallback Callback = static state =>
    {
        ArgumentExceptionHelper.ThrowIfNull(state);
        ((IWorkItem)state).Execute();
    };

    /// <summary>Stores the queueing implementation.</summary>
    private readonly Func<WaitCallback, IWorkItem, bool> _queueWorkItem;

    /// <summary>Initializes a new instance of the <see cref="ThreadPoolObserverNotificationScheduler"/> class.</summary>
    /// <param name="queueWorkItem">The queueing implementation.</param>
    internal ThreadPoolObserverNotificationScheduler(Func<WaitCallback, IWorkItem, bool> queueWorkItem)
    {
        ArgumentExceptionHelper.ThrowIfNull(queueWorkItem);
        _queueWorkItem = queueWorkItem;
    }

    /// <inheritdoc />
    public void Schedule(IWorkItem item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);
        var queued = _queueWorkItem(Callback, item);
        if (queued)
        {
            return;
        }

        throw new InvalidOperationException("The thread pool rejected observer notification work.");
    }

    /// <summary>Queues work to the runtime thread pool.</summary>
    /// <param name="callback">The callback to invoke.</param>
    /// <param name="item">The work item.</param>
    /// <returns><see langword="true"/> when the item was queued.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool QueueThreadPoolWorkItem(WaitCallback callback, IWorkItem item) =>
        ThreadPool.UnsafeQueueUserWorkItem(callback, item);
}
