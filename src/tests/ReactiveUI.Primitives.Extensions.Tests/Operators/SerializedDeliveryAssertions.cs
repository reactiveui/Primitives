// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>
/// Asserts that an operator holds no lock while its observer runs, so an observer that marshals synchronously to another
/// thread cannot deadlock the thread that produced the notification.
/// </summary>
internal static class SerializedDeliveryAssertions
{
    /// <summary>
    /// Pushes values from a worker thread through the operator; the observer's first notification during the push marshals
    /// synchronously to a dispatcher thread that completes the source. The push must finish promptly, and completion must be
    /// delivered after that notification.
    /// </summary>
    /// <typeparam name="TResult">The operator's element type.</typeparam>
    /// <param name="subscribe">Subscribes the operator under test over the source.</param>
    /// <param name="push">Pushes the values that make the operator notify its observer.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task ObserverMarshallingCompletionDoesNotDeadlock<TResult>(
        Func<IObservable<int>, IObserver<TResult>, IDisposable> subscribe,
        Action<IObserver<int>> push)
    {
        using MarshallingThread dispatcher = new();
        SyncDirectSource<int> source = new();
        var pushing = 0;
        var marshalled = 0;
        var notifications = 0;
        var notificationsAtCompletion = -1;
        var observer = Observer.Create<TResult>(
            _ =>
            {
                notifications++;
                if (Volatile.Read(ref pushing) == 0 || Interlocked.Exchange(ref marshalled, 1) != 0)
                {
                    return;
                }

                dispatcher.Invoke(() => source.Observer.OnCompleted());
            },
            static _ => { },
            () => notificationsAtCompletion = notifications);

        using var subscription = subscribe(source, observer);
        var worker = BackgroundThread.Start(() =>
        {
            Volatile.Write(ref pushing, 1);
            push(source.Observer);
        });

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        await Assert.That(marshalled).IsEqualTo(1);
        await Assert.That(notificationsAtCompletion).IsGreaterThanOrEqualTo(1);
    }
}
