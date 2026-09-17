// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies factory completion and cancellation cleanup.</summary>
public partial class TaskSignalTests
{
    /// <summary>Disposal with a cancellation handler.</summary>
    private const int DisposedEnding = 2;

    /// <summary>Cancellation requested through the source token.</summary>
    private const int TokenEnding = 3;

    /// <summary>Disposal without a cancellation handler.</summary>
    private const int UnhandledCancellationEnding = 4;

    /// <summary>Both factory overloads finish their body and subscription cleanup for each terminal path.</summary>
    /// <param name="generic">Whether to call the generic factory overload.</param>
    /// <param name="ending">Zero for success, one for fault, two for disposal, three for token cancellation, or four for unhandled cancellation.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(false, 2)]
    [Arguments(false, 3)]
    [Arguments(false, 4)]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    [Arguments(true, 2)]
    [Arguments(true, 3)]
    [Arguments(true, 4)]
    public async Task FactoryLifecycleCompletesCleanup(bool generic, int ending)
    {
        ConcurrentQueue<string> events = new();
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource cleanup = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<RxVoid>? operation = null;
        var signal = generic
            ? Signal.FromTask<RxVoid>(cts => operation = ExecuteFactoryAsync(events, release, ending, cts.Token))
            : Signal.FromTask(cts => operation = ExecuteFactoryAsync(events, release, ending, cts.Token));
        using var signalLifetime = (IDisposable)signal;
        var fixture = signal.Recover<RxVoid, Exception>(error =>
        {
            events.Enqueue("recovered");
            return Signal.Fail<RxVoid>(error);
        }).OnCleanup(() =>
        {
            events.Enqueue("cleanup");
            _ = cleanup.TrySetResult();
        });
        var results = 0;
        var subscription = fixture.Subscribe(_ => results++, static _ => { });
        await Assert.That(events).Contains("started");
        await Assert.That(operation is not null).IsTrue();
        await CompleteFactoryAsync(ending, signal, subscription, release, operation!);
        await cleanup.Task;
        subscription.Dispose();
        await AssertFactoryEventsAsync(events, ending, results);
    }

    /// <summary>Triggers the selected terminal path and joins the factory body.</summary>
    /// <param name="ending">The terminal path.</param>
    /// <param name="signal">The task signal.</param>
    /// <param name="subscription">The subscription to cancel.</param>
    /// <param name="release">The factory release signal.</param>
    /// <param name="operation">The factory body task.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async Task CompleteFactoryAsync(
        int ending,
        ITaskSignal<RxVoid> signal,
        IDisposable subscription,
        TaskCompletionSource release,
        Task<RxVoid> operation)
    {
        if (ending < DisposedEnding)
        {
            release.SetResult();
        }
        else if (ending == TokenEnding)
        {
            await signal.CancellationTokenSource!.CancelAsync();
        }
        else
        {
            subscription.Dispose();
        }

        if (ending == 1)
        {
            await Assert.That(async () => await operation).Throws<InvalidOperationException>();
        }
        else if (ending == UnhandledCancellationEnding)
        {
            await Assert.That(async () => await operation).Throws<OperationCanceledException>();
        }
        else
        {
            await operation;
        }
    }

    /// <summary>Checks cleanup and result delivery after the factory finishes.</summary>
    /// <param name="events">The recorded events.</param>
    /// <param name="ending">The terminal path.</param>
    /// <param name="results">The result count.</param>
    /// <returns>The asynchronous assertions.</returns>
    private static async Task AssertFactoryEventsAsync(ConcurrentQueue<string> events, int ending, int results)
    {
        await Assert.That(events.Count(static value => value == "cleanup")).IsEqualTo(1);
        await Assert.That(events.Contains("finished")).IsEqualTo(ending < DisposedEnding);
        await Assert.That(events.Contains("cancel-start")).IsEqualTo(ending is DisposedEnding or TokenEnding);
        await Assert.That(events.Contains("cancel-end")).IsEqualTo(ending is DisposedEnding or TokenEnding);
        await Assert.That(results).IsEqualTo(ending == 0 ? 1 : 0);
        await Assert.That(events.Contains("recovered")).IsEqualTo(ending is 1 or TokenEnding);
    }

    /// <summary>Runs until released or canceled, recording body cleanup.</summary>
    /// <param name="events">The event destination.</param>
    /// <param name="release">The factory release signal.</param>
    /// <param name="ending">The terminal path.</param>
    /// <param name="token">The cancellation token.</param>
    /// <returns>The factory result.</returns>
    /// <exception cref="InvalidOperationException">The selected terminal path is a factory failure.</exception>
    private static async Task<RxVoid> ExecuteFactoryAsync(
        ConcurrentQueue<string> events,
        TaskCompletionSource release,
        int ending,
        CancellationToken token)
    {
        events.Enqueue("started");
        var wait = release.Task.WaitAsync(token);
        if (ending == UnhandledCancellationEnding)
        {
            await wait;
        }
        else
        {
            await wait.HandleCancellation(() =>
            {
                events.Enqueue("cancel-start");
                events.Enqueue("cancel-end");
            });
        }

        if (!token.IsCancellationRequested)
        {
            events.Enqueue("finished");
        }

        if (ending == 1)
        {
            throw new InvalidOperationException(BreakExecutionMessage);
        }

        return RxVoid.Default;
    }
}
