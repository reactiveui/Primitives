// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// SignalFromTaskTest.
/// </summary>
public class SignalFromTaskTest
{
    /// <summary>
    /// Delay used before checking that a task has started.
    /// </summary>
    private const int InitialDelayMilliseconds = 500;

    /// <summary>
    /// Delay before token cancellation is requested.
    /// </summary>
    private const int TokenCancellationDelayMilliseconds = 1000;

    /// <summary>
    /// Delay used to simulate cancellation cleanup.
    /// </summary>
    private const int CleanupDelayMilliseconds = 5000;

    /// <summary>
    /// Delay used to wait for cancellation cleanup to finish.
    /// </summary>
    private const int CancellationWaitDelayMilliseconds = 6000;

    /// <summary>
    /// Delay used to wait for token cancellation cleanup to finish.
    /// </summary>
    private const int TokenCancellationWaitDelayMilliseconds = 8000;

    /// <summary>
    /// Delay used by the command body.
    /// </summary>
    private const int CommandDelayMilliseconds = 10000;

    /// <summary>
    /// Delay used to wait for normal command completion.
    /// </summary>
    private const int CompletionWaitDelayMilliseconds = 11000;

    /// <summary>
    /// Exception message used by user exception tests.
    /// </summary>
    private const string BreakExecutionMessage = "break execution";

    /// <summary>
    /// Status text recorded when a command starts.
    /// </summary>
    private const string StartedCommand = "started command";

    /// <summary>
    /// Status text recorded when cancellation cleanup starts.
    /// </summary>
    private const string StartingCancellingCommand = "starting cancelling command";

    /// <summary>
    /// Status text recorded when cancellation cleanup finishes.
    /// </summary>
    private const string FinishedCancellingCommand = "finished cancelling command";

    /// <summary>
    /// Status text recorded when the command completes normally.
    /// </summary>
    private const string FinishedCommandNormally = "finished command Normally";

    /// <summary>
    /// Status text recorded by the exception handler.
    /// </summary>
    private const string ExceptionShouldBeHere = "Exception Should Be here";

    /// <summary>
    /// Status text recorded by the finalizer callback.
    /// </summary>
    private const string ShouldAlwaysComeHere = "Should always come here.";

    /// <summary>
    /// Signals from task handles user exceptions.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTaskHandlesUserExceptions()
    {
        var statusTrail = new List<(int, string)>();
        var position = 0;
        var fixture = Signal.FromTask(
             async cts =>
             {
                 statusTrail.Add((position++, StartedCommand));
                 await Task.Delay(CommandDelayMilliseconds, cts.Token)
                     .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position))
                     .ConfigureAwait(true);

                 if (!cts.IsCancellationRequested)
                 {
                     statusTrail.Add((position++, FinishedCommandNormally));
                 }

                 throw new InvalidOperationException(BreakExecutionMessage);
             }).Catch<RxVoid, Exception>(
            ex =>
            {
                statusTrail.Add((position++, ExceptionShouldBeHere));
                return Signal.Throw<RxVoid>(ex);
            }).Finally(() => statusTrail.Add((position++, ShouldAlwaysComeHere)));

        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);

        Assert.Contains(StartedCommand, StatusMessages(statusTrail));

        await Task.Delay(CommandDelayMilliseconds).ConfigureAwait(true);
        subscription.Dispose();

        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);

        Assert.DoesNotContain(StartingCancellingCommand, StatusMessages(statusTrail));
        Assert.Contains(ShouldAlwaysComeHere, StatusMessages(statusTrail));
        Assert.DoesNotContain(FinishedCancellingCommand, StatusMessages(statusTrail));
        Assert.Contains(ExceptionShouldBeHere, StatusMessages(statusTrail));
        Assert.Contains(FinishedCommandNormally, StatusMessages(statusTrail));
        Assert.False(result);
    }

    /// <summary>
    /// Signals from task handles cancellation.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTaskHandlesCancellation()
    {
        var statusTrail = new List<(int, string)>();
        var position = 0;
        var fixture = Signal.FromTask(
             async cts =>
             {
                 statusTrail.Add((position++, StartedCommand));
                 await Task.Delay(CommandDelayMilliseconds, cts.Token)
                     .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position))
                     .ConfigureAwait(true);

                 if (!cts.IsCancellationRequested)
                 {
                     statusTrail.Add((position++, FinishedCommandNormally));
                 }

                 return RxVoid.Default;
             }).Catch<RxVoid, Exception>(
            ex =>
            {
                statusTrail.Add((position++, ExceptionShouldBeHere));
                return Signal.Throw<RxVoid>(ex);
            }).Finally(() => statusTrail.Add((position++, ShouldAlwaysComeHere)));

        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);

        Assert.Contains(StartedCommand, StatusMessages(statusTrail));
        subscription.Dispose();

        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);

        Assert.Contains(StartingCancellingCommand, StatusMessages(statusTrail));
        Assert.Contains(ShouldAlwaysComeHere, StatusMessages(statusTrail));
        Assert.Contains(FinishedCancellingCommand, StatusMessages(statusTrail));
        Assert.DoesNotContain(FinishedCommandNormally, StatusMessages(statusTrail));
        Assert.False(result);
    }

    /// <summary>
    /// Signals from task handles token cancellation.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTaskHandlesTokenCancellation()
    {
        var statusTrail = new List<(int, string)>();
        var position = 0;
        var fixture = Signal.FromTask(
             async cts =>
             {
                 statusTrail.Add((position++, StartedCommand));
                 await Task.Delay(TokenCancellationDelayMilliseconds, cts.Token).HandleCancellation().ConfigureAwait(true);

                 var cancellationTask = CancelAfterDelayAsync(cts);
                 await Task.Delay(CleanupDelayMilliseconds, cts.Token)
                     .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position))
                     .ConfigureAwait(true);
                 await cancellationTask.ConfigureAwait(false);

                 if (!cts.IsCancellationRequested)
                 {
                     statusTrail.Add((position++, FinishedCommandNormally));
                 }

                 return RxVoid.Default;
             }).Catch<RxVoid, Exception>(
            ex =>
            {
                statusTrail.Add((position++, ExceptionShouldBeHere));
                return Signal.Throw<RxVoid>(ex);
            }).Finally(() => statusTrail.Add((position++, ShouldAlwaysComeHere)));

        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);

        Assert.Contains(StartedCommand, StatusMessages(statusTrail));

        await Task.Delay(TokenCancellationWaitDelayMilliseconds).ConfigureAwait(false);

        Assert.Contains(StartingCancellingCommand, StatusMessages(statusTrail));
        Assert.Contains(ShouldAlwaysComeHere, StatusMessages(statusTrail));
        Assert.Contains(FinishedCancellingCommand, StatusMessages(statusTrail));
        Assert.DoesNotContain(FinishedCommandNormally, StatusMessages(statusTrail));
        Assert.False(result);
    }

    /// <summary>
    /// Signals from task handles cancellation in base.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTaskHandlesCancellationInBase()
    {
        var statusTrail = new List<(int, string)>();
        var position = 0;
        var fixture = Signal.FromTask(
             async cts =>
             {
                 statusTrail.Add((position++, StartedCommand));
                 await Task.Delay(CommandDelayMilliseconds, cts.Token).ConfigureAwait(true);
                 if (!cts.IsCancellationRequested)
                 {
                     statusTrail.Add((position++, FinishedCommandNormally));
                 }

                 return RxVoid.Default;
             }).Catch<RxVoid, Exception>(
            ex =>
            {
                statusTrail.Add((position++, ExceptionShouldBeHere));
                return Signal.Throw<RxVoid>(ex);
            }).Finally(() => statusTrail.Add((position++, ShouldAlwaysComeHere)));

        using var subscription = fixture.Subscribe();
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        Assert.Contains(StartedCommand, StatusMessages(statusTrail));
        subscription.Dispose();

        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);

        Assert.DoesNotContain(FinishedCommandNormally, StatusMessages(statusTrail));
        Assert.Equal(ShouldAlwaysComeHere, statusTrail[^1].Item2);
    }

    /// <summary>
    /// Signals from task handles completion.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTaskHandlesCompletion()
    {
        var statusTrail = new List<(int, string)>();
        var position = 0;
        var fixture = Signal.FromTask(
             async cts =>
             {
                 statusTrail.Add((position++, StartedCommand));
                 await Task.Delay(CommandDelayMilliseconds, cts.Token)
                     .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position))
                     .ConfigureAwait(true);

                 if (!cts.IsCancellationRequested)
                 {
                     statusTrail.Add((position++, FinishedCommandNormally));
                 }

                 return RxVoid.Default;
             }).Catch<RxVoid, Exception>(
            ex =>
            {
                statusTrail.Add((position++, ExceptionShouldBeHere));
                return Signal.Throw<RxVoid>(ex);
            }).Finally(() => statusTrail.Add((position++, ShouldAlwaysComeHere)));

        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);

        Assert.Contains(StartedCommand, StatusMessages(statusTrail));

        await Task.Delay(CompletionWaitDelayMilliseconds).ConfigureAwait(false);

        Assert.DoesNotContain(StartingCancellingCommand, StatusMessages(statusTrail));
        Assert.DoesNotContain(FinishedCancellingCommand, StatusMessages(statusTrail));
        Assert.Contains(FinishedCommandNormally, StatusMessages(statusTrail));
        Assert.Equal(ShouldAlwaysComeHere, statusTrail[^1].Item2);
        Assert.True(result);
    }

    /// <summary>
    /// Signals from task t handles user exceptions.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTask_T_HandlesUserExceptions()
    {
        var statusTrail = new List<(int, string)>();
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(
             async cts =>
             {
                 statusTrail.Add((position++, StartedCommand));
                 await Task.Delay(CommandDelayMilliseconds, cts.Token)
                     .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position))
                     .ConfigureAwait(true);

                 if (!cts.IsCancellationRequested)
                 {
                     statusTrail.Add((position++, FinishedCommandNormally));
                 }

                 throw new InvalidOperationException(BreakExecutionMessage);
             }).Catch<RxVoid, Exception>(
            ex =>
            {
                statusTrail.Add((position++, ExceptionShouldBeHere));
                return Signal.Throw<RxVoid>(ex);
            }).Finally(() => statusTrail.Add((position++, ShouldAlwaysComeHere)));

        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);

        Assert.Contains(StartedCommand, StatusMessages(statusTrail));

        await Task.Delay(CommandDelayMilliseconds).ConfigureAwait(true);
        subscription.Dispose();

        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);

        Assert.DoesNotContain(StartingCancellingCommand, StatusMessages(statusTrail));
        Assert.Contains(ShouldAlwaysComeHere, StatusMessages(statusTrail));
        Assert.DoesNotContain(FinishedCancellingCommand, StatusMessages(statusTrail));
        Assert.Contains(ExceptionShouldBeHere, StatusMessages(statusTrail));
        Assert.Contains(FinishedCommandNormally, StatusMessages(statusTrail));
        Assert.False(result);
    }

    /// <summary>
    /// Signals from task t handles cancellation.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTask_T_HandlesCancellation()
    {
        var statusTrail = new List<(int, string)>();
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(
             async cts =>
             {
                 statusTrail.Add((position++, StartedCommand));
                 await Task.Delay(CommandDelayMilliseconds, cts.Token)
                     .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position))
                     .ConfigureAwait(true);

                 if (!cts.IsCancellationRequested)
                 {
                     statusTrail.Add((position++, FinishedCommandNormally));
                 }

                 return RxVoid.Default;
             }).Catch<RxVoid, Exception>(
            ex =>
            {
                statusTrail.Add((position++, ExceptionShouldBeHere));
                return Signal.Throw<RxVoid>(ex);
            }).Finally(() => statusTrail.Add((position++, ShouldAlwaysComeHere)));

        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);

        Assert.Contains(StartedCommand, StatusMessages(statusTrail));
        subscription.Dispose();

        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);

        Assert.Contains(StartingCancellingCommand, StatusMessages(statusTrail));
        Assert.Contains(ShouldAlwaysComeHere, StatusMessages(statusTrail));
        Assert.Contains(FinishedCancellingCommand, StatusMessages(statusTrail));
        Assert.DoesNotContain(FinishedCommandNormally, StatusMessages(statusTrail));
        Assert.False(result);
    }

    /// <summary>
    /// Signals from task t handles token cancellation.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTask_T_HandlesTokenCancellation()
    {
        var statusTrail = new List<(int, string)>();
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(
             async cts =>
             {
                 statusTrail.Add((position++, StartedCommand));
                 await Task.Delay(TokenCancellationDelayMilliseconds, cts.Token).HandleCancellation().ConfigureAwait(true);

                 var cancellationTask = CancelAfterDelayAsync(cts);
                 await Task.Delay(CleanupDelayMilliseconds, cts.Token)
                     .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position))
                     .ConfigureAwait(true);
                 await cancellationTask.ConfigureAwait(false);

                 if (!cts.IsCancellationRequested)
                 {
                     statusTrail.Add((position++, FinishedCommandNormally));
                 }

                 return RxVoid.Default;
             }).Catch<RxVoid, Exception>(
            ex =>
            {
                statusTrail.Add((position++, ExceptionShouldBeHere));
                return Signal.Throw<RxVoid>(ex);
            }).Finally(() => statusTrail.Add((position++, ShouldAlwaysComeHere)));

        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);

        Assert.Contains(StartedCommand, StatusMessages(statusTrail));

        await Task.Delay(TokenCancellationWaitDelayMilliseconds).ConfigureAwait(false);

        Assert.Contains(StartingCancellingCommand, StatusMessages(statusTrail));
        Assert.Contains(ShouldAlwaysComeHere, StatusMessages(statusTrail));
        Assert.Contains(FinishedCancellingCommand, StatusMessages(statusTrail));
        Assert.DoesNotContain(FinishedCommandNormally, StatusMessages(statusTrail));
        Assert.False(result);
    }

    /// <summary>
    /// Signals from task t handles cancellation in base.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTask_T_HandlesCancellationInBase()
    {
        var statusTrail = new List<(int, string)>();
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(
             async cts =>
             {
                 statusTrail.Add((position++, StartedCommand));
                 await Task.Delay(CommandDelayMilliseconds, cts.Token).ConfigureAwait(true);
                 if (!cts.IsCancellationRequested)
                 {
                     statusTrail.Add((position++, FinishedCommandNormally));
                 }

                 return RxVoid.Default;
             }).Catch<RxVoid, Exception>(
            ex =>
            {
                statusTrail.Add((position++, ExceptionShouldBeHere));
                return Signal.Throw<RxVoid>(ex);
            }).Finally(() => statusTrail.Add((position++, ShouldAlwaysComeHere)));

        using var subscription = fixture.Subscribe();
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        Assert.Contains(StartedCommand, StatusMessages(statusTrail));
        subscription.Dispose();

        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);

        Assert.DoesNotContain(FinishedCommandNormally, StatusMessages(statusTrail));
        Assert.Equal(ShouldAlwaysComeHere, statusTrail[^1].Item2);
    }

    /// <summary>
    /// Signals from task t handles completion.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTask_T_HandlesCompletion()
    {
        var statusTrail = new List<(int, string)>();
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(
             async cts =>
             {
                 statusTrail.Add((position++, StartedCommand));
                 await Task.Delay(CommandDelayMilliseconds, cts.Token)
                     .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position))
                     .ConfigureAwait(true);

                 if (!cts.IsCancellationRequested)
                 {
                     statusTrail.Add((position++, FinishedCommandNormally));
                 }

                 return RxVoid.Default;
             }).Catch<RxVoid, Exception>(
            ex =>
            {
                statusTrail.Add((position++, ExceptionShouldBeHere));
                return Signal.Throw<RxVoid>(ex);
            }).Finally(() => statusTrail.Add((position++, ShouldAlwaysComeHere)));

        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);

        Assert.Contains(StartedCommand, StatusMessages(statusTrail));

        await Task.Delay(CompletionWaitDelayMilliseconds).ConfigureAwait(false);

        Assert.DoesNotContain(StartingCancellingCommand, StatusMessages(statusTrail));
        Assert.DoesNotContain(FinishedCancellingCommand, StatusMessages(statusTrail));
        Assert.Contains(FinishedCommandNormally, StatusMessages(statusTrail));
        Assert.Equal(ShouldAlwaysComeHere, statusTrail[^1].Item2);
        Assert.True(result);
    }

    /// <summary>
    /// Gets the recorded status messages.
    /// </summary>
    /// <param name="statusTrail">The status trail.</param>
    /// <returns>The recorded messages.</returns>
    private static IEnumerable<string> StatusMessages(IEnumerable<(int Position, string Message)> statusTrail) =>
        statusTrail.Select(static x => x.Message);

    /// <summary>
    /// Records synchronous cancellation cleanup.
    /// </summary>
    /// <param name="statusTrail">The status trail.</param>
    /// <param name="position">The current status position.</param>
    private static void RecordCancellationCleanup(List<(int Position, string Message)> statusTrail, ref int position)
    {
        statusTrail.Add((position++, StartingCancellingCommand));
        Thread.Sleep(CleanupDelayMilliseconds);
        statusTrail.Add((position++, FinishedCancellingCommand));
    }

    /// <summary>
    /// Cancels the source after the token cancellation delay.
    /// </summary>
    /// <param name="cts">The cancellation source.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    private static async Task CancelAfterDelayAsync(CancellationTokenSource cts)
    {
        await Task.Delay(TokenCancellationDelayMilliseconds).ConfigureAwait(false);
        await cts.CancelAsync().ConfigureAwait(false);
    }
}
