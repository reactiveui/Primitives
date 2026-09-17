// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="CommandExecution{TResult}"/>, the awaitable a command hands back.</summary>
public class CommandExecutionTests
{
    /// <summary>The value a successful command produces.</summary>
    private const int CommandResult = 42;

    /// <summary>Awaiting a synchronous command hands back its result without ever touching a task.</summary>
    /// <returns>A task that completes when the synchronous-result assertions finish.</returns>
    [Test]
    public async Task AwaitingASynchronousCommandReturnsItsResult()
    {
        using CommandSignal<int> command = new(static () => CommandResult);

        var execution = command.ExecuteAsync();
        var awaiter = execution.GetAwaiter();

        await Assert.That(awaiter.IsCompleted).IsTrue();
        await Assert.That(awaiter.GetResult()).IsEqualTo(CommandResult);
        await Assert.That(await execution).IsEqualTo(CommandResult);
    }

    /// <summary>Awaiting a failed synchronous command rethrows the original exception, not an <see cref="AggregateException"/>.</summary>
    /// <returns>A task that completes when the synchronous-fault assertions finish.</returns>
    [Test]
    public async Task AwaitingAFailedSynchronousCommandRethrowsTheOriginalException()
    {
        InvalidOperationException fault = new("sync failed");
        using CommandSignal<int> command = new(() => throw fault);

        var execution = command.ExecuteAsync();

        await Assert.That(execution.GetAwaiter().IsCompleted).IsTrue();
        var thrown = Assert.Throws<InvalidOperationException>(() => execution.GetAwaiter().GetResult());
        await Assert.That(thrown!).IsSameReferenceAs(fault);
    }

    /// <summary><c>ConfigureAwait</c> preserves the outcome of a task, a bare result, and a bare exception alike.</summary>
    /// <returns>A task that completes when the configure-await assertions finish.</returns>
    [Test]
    public async Task ConfigureAwaitPreservesTheOutcomeOfEveryExecutionShape()
    {
        using CommandSignal<int> synchronous = new(static () => CommandResult);
        await Assert.That(await synchronous.ExecuteAsync().ConfigureAwait(false)).IsEqualTo(CommandResult);

        using CommandSignal<int> asynchronous = new(static async token =>
        {
            await Task.Yield();
            token.ThrowIfCancellationRequested();
            return CommandResult;
        });
        await Assert.That(await asynchronous.ExecuteAsync().ConfigureAwait(false)).IsEqualTo(CommandResult);

        InvalidOperationException fault = new("configured failure");
        using CommandSignal<int> failing = new(() => throw fault);
        var configured = failing.ExecuteAsync().ConfigureAwait(false);
        var thrown = Assert.Throws<InvalidOperationException>(() => configured.GetAwaiter().GetResult());
        await Assert.That(thrown!).IsSameReferenceAs(fault);
    }

    /// <summary>A continuation scheduled through <see cref="System.Runtime.CompilerServices.INotifyCompletion"/> is resumed and sees the result.</summary>
    /// <returns>A task that completes when the continuation assertions finish.</returns>
    [Test]
    public async Task TheAwaiterResumesAContinuationScheduledThroughOnCompleted()
{
        TaskCompletionSource<int> release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CommandSignal<int> command = new(release.Task.WaitAsync);
        var awaiter = command.ExecuteAsync().GetAwaiter();
        TaskCompletionSource resumed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        awaiter.OnCompleted(() => resumed.SetResult());
        await Assert.That(awaiter.IsCompleted).IsFalse();
        release.SetResult(CommandResult);
        await resumed.Task;
        await Assert.That(awaiter.GetResult()).IsEqualTo(CommandResult);
    }

    /// <summary>An execution cannot be built from a missing task or a missing exception.</summary>
    /// <returns>A task that completes when the argument-validation assertions finish.</returns>
    [Test]
    public async Task ExecutionRejectsAMissingTaskOrException()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            CommandExecution<int> invalid = new((Task<int>)null!);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            CommandExecution<int> invalid = new((Exception)null!);
            GC.KeepAlive(invalid);
        });

        CommandExecution<int> valid = new(CommandResult);
        await Assert.That(await valid).IsEqualTo(CommandResult);
    }
}
