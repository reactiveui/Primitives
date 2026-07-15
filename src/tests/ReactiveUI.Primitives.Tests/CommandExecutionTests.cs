// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies <see cref="CommandExecution{TResult}"/>, the awaitable a command hands back. It exists so a command
/// that finishes synchronously does not have to allocate a task, which means it has three shapes to honour —
/// a task, a bare result, and a bare exception — behind one awaiter.
/// </summary>
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

    /// <summary>
    /// A synchronous command that fails is carried as a bare exception, and awaiting it must rethrow that exact
    /// exception rather than an <see cref="AggregateException"/> wrapper.
    /// </summary>
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

    /// <summary>
    /// <c>ConfigureAwait</c> hands back a fresh awaitable that carries the same outcome. It must not lose the
    /// result on the way through, whichever of the three shapes the execution is carrying.
    /// </summary>
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

    /// <summary>
    /// The awaiter implements the plain <see cref="System.Runtime.CompilerServices.INotifyCompletion"/>
    /// continuation path as well as the critical one the C# compiler prefers. A caller that schedules through it
    /// must still be resumed, and must still see the result.
    /// </summary>
    /// <returns>A task that completes when the continuation assertions finish.</returns>
    [Test]
    public async Task TheAwaiterResumesAContinuationScheduledThroughOnCompleted()
    {
        using ManualResetEventSlim release = new(false);
        using CommandSignal<int> command = new(async token =>
        {
            await Task.Run(() => release.Wait(token), token);
            return CommandResult;
        });

        var awaiter = command.ExecuteAsync().GetAwaiter();
        TaskCompletionSource resumed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        awaiter.OnCompleted(() => resumed.SetResult());

        await Assert.That(awaiter.IsCompleted).IsFalse();

        release.Set();
        await resumed.Task;

        await Assert.That(awaiter.IsCompleted).IsTrue();
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
