// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="CommandSignal{TResult}"/> result, failure, running-state, and disposal contracts.</summary>
public sealed class CommandSignalTests
{
    /// <summary>Initial behavior state value used by command tests.</summary>
    private const int InitialStateValue = 10;

    /// <summary>Updated behavior state value used by command tests.</summary>
    private const int UpdatedStateValue = 11;

    /// <summary>Successful command result.</summary>
    private const int CommandResult = 42;

    /// <summary>Expected command results.</summary>
    private static readonly int[] ExpectedCommandResults = [CommandResult];

    /// <summary>Expected command running-state notifications.</summary>
    private static readonly bool[] ExpectedRunningValues = [false, true, false];

    /// <summary>Verifies command signals publish results, failures, and running state.</summary>
    /// <returns>A task that completes when the command assertions finish.</returns>
    [Test]
    public async Task CommandSignalPublishesResultsFailuresAndRunningState()
    {
        StateSignal<bool> canRun = new(true);
        CommandSignal<int> command = new(
            async token =>
            {
                await Task.Yield();
                token.ThrowIfCancellationRequested();
                return CommandResult;
            },
            canRun);
        List<int> results = [];
        List<bool> running = [];
        _ = command.Results.Subscribe(results.Add);
        _ = command.IsRunning.Changed.Subscribe(running.Add);
        var executed = await command.ExecuteAsync();
        canRun.Value = false;
        InvalidOperationException? rejected = null;
        try
        {
            await command.ExecuteAsync();
        }
        catch (InvalidOperationException error)
        {
            rejected = error;
        }

        await Assert.That(rejected).IsNotNull();
        await Assert.That(executed).IsEqualTo(CommandResult);
        await Assert.That(results.SequenceEqual(ExpectedCommandResults)).IsTrue();
        await Assert.That(running.SequenceEqual(ExpectedRunningValues)).IsTrue();
        await Assert.That(rejected!.Message).IsEqualTo("Command cannot run.");
    }

    /// <summary>Verifies command aliases, sync execution failures, and disposal branches.</summary>
    /// <returns>A task that completes when command assertions finish.</returns>
    [Test]
    public async Task CommandSignalCoversSyncFaultAndDisposalBranches()
    {
        BehaviorSignal<int> behavior = new(InitialStateValue);
        MultipleDisposable disposable = new(EmptyDisposable.Instance);
        InvalidOperationException fault = new("sync failed");
        CommandSignal<int> command = new(() => throw fault);
        List<int> results = [];
        List<Exception> faults = [];
        _ = command.Results.Subscribe(results.Add);
        _ = command.Faults.Subscribe(faults.Add);
        behavior.OnNext(UpdatedStateValue);
        disposable.Dispose();
        InvalidOperationException? observed = null;
        try
        {
            await command.ExecuteAsync();
        }
        catch (InvalidOperationException error)
        {
            observed = error;
        }

        command.Dispose();
        command.Dispose();
        ObjectDisposedException? disposed = null;
        try
        {
            await command.ExecuteAsync();
        }
        catch (ObjectDisposedException error)
        {
            disposed = error;
        }

        await Assert.That(observed!).IsSameReferenceAs(fault);
        await Assert.That(results.Count).IsEqualTo(0);
        await Assert.That(faults.Count).IsEqualTo(1);
        await Assert.That(faults[0]).IsSameReferenceAs(fault);
        await Assert.That(behavior.Value).IsEqualTo(UpdatedStateValue);
        await Assert.That(disposable.IsDisposed).IsTrue();
        await Assert.That(disposed).IsNotNull();
    }
}
