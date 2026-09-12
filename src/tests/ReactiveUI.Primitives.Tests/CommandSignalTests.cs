// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="CommandSignal{TResult}"/> result, failure, running-state, and disposal contracts.</summary>
public sealed partial class CommandSignalTests
{
    /// <summary>Initial behavior state value used by command tests.</summary>
    private const int InitialStateValue = 10;

    /// <summary>Updated behavior state value used by command tests.</summary>
    private const int UpdatedStateValue = 11;

    /// <summary>Successful command result.</summary>
    private const int CommandResult = 42;

    /// <summary>Number of results the longest-lived result subscriber receives in the fan-out test.</summary>
    private const int ThreeResults = 3;

    /// <summary>Number of results the second-longest-lived result subscriber receives in the fan-out test.</summary>
    private const int TwoResults = 2;

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
            static async token =>
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

    /// <summary>A synchronous command that throws publishes the fault, and a disposed command rejects execution.</summary>
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

    /// <summary>Installing a stale running snapshot reconciles it with the completed execution.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task InstallingAStaleRunningSnapshotReconcilesCompletion()
{
        TaskCompletionSource<int> release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CommandSignal<int> command = new(release.Task.WaitAsync);
        var execution = command.ExecuteAsync();
        StateSignal<bool> candidate = new(true);
        release.SetResult(CommandResult);
        _ = await execution;
        var installed = command.InstallRunningState(candidate);
        await Assert.That(installed).IsSameReferenceAs(candidate);
        await Assert.That(installed.Value).IsFalse();
    }

    /// <summary>The running-state stream is allocated lazily, cached, and reports <see langword="false"/> on an idle command.</summary>
    /// <returns>A task that completes when the lazy-allocation assertions finish.</returns>
    [Test]
    public async Task IsRunningAllocatesLazilyAndCachesTheStream()
    {
        CommandSignal<int> command = new(static () => CommandResult);

        var first = command.IsRunning;
        var second = command.IsRunning;

        await Assert.That(first).IsSameReferenceAs(second);
        await Assert.That(first.Value).IsFalse();
    }

    /// <summary>A stream observed before execution sees the true-then-false transition and ends at <see langword="false"/>.</summary>
    /// <returns>A task that completes when the transition assertions finish.</returns>
    [Test]
    public async Task IsRunningTransitionsTrueThenFalseThroughInstalledStream()
    {
        CommandSignal<int> command = new(static () => CommandResult);
        List<bool> running = [];
        _ = command.IsRunning.Changed.Subscribe(running.Add);

        _ = command.ExecuteAsync();

        await Assert.That(command.IsRunning.Value).IsFalse();
        await Assert.That(running.SequenceEqual(ExpectedRunningValues)).IsTrue();
    }

    /// <summary>A first observation made after the execution completes reports <see langword="false"/>.</summary>
    /// <returns>A task that completes when the deferred-observation assertions finish.</returns>
    [Test]
    public async Task IsRunningReportsFalseWhenObservedOnlyAfterExecution()
    {
        CommandSignal<int> command = new(static () => CommandResult);

        _ = command.ExecuteAsync();

        await Assert.That(command.IsRunning.Value).IsFalse();
    }

    /// <summary>A stream first observed mid-flight reports <see langword="true"/>, then <see langword="false"/> once the execution completes.</summary>
    /// <returns>A task that completes when the mid-flight assertions finish.</returns>
    [Test]
    public async Task IsRunningObservedMidFlightSettlesFalseAfterCompletion()
{
        TaskCompletionSource<int> release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CommandSignal<int> command = new(release.Task.WaitAsync);
        var execution = command.ExecuteAsync();
        var stream = command.IsRunning;
        await Assert.That(stream.Value).IsTrue();
        release.SetResult(CommandResult);
        _ = await execution;
        await Assert.That(stream.Value).IsFalse();
    }

    /// <summary>Results reach each active subscriber as subscriptions are added and removed.</summary>
    /// <returns>A task that completes when the fan-out assertions finish.</returns>
    [Test]
    public async Task ResultsFanOutToEverySubscriberAndStopAtUnsubscribe()
    {
        CommandSignal<int> command = new(static () => CommandResult);
        List<int> first = [];
        List<int> second = [];
        List<int> third = [];

        var firstSubscription = command.Results.Subscribe(first.Add);
        var secondSubscription = command.Results.Subscribe(second.Add);
        var thirdSubscription = command.Results.Subscribe(third.Add);

        _ = command.ExecuteAsync();

        // Removing the middle of a three-observer array leaves both survivors receiving.
        secondSubscription.Dispose();
        _ = command.ExecuteAsync();

        thirdSubscription.Dispose();
        _ = command.ExecuteAsync();

        firstSubscription.Dispose();
        firstSubscription.Dispose();
        _ = command.ExecuteAsync();

        await Assert.That(first.Count).IsEqualTo(ThreeResults);
        await Assert.That(second.Count).IsEqualTo(1);
        await Assert.That(third.Count).IsEqualTo(TwoResults);
        await Assert.That(first.TrueForAll(static value => value == CommandResult)).IsTrue();
    }

    /// <summary>Disposing a result subscription after the command is disposed is a quiet no-op.</summary>
    /// <returns>A task that completes when the post-disposal assertions finish.</returns>
    [Test]
    public async Task ResultSubscriptionDisposedAfterTheCommandIsSafe()
    {
        CommandSignal<int> command = new(static () => CommandResult);
        List<int> results = [];
        var subscription = command.Results.Subscribe(results.Add);

        command.Dispose();
        subscription.Dispose();

        await Assert.That(results.Count).IsEqualTo(0);
        _ = Assert.Throws<ObjectDisposedException>(() => command.Results.Subscribe(results.Add));
    }

    /// <summary>An async command that faults publishes the fault before the await rethrows it, and lowers the running flag.</summary>
    /// <returns>A task that completes when the async-fault assertions finish.</returns>
    [Test]
    public async Task AsyncExecutionPublishesTheFaultAndStillLowersTheRunningFlag()
    {
        InvalidOperationException fault = new("async failed");

        // The delegate type is spelled out: a throw-only body gives the compiler nothing to infer Task<int> from.
        Func<CancellationToken, Task<int>> execute = async token =>
        {
            await Task.Yield();
            token.ThrowIfCancellationRequested();
            throw fault;
        };

        CommandSignal<int> command = new(execute);
        List<Exception> faults = [];
        List<int> results = [];
        _ = command.Faults.Subscribe(faults.Add);
        _ = command.Results.Subscribe(results.Add);

        InvalidOperationException? observed = null;
        try
        {
            _ = await command.ExecuteAsync();
        }
        catch (InvalidOperationException error)
        {
            observed = error;
        }

        await Assert.That(observed!).IsSameReferenceAs(fault);
        await Assert.That(faults.Count).IsEqualTo(1);
        await Assert.That(faults[0]).IsSameReferenceAs(fault);
        await Assert.That(results.Count).IsEqualTo(0);
        await Assert.That(command.IsRunning.Value).IsFalse();
    }

    /// <summary>The fault stream is cached after first use, and disposing the command releases the gate subscription.</summary>
    /// <returns>A task that completes when the lazy-fault-stream assertions finish.</returns>
    [Test]
    public async Task FaultsAllocateLazilyAndDisposalReleasesTheGateSubscription()
    {
        StateSignal<bool> canRun = new(true);
        CommandSignal<int> command = new(static () => CommandResult, canRun);

        var faults = command.Faults;
        var running = command.IsRunning;

        await Assert.That(command.Faults).IsSameReferenceAs(faults);
        await Assert.That(command.CanRun).IsTrue();
        await Assert.That(canRun.HasObservers).IsTrue();

        command.Dispose();

        // The command released the gate, so the gate signal feeds nothing.
        await Assert.That(canRun.HasObservers).IsFalse();
        await Assert.That(running.IsDisposed).IsTrue();
    }

    /// <summary>Competing fault-stream candidates return the installed stream and dispose the unused candidate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompetingFaultStreamCandidatesShareTheInstalledStream()
{
        using CommandSignal<int> command = new(static () => CommandResult);
        Signal<Exception> first = new();
        Signal<Exception> second = new();
        var winner = command.InstallFaultsSignal(first);
        var loser = command.InstallFaultsSignal(second);
        await Assert.That(winner).IsSameReferenceAs(first);
        await Assert.That(loser).IsSameReferenceAs(first);
        await Assert.That(command.Faults).IsSameReferenceAs(first);
        await Assert.That(first.IsDisposed).IsFalse();
        await Assert.That(second.IsDisposed).IsTrue();
    }

    /// <summary>Competing running-stream candidates return the installed stream and dispose the unused candidate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompetingRunningStreamCandidatesShareTheInstalledStream()
{
        using CommandSignal<int> command = new(static () => CommandResult);
        StateSignal<bool> first = new(false);
        StateSignal<bool> second = new(false);
        var winner = command.InstallRunningState(first);
        var loser = command.InstallRunningState(second);
        await Assert.That(winner).IsSameReferenceAs(first);
        await Assert.That(loser).IsSameReferenceAs(first);
        await Assert.That(command.IsRunning).IsSameReferenceAs(first);
        await Assert.That(first.IsDisposed).IsFalse();
        await Assert.That(second.IsDisposed).IsTrue();
    }
}
