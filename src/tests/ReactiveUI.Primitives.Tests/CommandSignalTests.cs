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

    /// <summary>
    /// Reproduces the lazy-init race for <see cref="CommandSignal{TResult}.IsRunning"/>: the state
    /// stream is requested for the first time while an execution is finishing. If the getter
    /// snapshots a <see langword="true"/> flag and installs the stream after the matching
    /// completion lowered it, the stream must still settle at <see langword="false"/> rather than
    /// latching permanently true with no in-flight execution to correct it.
    /// </summary>
    /// <returns>A task that completes when every interleaving has settled at false.</returns>
    [Test]
    public async Task IsRunningNeverLatchesTrueWhenFirstObservedDuringCompletion()
    {
        const int iterations = 20_000;

        for (var iteration = 0; iteration < iterations; iteration++)
        {
            CommandSignal<int> command = new(() => CommandResult);
            using ManualResetEventSlim ready = new(false);

            // Race the first observation of the lazily allocated stream against the execution that
            // raises and immediately lowers the running flag.
            var reader = Task.Run(() =>
            {
                ready.Wait();
                return command.IsRunning;
            });

            ready.Set();
            _ = command.ExecuteAsync();
            var stream = await reader;

            // No execution is in flight once ExecuteAsync returns for the synchronous path, so a
            // stuck-true stream would have no future event to correct it. TryGetValue reads under
            // the state lock, giving a synchronized view of the settled value.
            _ = stream.TryGetValue(out var latched);
            await Assert.That(latched).IsFalse();
        }
    }

    /// <summary>
    /// Verifies the running-state stream is allocated lazily, cached on the second access, and
    /// reports <see langword="false"/> when first observed on an idle command (the install CAS wins
    /// and the post-install re-sync reads a non-drifted authoritative flag).
    /// </summary>
    /// <returns>A task that completes when the lazy-allocation assertions finish.</returns>
    [Test]
    public async Task IsRunningAllocatesLazilyAndCachesTheStream()
    {
        CommandSignal<int> command = new(() => CommandResult);

        var first = command.IsRunning;
        var second = command.IsRunning;

        await Assert.That(first).IsSameReferenceAs(second);
        await Assert.That(first.Value).IsFalse();
    }

    /// <summary>
    /// Verifies a normal true-then-false transition flows through an already-installed stream: the
    /// stream is observed before execution, so <c>SetRunning</c> takes the "stream present" path on
    /// both edges and the running flag returns to <see langword="false"/> at the end.
    /// </summary>
    /// <returns>A task that completes when the transition assertions finish.</returns>
    [Test]
    public async Task IsRunningTransitionsTrueThenFalseThroughInstalledStream()
    {
        CommandSignal<int> command = new(() => CommandResult);
        List<bool> running = [];
        _ = command.IsRunning.Changed.Subscribe(running.Add);

        _ = command.ExecuteAsync();

        await Assert.That(command.IsRunning.Value).IsFalse();
        await Assert.That(running.SequenceEqual(ExpectedRunningValues)).IsTrue();
    }

    /// <summary>
    /// Verifies that when an execution completes without the running-state stream ever having been
    /// observed, <c>SetRunning</c> exercises the "stream still null" reconciliation branch and a
    /// later first observation still reports <see langword="false"/>.
    /// </summary>
    /// <returns>A task that completes when the deferred-observation assertions finish.</returns>
    [Test]
    public async Task IsRunningReportsFalseWhenObservedOnlyAfterExecution()
    {
        CommandSignal<int> command = new(() => CommandResult);

        _ = command.ExecuteAsync();

        await Assert.That(command.IsRunning.Value).IsFalse();
    }

    /// <summary>
    /// Drives the lazy install deterministically: the stream is first observed while an async
    /// execution is in flight (running flag true), then the execution completes and lowers it. This
    /// exercises the install-side re-sync seeding a <see langword="true"/> value followed by the
    /// installed-stream completion edge.
    /// </summary>
    /// <returns>A task that completes when the mid-flight assertions finish.</returns>
    [Test]
    public async Task IsRunningObservedMidFlightSettlesFalseAfterCompletion()
    {
        using ManualResetEventSlim release = new(false);
        using ManualResetEventSlim entered = new(false);
        CommandSignal<int> command = new(async token =>
        {
            entered.Set();
            await Task.Run(() => release.Wait(token), token);
            return CommandResult;
        });

        var execution = command.ExecuteAsync();
        entered.Wait();

        // The first observation happens while the command is genuinely running.
        var stream = command.IsRunning;
        await Assert.That(stream.Value).IsTrue();

        release.Set();
        _ = await execution;

        await Assert.That(stream.Value).IsFalse();
    }

    /// <summary>
    /// Forces concurrent first observations of the lazily allocated stream so the install CAS has a
    /// loser, exercising the dispose-and-return-installed branch. All racers must observe the same
    /// instance.
    /// </summary>
    /// <returns>A task that completes when the concurrent-install assertions finish.</returns>
    [Test]
    public async Task ConcurrentFirstObservationsShareASingleStream()
    {
        const int iterations = 5_000;

        for (var iteration = 0; iteration < iterations; iteration++)
        {
            CommandSignal<int> command = new(() => CommandResult);
            using Barrier barrier = new(2);

            var left = Task.Run(() =>
            {
                barrier.SignalAndWait();
                return command.IsRunning;
            });
            var right = Task.Run(() =>
            {
                barrier.SignalAndWait();
                return command.IsRunning;
            });

            var streams = await Task.WhenAll(left, right);
            await Assert.That(streams[0]).IsSameReferenceAs(streams[1]);
        }
    }
}
