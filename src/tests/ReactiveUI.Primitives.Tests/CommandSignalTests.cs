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

    /// <summary>Number of tasks that race for the lazily allocated stream in the contention test.</summary>
    private const int ContendingTasks = 2;

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
            CommandSignal<int> command = new(static () => CommandResult);
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
    /// and the post-install reconcile publishes the authoritative flag).
    /// </summary>
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

    /// <summary>
    /// Verifies a normal true-then-false transition flows through an already-installed stream: the
    /// stream is observed before execution, so <c>SetRunning</c> takes the "stream present" path on
    /// both edges and the running flag returns to <see langword="false"/> at the end.
    /// </summary>
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

    /// <summary>
    /// Verifies that when an execution completes without the running-state stream ever having been
    /// observed, <c>SetRunning</c> exercises the "stream still null" reconciliation branch and a
    /// later first observation still reports <see langword="false"/>.
    /// </summary>
    /// <returns>A task that completes when the deferred-observation assertions finish.</returns>
    [Test]
    public async Task IsRunningReportsFalseWhenObservedOnlyAfterExecution()
    {
        CommandSignal<int> command = new(static () => CommandResult);

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
    /// Results fan out to every active subscriber, and unsubscribing removes that subscriber and nobody else.
    /// This walks the observer set through all three of its shapes — one observer, a pair, a longer array — and
    /// back down again, because each shape has its own add and remove path.
    /// </summary>
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

        // Remove from the middle of a three-observer array: the survivors must both keep receiving.
        secondSubscription.Dispose();
        _ = command.ExecuteAsync();

        // Remove from a two-observer array, collapsing it back to a single observer.
        thirdSubscription.Dispose();
        _ = command.ExecuteAsync();

        // Removing the last observer, then disposing the same handle again, must both be safe.
        firstSubscription.Dispose();
        firstSubscription.Dispose();
        _ = command.ExecuteAsync();

        await Assert.That(first.Count).IsEqualTo(ThreeResults);
        await Assert.That(second.Count).IsEqualTo(1);
        await Assert.That(third.Count).IsEqualTo(TwoResults);
        await Assert.That(first.TrueForAll(static value => value == CommandResult)).IsTrue();
    }

    /// <summary>
    /// Disposing the command drops its observer set, so a subscription handle disposed afterwards has nothing
    /// to detach from. That must be a quiet no-op rather than a failure.
    /// </summary>
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

    /// <summary>
    /// An async command that faults publishes the fault to the fault stream before the awaited task rethrows it,
    /// and still lowers the running flag on the way out.
    /// </summary>
    /// <returns>A task that completes when the async-fault assertions finish.</returns>
    [Test]
    public async Task AsyncExecutionPublishesTheFaultAndStillLowersTheRunningFlag()
    {
        InvalidOperationException fault = new("async failed");

        // The delegate type is spelled out because a body that only throws gives the compiler no return
        // expression to infer Task<int> from.
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

    /// <summary>
    /// The fault stream is allocated on first use and cached thereafter, and disposing the command tears down
    /// the gate subscription along with the streams it created.
    /// </summary>
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

        // The command released the gate, so the gate signal no longer feeds anything.
        await Assert.That(canRun.HasObservers).IsFalse();
        await Assert.That(running.IsDisposed).IsTrue();
    }

    /// <summary>
    /// Forces concurrent first observations of the lazily allocated fault stream so the install CAS has a loser,
    /// exercising the dispose-and-return-installed branch. All racers must observe the same instance.
    /// </summary>
    /// <returns>A task that completes when the concurrent fault-stream assertions finish.</returns>
    [Test]
    public async Task ConcurrentFirstObservationsShareASingleFaultStream()
    {
        const int iterations = 5_000;

        for (var iteration = 0; iteration < iterations; iteration++)
        {
            CommandSignal<int> command = new(static () => CommandResult);
            using Barrier barrier = new(ContendingTasks);

            var left = Task.Run(() =>
            {
                barrier.SignalAndWait();
                return command.Faults;
            });
            var right = Task.Run(() =>
            {
                barrier.SignalAndWait();
                return command.Faults;
            });

            var streams = await Task.WhenAll(left, right);
            await Assert.That(streams[0]).IsSameReferenceAs(streams[1]);
        }
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
            CommandSignal<int> command = new(static () => CommandResult);
            using Barrier barrier = new(ContendingTasks);

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
