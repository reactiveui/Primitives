// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteSynchronousCommandWorker"/>.</summary>
public sealed class SqliteSynchronousCommandWorkerTests
{
    /// <summary>Defines a single admitted command.</summary>
    private const int OneCommand = 1;

    /// <summary>Defines two admitted commands.</summary>
    private const int TwoCommands = 2;

    /// <summary>Defines three admitted commands.</summary>
    private const int ThreeCommands = 3;

    /// <summary>The first command result.</summary>
    private const int FirstResult = 1;

    /// <summary>The second command result.</summary>
    private const int SecondResult = 2;

    /// <summary>The third command result.</summary>
    private const int ThirdResult = 3;

    /// <summary>An invalid zero count value.</summary>
    private const int ZeroCount = 0;

    /// <summary>Defines a single caller-declared byte.</summary>
    private const long OneByte = 1;

    /// <summary>Defines two caller-declared bytes.</summary>
    private const long TwoBytes = 2;

    /// <summary>Defines three caller-declared bytes.</summary>
    private const long ThreeBytes = 3;

    /// <summary>Defines four caller-declared bytes.</summary>
    private const long FourBytes = 4;

    /// <summary>An invalid zero byte value.</summary>
    private const long ZeroBytes = 0;

    /// <summary>A committed receipt client sequence.</summary>
    private const long ReceiptClientSequence = 7;

    /// <summary>A committed receipt snapshot revision.</summary>
    private const long ReceiptSnapshotRevision = 11;

    /// <summary>Defines a short guard timeout for deterministic asynchronous tests.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies commands run serially in FIFO order on one processing task.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCommandsAreQueued_ThenWorkerRunsThemSeriallyInFifoOrder()
    {
        await using var worker = new SqliteSynchronousCommandWorker(ThreeCommands, ThreeBytes);
        using var firstStarted = new ManualResetEventSlim();
        using var releaseFirst = new ManualResetEventSlim();
        List<int> order = [];
        List<int> threadIds = [];
        var activeCommands = 0;
        var maximumActiveCommands = 0;

        var first = worker.ExecuteAsync(
            _ => RunTrackedCommand(FirstResult, firstStarted, releaseFirst),
            OneByte,
            CancellationToken.None);
        await Assert.That(firstStarted.Wait(GuardTimeout)).IsTrue();
        var second = worker.ExecuteAsync(
            _ => RunTrackedCommand(SecondResult),
            OneByte,
            CancellationToken.None);
        var third = worker.ExecuteAsync(
            _ => RunTrackedCommand(ThirdResult),
            OneByte,
            CancellationToken.None);

        await Assert.That(order.Count).IsEqualTo(ZeroCount);
        releaseFirst.Set();
        _ = await Task.WhenAll(first, second, third).WaitAsync(GuardTimeout);

        await Assert.That(order[0]).IsEqualTo(FirstResult);
        await Assert.That(order[1]).IsEqualTo(SecondResult);
        await Assert.That(order[2]).IsEqualTo(ThirdResult);
        await Assert.That(maximumActiveCommands).IsEqualTo(OneCommand);
        await Assert.That(threadIds[1]).IsEqualTo(threadIds[0]);
        await Assert.That(threadIds[2]).IsEqualTo(threadIds[0]);

        int RunTrackedCommand(int value, ManualResetEventSlim? started = null, ManualResetEventSlim? release = null)
        {
            _ = Interlocked.Increment(ref activeCommands);
            maximumActiveCommands = Math.Max(maximumActiveCommands, Volatile.Read(ref activeCommands));
            started?.Set();
            if (release is not null)
            {
                _ = release.Wait(GuardTimeout, CancellationToken.None);
            }

            order.Add(value);
            threadIds.Add(Environment.CurrentManagedThreadId);
            _ = Interlocked.Decrement(ref activeCommands);
            return value;
        }
    }

    /// <summary>Verifies count overflow considers the active command.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenActiveCommandConsumesCountCapacity_ThenNextCommandIsRejectedImmediately()
    {
        await using var worker = new SqliteSynchronousCommandWorker(OneCommand, TwoBytes);
        using var firstStarted = new ManualResetEventSlim();
        using var releaseFirst = new ManualResetEventSlim();

        var first = worker.ExecuteAsync(
            commandCancellation =>
            {
                firstStarted.Set();
                _ = releaseFirst.Wait(GuardTimeout, commandCancellation);
                return FirstResult;
            },
            OneByte,
            CancellationToken.None);
        await Assert.That(firstStarted.Wait(GuardTimeout)).IsTrue();

        Func<Task<int>> rejected = () => worker.ExecuteAsync(static _ => SecondResult, OneByte, CancellationToken.None);

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(rejected);
        releaseFirst.Set();

        await Assert.That(exception?.CanFitWhenEmpty).IsTrue();
        await Assert.That(await first.WaitAsync(GuardTimeout)).IsEqualTo(FirstResult);
    }

    /// <summary>Verifies byte overflow considers active work and reports whether the command can ever fit.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenActiveCommandConsumesByteCapacity_ThenNextCommandIsRejectedImmediately()
    {
        await using var worker = new SqliteSynchronousCommandWorker(TwoCommands, ThreeBytes);
        using var firstStarted = new ManualResetEventSlim();
        using var releaseFirst = new ManualResetEventSlim();

        var first = worker.ExecuteAsync(
            commandCancellation =>
            {
                firstStarted.Set();
                _ = releaseFirst.Wait(GuardTimeout, commandCancellation);
                return FirstResult;
            },
            TwoBytes,
            CancellationToken.None);
        await Assert.That(firstStarted.Wait(GuardTimeout)).IsTrue();

        Func<Task<int>> fitsWhenEmpty = () => worker.ExecuteAsync(static _ => SecondResult, TwoBytes, CancellationToken.None);
        Func<Task<int>> neverFits = () => worker.ExecuteAsync(static _ => ThirdResult, FourBytes, CancellationToken.None);

        var transient = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(fitsWhenEmpty);
        var oversize = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(neverFits);
        releaseFirst.Set();

        await Assert.That(transient?.CanFitWhenEmpty).IsTrue();
        await Assert.That(oversize?.CanFitWhenEmpty).IsFalse();
        await Assert.That(await first.WaitAsync(GuardTimeout)).IsEqualTo(FirstResult);
    }

    /// <summary>Verifies cancellation before dispatch removes queued work and frees capacity.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQueuedCommandIsCanceledBeforeDispatch_ThenItNeverExecutes()
    {
        await using var worker = new SqliteSynchronousCommandWorker(TwoCommands, TwoBytes);
        using var firstStarted = new ManualResetEventSlim();
        using var releaseFirst = new ManualResetEventSlim();
        using var cancellation = new CancellationTokenSource();
        var canceledCommandExecuted = false;

        var first = worker.ExecuteAsync(
            commandCancellation =>
            {
                firstStarted.Set();
                _ = releaseFirst.Wait(GuardTimeout, commandCancellation);
                return FirstResult;
            },
            OneByte,
            CancellationToken.None);
        await Assert.That(firstStarted.Wait(GuardTimeout)).IsTrue();
        var canceled = worker.ExecuteAsync(
            _ =>
            {
                canceledCommandExecuted = true;
                return SecondResult;
            },
            OneByte,
            cancellation.Token);

        await cancellation.CancelAsync();
        await Assert.That(canceled).ThrowsExactly<TaskCanceledException>();
        var admittedAfterCancel = worker.ExecuteAsync(static _ => ThirdResult, OneByte, CancellationToken.None);
        releaseFirst.Set();

        await Assert.That(await first.WaitAsync(GuardTimeout)).IsEqualTo(FirstResult);
        await Assert.That(await admittedAfterCancel.WaitAsync(GuardTimeout)).IsEqualTo(ThirdResult);
        await Assert.That(canceledCommandExecuted).IsFalse();
    }

    /// <summary>Verifies cancellation after a command has produced a receipt cannot replace that receipt.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCancellationArrivesAfterReceipt_ThenReceiptIsReturned()
    {
        await using var worker = new SqliteSynchronousCommandWorker(OneCommand, OneByte);
        using var cancellation = new CancellationTokenSource();
        var receipt = new LocalCommitResult(
            OperationId.New(),
            ReceiptClientSequence,
            ReceiptSnapshotRevision,
            new(2026, 9, 12, 8, 0, 0, TimeSpan.Zero));

        var task = worker.ExecuteAsync(
            _ =>
            {
                cancellation.Cancel();
                return receipt;
            },
            OneByte,
            cancellation.Token);

        await Assert.That(await task.WaitAsync(GuardTimeout)).IsEqualTo(receipt);
    }

    /// <summary>Verifies disposal closes admission, rejects queued work, and waits for active work.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenWorkerIsDisposedAsync_ThenQueuedWorkIsRejectedAndActiveWorkIsJoined()
    {
        var worker = new SqliteSynchronousCommandWorker(TwoCommands, TwoBytes);
        using var firstStarted = new ManualResetEventSlim();
        using var releaseFirst = new ManualResetEventSlim();

        var first = worker.ExecuteAsync(
            commandCancellation =>
            {
                firstStarted.Set();
                _ = releaseFirst.Wait(GuardTimeout, commandCancellation);
                return FirstResult;
            },
            OneByte,
            CancellationToken.None);
        await Assert.That(firstStarted.Wait(GuardTimeout)).IsTrue();
        var queued = worker.ExecuteAsync(static _ => SecondResult, OneByte, CancellationToken.None);

        var disposeTask = worker.DisposeAsync().AsTask();
        var concurrentDisposeTask = worker.DisposeAsync().AsTask();
        Func<Task<int>> afterDispose = () => worker.ExecuteAsync(static _ => ThirdResult, OneByte, CancellationToken.None);

        await Assert.That(queued).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(afterDispose).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(disposeTask.IsCompleted).IsFalse();
        await Assert.That(concurrentDisposeTask.IsCompleted).IsFalse();
        releaseFirst.Set();

        await Assert.That(await first.WaitAsync(GuardTimeout)).IsEqualTo(FirstResult);
        await Task.WhenAll(disposeTask, concurrentDisposeTask).WaitAsync(GuardTimeout);
        await Assert.That(queued.IsCompleted).IsTrue();
        await Assert.That(first.IsCompletedSuccessfully).IsTrue();
        await worker.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
    }

    /// <summary>Verifies command faults and active cancellations complete correctly and release capacity.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCommandFaultsOrCancelsDuringDispatch_ThenCapacityIsReleased()
    {
        await using var worker = new SqliteSynchronousCommandWorker(OneCommand, OneByte);
        var failure = new InvalidOperationException("boom");
        Func<CancellationToken, int> throwing = _ => throw failure;

        var failed = worker.ExecuteAsync(throwing, OneByte, CancellationToken.None);
        await Assert.That(failed).ThrowsExactly<InvalidOperationException>();
        var afterFailure = worker.ExecuteAsync(static _ => FirstResult, OneByte, CancellationToken.None);
        await Assert.That(await afterFailure.WaitAsync(GuardTimeout)).IsEqualTo(FirstResult);

        using var cancellation = new CancellationTokenSource();
        var canceled = worker.ExecuteAsync(
            token =>
            {
                cancellation.Cancel();
                token.ThrowIfCancellationRequested();
                return SecondResult;
            },
            OneByte,
            cancellation.Token);

        await Assert.That(canceled).ThrowsExactly<TaskCanceledException>();
        var afterCancellation = worker.ExecuteAsync(static _ => ThirdResult, OneByte, CancellationToken.None);
        await Assert.That(await afterCancellation.WaitAsync(GuardTimeout)).IsEqualTo(ThirdResult);
    }

    /// <summary>Verifies invalid configuration and admission inputs fail before work is admitted.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenConfigurationOrAdmissionInputIsInvalid_ThenWorkerRejectsBeforeDispatch()
    {
        Action invalidCount = static () => _ = new SqliteSynchronousCommandWorker(ZeroCount, OneByte);
        Action invalidBytes = static () => _ = new SqliteSynchronousCommandWorker(OneCommand, ZeroBytes);
        await using var worker = new SqliteSynchronousCommandWorker(OneCommand, OneByte);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Func<Task<int>> invalidRetainedBytes = () => worker.ExecuteAsync(static _ => FirstResult, ZeroBytes, CancellationToken.None);
        Func<Task<int>> preCanceled = () => worker.ExecuteAsync(static _ => FirstResult, OneByte, cancellation.Token);

        await Assert.That(invalidCount).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(invalidBytes).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(invalidRetainedBytes).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(preCanceled).ThrowsExactly<OperationCanceledException>();
    }
}
