// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests ownership cleanup before terminal task results are returned.</summary>
public sealed class TaskResultCompletionSourceTests
{
    /// <summary>A successful result is returned only after asynchronous owner disposal completes.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task AwaitResultAsync_Success_AwaitsOwnerDisposal()
    {
        using CancellationTokenSource cancellation = new();
        TaskResultCompletionSource<int> source = new(cancellation.Token);
        ControlledOwner owner = new();
        var pending = source.AwaitResultAsync(owner).AsTask();

        await source.SetResultAndDisposeAsync(1, owner);
        await owner.Disposing.Task;
        await Assert.That(pending.IsCompleted).IsFalse();
        owner.Release.SetResult();

        await Assert.That(await pending).IsEqualTo(1);
        await Assert.That(owner.ReentrantDisposalCount).IsEqualTo(1);
    }

    /// <summary>Cancellation also waits for asynchronous owner disposal before reporting cancellation.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task AwaitResultAsync_Canceled_AwaitsOwnerDisposal()
    {
        using CancellationTokenSource cancellation = new();
        TaskResultCompletionSource<int> source = new(cancellation.Token);
        ControlledOwner owner = new();
        var pending = source.AwaitResultAsync(owner).AsTask();

        await cancellation.CancelAsync();
        await owner.Disposing.Task;
        await Assert.That(pending.IsCompleted).IsFalse();
        owner.Release.SetResult();

        var error = await Assert.That(async () => await pending).Throws<OperationCanceledException>();
        await Assert.That(error!.CancellationToken).IsEqualTo(cancellation.Token);
    }

    /// <summary>An owner disposal failure takes precedence over either a result or the original task failure.</summary>
    /// <param name="sourceFails">Whether the task result itself fails.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AwaitResultAsync_OwnerDisposalFails_PropagatesDisposalFailure(bool sourceFails)
    {
        TaskResultCompletionSource<int> source = new(CancellationToken.None);
        ControlledOwner owner = new();
        InvalidOperationException expected = new("owner disposal failed");
        var pending = source.AwaitResultAsync(owner).AsTask();
        if (sourceFails)
        {
            await source.SetExceptionAndDisposeAsync(new ArgumentException("source failed"), owner);
        }
        else
        {
            await source.SetResultAndDisposeAsync(1, owner);
        }

        await owner.Disposing.Task;
        owner.Release.SetException(expected);

        var error = await Assert.That(async () => await pending).ThrowsExactly<InvalidOperationException>();
        await Assert.That(error).IsSameReferenceAs(expected);
    }

    /// <summary>Separates notification disposal from externally awaited cleanup.</summary>
    private sealed class ControlledOwner : IAsyncDisposable, IReentrantAsyncDisposable
    {
        /// <summary>Gets the signal that external disposal has started.</summary>
        internal TaskCompletionSource Disposing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the gate controlling completion of external disposal.</summary>
        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the count of notification-side disposals.</summary>
        internal int ReentrantDisposalCount { get; private set; }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            Disposing.SetResult();
            await Release.Task;
        }

        /// <inheritdoc/>
        public ValueTask DisposeFromNotificationAsync()
        {
            ReentrantDisposalCount++;
            return default;
        }
    }
}
