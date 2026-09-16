// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests ownership cleanup before terminal task results are returned.</summary>
public sealed class TaskResultCompletionSourceTests
{
    /// <summary>Publishing a result disposes the owner through the notification path, and the awaited result waits for external disposal.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task AwaitResultAsync_Success_AwaitsOwnerDisposal()
    {
        TaskResultCompletionSource<int> source = new(CancellationToken.None);
        ControlledOwner owner = new();

        await source.SetResultAndDisposeAsync(1, owner);
        await Assert.That(owner.ReentrantDisposalCount).IsEqualTo(1);

        var pending = source.AwaitResultAsync(owner).AsTask();
        await Assert.That(owner.Disposing.Task.IsCompleted).IsTrue();
        await Assert.That(pending.IsCompleted).IsFalse();
        owner.Release.SetResult();

        await Assert.That(await pending).IsEqualTo(1);
    }

    /// <summary>A cancelled wait also waits for owner disposal before reporting cancellation.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task AwaitResultAsync_Canceled_AwaitsOwnerDisposal()
    {
        using CancellationTokenSource cancellation = new();
        await cancellation.CancelAsync();
        TaskResultCompletionSource<int> source = new(cancellation.Token);
        ControlledOwner owner = new();

        var pending = source.AwaitResultAsync(owner).AsTask();
        await Assert.That(owner.Disposing.Task.IsCompleted).IsTrue();
        await Assert.That(pending.IsCompleted).IsFalse();
        owner.Release.SetResult();

        var error = await Assert.That(async () => await pending).Throws<OperationCanceledException>();
        await Assert.That(error!.CancellationToken).IsEqualTo(cancellation.Token);
        await Assert.That(owner.ReentrantDisposalCount).IsEqualTo(0);
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
        if (sourceFails)
        {
            await source.SetExceptionAndDisposeAsync(new ArgumentException("source failed"), owner);
        }
        else
        {
            await source.SetResultAndDisposeAsync(1, owner);
        }

        var pending = source.AwaitResultAsync(owner).AsTask();
        owner.Release.SetException(expected);

        var error = await Assert.That(async () => await pending).ThrowsExactly<InvalidOperationException>();
        await Assert.That(error).IsSameReferenceAs(expected);
        await Assert.That(owner.ReentrantDisposalCount).IsEqualTo(1);
    }

    /// <summary>Completes a disposal made from inside a notification immediately and holds any other disposal open.</summary>
    [DebuggerDisplay("ControlledOwner: {_witness}")]
    private sealed class ControlledOwner : IWitnessState
    {
        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness;

        /// <summary>Gets the signal that external disposal has started.</summary>
        internal TaskCompletionSource Disposing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the gate controlling completion of external disposal.</summary>
        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the count of disposals the witness state recorded as made from inside a notification.</summary>
        internal int ReentrantDisposalCount { get; private set; }

        /// <inheritdoc/>
        ref WitnessAsyncState IWitnessState.Witness => ref _witness;

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            if (_witness.IsDisposingFromNotification && ReentrantDisposalCount == 0)
            {
                ReentrantDisposalCount++;
                return;
            }

            Disposing.SetResult();
            await Release.Task;
            await WitnessAsync.DisposeStateAsync(this);
        }
    }
}
