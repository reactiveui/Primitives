// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <summary>Tests the scoped Ctrl+C adapter without mutating the process console event.</summary>
public sealed class ConsoleCancellationScopeTests
{
    /// <summary>The finite barrier wait for an in-flight callback.</summary>
    private static readonly TimeSpan CallbackTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies repeated requests share one cancellation and event removal happens once.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RepeatedCancellationUnsubscribesAndDisposesOnce()
    {
        ConsoleCancelEventHandler? subscribed = null;
        var removals = 0;
        await using var scope = new ConsoleCancellationScope(
            handler => subscribed = handler,
            handler =>
            {
                if (ReferenceEquals(handler, subscribed))
                {
                    removals++;
                }
            });
        var token = scope.Token;

        var first = scope.RequestCancellation();
        var repeated = scope.RequestCancellation();
        await first.ConfigureAwait(false);
        await repeated.ConfigureAwait(false);
        await scope.DisposeAsync().ConfigureAwait(false);
        await scope.DisposeAsync().ConfigureAwait(false);
        await scope.RequestCancellation().ConfigureAwait(false);

        await Assert.That(subscribed).IsNotNull();
        await Assert.That(ReferenceEquals(first, repeated)).IsTrue();
        await Assert.That(token.IsCancellationRequested).IsTrue();
        await Assert.That(removals).IsEqualTo(1);
    }

    /// <summary>Verifies a cancellation callback failure is retained and observed during disposal.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancellationCallbackFailureIsObservedByDisposal()
    {
        await using var scope = new ConsoleCancellationScope(static handler => { }, static handler => { });
        await using var registration = scope.Token.Register(static () => throw new InvalidOperationException("callback failed"));

        var cancellationError = await Assert.ThrowsAsync<AggregateException>(() => scope.RequestCancellation());
        await scope.DisposeAsync().ConfigureAwait(false);

        await Assert.That(cancellationError).IsNotNull();
        await Assert.That(ReferenceEquals(cancellationError, scope.Failure)).IsTrue();
    }

    /// <summary>Verifies disposal waits for an in-flight cancellation operation after its callback runs.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposalWaitsForInFlightCancellationOperation()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbackCount = new StrongBox<int>();
        var removals = 0;
        await using var scope = new ConsoleCancellationScope(
            static handler => { },
            handler => removals++,
            async source =>
            {
                await source.CancelAsync().ConfigureAwait(false);
                _ = entered.TrySetResult();
                await release.Task.WaitAsync(CallbackTimeout).ConfigureAwait(false);
            });
        await using var registration = scope.Token.UnsafeRegister(
            static state =>
            {
                if (state is StrongBox<int> count)
                {
                    _ = Interlocked.Increment(ref count.Value);
                }
            },
            callbackCount);

        var first = scope.RequestCancellation();
        var repeated = scope.RequestCancellation();
        var disposal = Task.CompletedTask;
        try
        {
            await entered.Task.WaitAsync(CallbackTimeout).ConfigureAwait(false);
            disposal = scope.DisposeAsync().AsTask();
            await Assert.That(disposal.IsCompleted).IsFalse();
            await Assert.That(ReferenceEquals(first, repeated)).IsTrue();
        }
        finally
        {
            _ = release.TrySetResult();
        }

        await first.ConfigureAwait(false);
        await repeated.ConfigureAwait(false);
        await disposal.ConfigureAwait(false);
        await scope.RequestCancellation().ConfigureAwait(false);

        await Assert.That(callbackCount.Value).IsEqualTo(1);
        await Assert.That(removals).IsEqualTo(1);
    }

    /// <summary>Verifies subscription failure keeps the original exception.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionFailurePropagatesOriginalException()
    {
        var expected = new InvalidOperationException("subscription failed");

        var observed = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
        {
            _ = new ConsoleCancellationScope(handler => throw expected, static handler => { });
            return Task.CompletedTask;
        });

        await Assert.That(ReferenceEquals(observed, expected)).IsTrue();
    }
}
