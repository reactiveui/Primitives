// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OperationSynchronizationWaiter"/>.</summary>
/// <content>Invalid waiting requests and status results.</content>
public sealed partial class OperationSynchronizationWaiterTests
{
    /// <summary>Verifies invalid timeout values are rejected before subscribing or looking up state.</summary>
    /// <param name="milliseconds">The invalid timeout.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(-2L)]
    [Arguments(4_294_967_295L)]
    public async Task WhenTimeoutIsInvalid_ThenNoStatusWorkStarts(long milliseconds)
    {
        using var states = new Signal<SyncOperationStatus>();
        var lookups = 0;
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            _ =>
            {
                lookups++;
                return new((SyncOperationStatus?)null);
            },
            OperationId.New(),
            TimeSpan.FromMilliseconds(milliseconds),
            new FakeTimeProvider(),
            CancellationToken.None);

        await Assert.That(() => wait).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(lookups).IsEqualTo(0);
        await Assert.That(states.HasObservers).IsFalse();
    }

    /// <summary>Verifies an empty operation identifier cannot start a wait.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenOperationIdIsEmpty_ThenWaitRejectsIt()
    {
        using var states = new Signal<SyncOperationStatus>();
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            static _ => new((SyncOperationStatus?)null),
            default,
            WaitTimeout,
            new FakeTimeProvider(),
            CancellationToken.None);

        await Assert.That(() => wait).ThrowsExactly<ArgumentException>();
        await Assert.That(states.HasObservers).IsFalse();
    }

    /// <summary>Verifies a lookup cannot accidentally satisfy a different operation's wait.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenLookupReturnsAnotherOperation_ThenWaitFailsClosed()
    {
        using var states = new Signal<SyncOperationStatus>();
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            static _ => new(Status(OperationId.New(), SyncOperationState.Synchronized)),
            OperationId.New(),
            WaitTimeout,
            new FakeTimeProvider(),
            CancellationToken.None);

        await Assert.That(() => wait).ThrowsExactly<InvalidOperationException>();
        await Assert.That(states.HasObservers).IsFalse();
    }

    /// <summary>Verifies an already canceled caller does not create a subscription.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenCallerAlreadyCanceled_ThenNoSubscriptionIsCreated()
    {
        using var states = new Signal<SyncOperationStatus>();
        var wait = OperationSynchronizationWaiter.WaitAsync(
            states,
            static _ => new((SyncOperationStatus?)null),
            OperationId.New(),
            WaitTimeout,
            new FakeTimeProvider(),
            new(canceled: true));

        await Assert.That(() => wait).ThrowsExactly<OperationCanceledException>();
        await Assert.That(states.HasObservers).IsFalse();
    }
}
