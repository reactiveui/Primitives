// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Internal;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="AsyncStopTokenHelper"/>.</summary>
public sealed class AsyncStopTokenHelperTests
{
    /// <summary>Verifies <see cref="AsyncStopTokenHelper.LinkExternal"/> skips non-cancelable or identical tokens, cancels pre-canceled tokens immediately, and links live tokens.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task LinkExternal_HandlesUncancelableSamePreCanceledAndLiveTokens()
    {
        using CancellationTokenSource disposeCts = new();
        var noneReg = AsyncStopTokenHelper.LinkExternal(disposeCts, CancellationToken.None);
        await Assert.That(noneReg).IsEqualTo(default(CancellationTokenRegistration));

        var sameReg = AsyncStopTokenHelper.LinkExternal(disposeCts, disposeCts.Token);
        await Assert.That(sameReg).IsEqualTo(default(CancellationTokenRegistration));
        await Assert.That(disposeCts.IsCancellationRequested).IsFalse();

        using CancellationTokenSource liveExternal = new();
        var liveReg = AsyncStopTokenHelper.LinkExternal(disposeCts, liveExternal.Token);
        await liveExternal.CancelAsync();
        await Assert.That(disposeCts.IsCancellationRequested).IsTrue();
        await AsyncStopTokenHelper.DisposeStopRegistrationAsync(liveReg);

        using CancellationTokenSource preCanceledExternal = new();
        await preCanceledExternal.CancelAsync();
        using CancellationTokenSource secondDisposeCts = new();
        var preReg = AsyncStopTokenHelper.LinkExternal(secondDisposeCts, preCanceledExternal.Token);
        await Assert.That(preReg).IsEqualTo(default(CancellationTokenRegistration));
        await Assert.That(secondDisposeCts.IsCancellationRequested).IsTrue();
    }

    /// <summary>
    /// Verifies <see cref="AsyncStopTokenHelper.WatchStopToken{TCoordinator}"/> and
    /// <see cref="AsyncStopTokenHelper.ShouldReleaseAfterSubscribe{TCoordinator}"/> reflect cancellation and witness disposal.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WatchStopTokenAndShouldReleaseAfterSubscribe_TrackCancellationAndDisposal()
    {
        StubCoordinator coordinator = new();
        await Assert.That(AsyncStopTokenHelper.ShouldReleaseAfterSubscribe(coordinator, CancellationToken.None)).IsFalse();

        using CancellationTokenSource stopCts = new();
        var reg = AsyncStopTokenHelper.WatchStopToken(
            coordinator,
            static state => ((StubCoordinator)state!).CallbackCount++,
            stopCts.Token);

        await stopCts.CancelAsync();
        await Assert.That(coordinator.CallbackCount).IsEqualTo(1);
        await Assert.That(AsyncStopTokenHelper.ShouldReleaseAfterSubscribe(coordinator, stopCts.Token)).IsTrue();
        await AsyncStopTokenHelper.DisposeStopRegistrationAsync(reg);

        await coordinator.DisposeAsync();
        await Assert.That(AsyncStopTokenHelper.ShouldReleaseAfterSubscribe(coordinator, CancellationToken.None)).IsTrue();
    }

    /// <summary>Minimal <see cref="IWitnessState"/> coordinator double.</summary>
    private sealed class StubCoordinator : IWitnessState
    {
        /// <summary>Backing witness state.</summary>
        private WitnessAsyncState _witness;

        /// <summary>Gets or sets the stop callback invocation count.</summary>
        public int CallbackCount { get; set; }

        /// <inheritdoc/>
        public ref WitnessAsyncState Witness => ref _witness;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);
    }
}
