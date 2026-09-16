// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the notification gate, cancellation link and disposal state embedded by asynchronous witnesses.</summary>
public sealed class WitnessAsyncStateTests
{
    /// <summary>The value delivered to the witness.</summary>
    private const int DeliveredValue = 1;

    /// <summary>A construction-time token reports disposal once cancelled, before the witness has linked it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task HasDisposed_ConstructionTokenCancelledBeforeLink_ReportsDisposed()
    {
        using CancellationTokenSource external = new();
        StateWitness witness = new(external.Token);

        await Assert.That(witness.HasDisposed).IsFalse();
        await external.CancelAsync();

        await Assert.That(witness.HasDisposed).IsTrue();
    }

    /// <summary>Linking a token that cannot be cancelled leaves the witness active.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task LinkUpstreamCancellation_NonCancellableToken_LeavesWitnessActive()
    {
        StateWitness witness = new(CancellationToken.None);

        witness.LinkUpstreamCancellation(CancellationToken.None);
        await witness.OnNextAsync(DeliveredValue, CancellationToken.None);

        await Assert.That(witness.HasDisposed).IsFalse();
        await Assert.That(witness.Received).IsEqualTo(DeliveredValue);
    }

    /// <summary>Witness that counts delivered values.</summary>
    /// <param name="externalLink">The construction-time token whose cancellation disposes the witness.</param>
    [DebuggerDisplay("StateWitness: {_witness}")]
    private sealed class StateWitness(CancellationToken externalLink) : IWitnessAsync<int>
    {
        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness = new(externalLink);

        /// <summary>Gets the number of delivered values.</summary>
        internal int Received { get; private set; }

        /// <inheritdoc/>
        ref WitnessAsyncState IWitnessState.Witness => ref _witness;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) =>
            WitnessAsync.OnNextAsync(this, value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<int>.OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            Received++;
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnCompletedAsyncCore(Result result) => default;
    }
}
