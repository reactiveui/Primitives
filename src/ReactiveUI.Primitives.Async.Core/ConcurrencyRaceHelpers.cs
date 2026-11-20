// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Internal;

namespace ReactiveUI.Primitives.Internal;

/// <summary>Atomic ownership claims, gate teardown, and cancellation tolerant of prior disposal.</summary>
internal static class ConcurrencyRaceHelpers
{
    /// <summary>Atomically transitions <paramref name="state"/> from <paramref name="openSentinel"/> to <paramref name="claimedSentinel"/>.</summary>
    /// <param name="state">The reference to the state field.</param>
    /// <param name="openSentinel">The sentinel value the state must currently hold.</param>
    /// <param name="claimedSentinel">The sentinel value the state transitions to on success.</param>
    /// <returns>
    /// <see langword="true"/> when this caller won the claim; <see langword="false"/> when another caller held it.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool TryClaim(ref int state, int openSentinel, int claimedSentinel) =>
        Interlocked.CompareExchange(ref state, claimedSentinel, openSentinel) == openSentinel;

    /// <summary>Cancels <paramref name="cts"/>, swallowing the <see cref="ObjectDisposedException"/> a racing dispose raises.</summary>
    /// <param name="cts">The cancellation token source to cancel.</param>
    /// <returns>
    /// <see langword="true"/> when the cancellation completed; <see langword="false"/> when a concurrent caller had
    /// disposed the source first.
    /// </returns>
    internal static async ValueTask<bool> TryCancelAsync(CancellationTokenSource cts)
    {
        try
        {
            await cts.CancelAsync().ConfigureAwait(false);
            return true;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }

    /// <summary>Executes <paramref name="action"/>, swallowing <see cref="OperationCanceledException"/> and <see cref="ObjectDisposedException"/> raised during concurrent teardown.</summary>
    /// <typeparam name="TState">The caller state type.</typeparam>
    /// <param name="state">The state forwarded to <paramref name="action"/>.</param>
    /// <param name="action">The asynchronous teardown work.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="action"/> completed normally; <see langword="false"/> when
    /// cancellation or prior disposal interrupted it.
    /// </returns>
    internal static async ValueTask<bool> IgnoreCanceledOrDisposedAsync<TState>(
        TState state,
        Func<TState, ValueTask> action)
    {
        try
        {
            await action(state).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }

    /// <summary>Enters <paramref name="gate"/> and executes <paramref name="action"/>, ignoring cancellation or disposal races.</summary>
    /// <typeparam name="TState">The caller state type.</typeparam>
    /// <param name="gate">The serialization gate to enter.</param>
    /// <param name="state">The state forwarded to <paramref name="action"/>.</param>
    /// <param name="action">The mutation to run while holding <paramref name="gate"/>.</param>
    /// <param name="cancellationToken">The disposal token guarding the gate wait.</param>
    /// <returns>A task representing the gated teardown operation.</returns>
    internal static async ValueTask RunUnderGateIgnoringTeardownAsync<TState>(
        AsyncFlowGate gate,
        TState state,
        Action<TState> action,
        CancellationToken cancellationToken) =>
        _ = await IgnoreCanceledOrDisposedAsync(
            (Gate: gate, Token: cancellationToken, State: state, Action: action),
            static async s =>
            {
                using (await s.Gate.EnterAsync(s.Token).ConfigureAwait(false))
                {
                    s.Action(s.State);
                }
            }).ConfigureAwait(false);
}
