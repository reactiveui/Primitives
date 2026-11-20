// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Advanced;

namespace ReactiveUI.Primitives.Async.Internal;

/// <summary>Shared external-cancellation linking and stop-token registration helpers for async coordinators.</summary>
internal static class AsyncStopTokenHelper
{
    /// <summary>Links an external subscribe-time <see cref="CancellationToken"/> into <paramref name="disposeCts"/> unless already using its token.</summary>
    /// <param name="disposeCts">The cancellation source triggered on disposal.</param>
    /// <param name="external">The subscribe-time cancellation token.</param>
    /// <returns>The token registration when <paramref name="external"/> can still be cancelled; otherwise, <see langword="default"/>.</returns>
    internal static CancellationTokenRegistration LinkExternal(
        CancellationTokenSource disposeCts,
        CancellationToken external)
    {
        if (!external.CanBeCanceled || external == disposeCts.Token)
        {
            return default;
        }

        if (external.IsCancellationRequested)
        {
            disposeCts.Cancel();
            return default;
        }

        return external.UnsafeRegister(
            static state => ((CancellationTokenSource)state!).Cancel(),
            disposeCts);
    }

    /// <summary>Registers a stop token that invokes <paramref name="completeCallback"/> with <paramref name="coordinator"/> when cancelled.</summary>
    /// <typeparam name="TCoordinator">The coordinator type.</typeparam>
    /// <param name="coordinator">The coordinator passed as callback state.</param>
    /// <param name="completeCallback">Callback invoked when <paramref name="stopToken"/> fires.</param>
    /// <param name="stopToken">The stop token to watch.</param>
    /// <returns>The token registration, or <see langword="default"/> when <paramref name="stopToken"/> cannot be cancelled.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static CancellationTokenRegistration WatchStopToken<TCoordinator>(
        TCoordinator coordinator,
        Action<object?> completeCallback,
        CancellationToken stopToken)
        where TCoordinator : class =>
        stopToken.CanBeCanceled
            ? stopToken.UnsafeRegister(completeCallback, coordinator)
            : default;

    /// <summary>Checks whether a newly attached source subscription must be released immediately because the coordinator disposed or <paramref name="stopToken"/> fired during subscribe.</summary>
    /// <typeparam name="TCoordinator">The coordinator witness type.</typeparam>
    /// <param name="coordinator">The coordinator witness.</param>
    /// <param name="stopToken">The stop token watched by the coordinator.</param>
    /// <returns><see langword="true"/> when the subscription must be disposed immediately; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool ShouldReleaseAfterSubscribe<TCoordinator>(
        TCoordinator coordinator,
        CancellationToken stopToken)
        where TCoordinator : class, IWitnessState =>
        coordinator.Witness.HasDisposed || stopToken.IsCancellationRequested;

    /// <summary>Disposes a stop-token registration across target frameworks.</summary>
    /// <param name="stopRegistration">The registration to dispose.</param>
    /// <returns>A task representing the asynchronous disposal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ValueTask DisposeStopRegistrationAsync(CancellationTokenRegistration stopRegistration)
    {
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        return stopRegistration.DisposeAsync();
#else
        stopRegistration.Dispose();
        return default;
#endif
    }
}
