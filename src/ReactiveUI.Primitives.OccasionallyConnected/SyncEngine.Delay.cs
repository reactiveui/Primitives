// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Delay helpers for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Registers cancellation for an injected-clock delay without flowing execution context.</summary>
    /// <param name="completion">The delay completion.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The async-disposable cancellation registration.</returns>
    private static AsyncDelayCancellationRegistration UnsafeRegisterDelayCancellation(
        TaskCompletionSource<bool> completion,
        CancellationToken cancellationToken) =>
        new(cancellationToken.UnsafeRegister(
            static state =>
            {
                ArgumentExceptionHelper.ThrowIfNull(state);
                _ = ((TaskCompletionSource<bool>)state).TrySetCanceled();
            },
            completion));

    /// <summary>Wraps a cancellation registration for analyzer-consistent asynchronous disposal.</summary>
    /// <param name="registration">The cancellation registration.</param>
    private readonly struct AsyncDelayCancellationRegistration(CancellationTokenRegistration registration) : IAsyncDisposable
    {
        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            registration.Dispose();
            return default;
        }
    }
}
