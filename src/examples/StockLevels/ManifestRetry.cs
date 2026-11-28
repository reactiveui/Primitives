// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async;

namespace StockLevels;

/// <summary>Retries a manifest read and falls back to the last known stock count.</summary>
internal static class ManifestRetry
{
    /// <summary>Checks a successful retry and a fallback after persistent failures.</summary>
    /// <param name="token">Cancels both manifest reads.</param>
    /// <returns>Completion after checking the fresh and cached counts.</returns>
    /// <exception cref="InvalidOperationException">The manifest produced an unexpected count or attempt total.</exception>
    internal static async Task CheckRetryAsync(CancellationToken token)
    {
        const int FreshStock = 18;
        const int CachedStock = 12;
        const int MaximumAttempts = 2;
        var attempts = 0;
        var read = SignalAsync.Defer(() =>
        {
            attempts++;
            return attempts == 1
                ? SignalAsync.Fail<int>(new IOException("The manifest is temporarily unavailable."))
                : SignalAsync.Emit(FreshStock);
        });
        var fresh = await read.Retry(MaximumAttempts).ToListAsync(token);
        var cached = await SignalAsync.Fail<int>(new IOException("The carrier is offline."))
            .Recover(static _ => SignalAsync.Emit(CachedStock))
            .ToListAsync(token);
        if (attempts != MaximumAttempts || fresh is not [FreshStock] || cached is not [CachedStock])
        {
            throw new InvalidOperationException("A transient failure should retry and an offline manifest should use the cached count.");
        }
    }
}
