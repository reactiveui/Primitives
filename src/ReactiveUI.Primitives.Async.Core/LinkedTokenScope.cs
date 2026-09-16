// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Pairs one notification call's effective cancellation token with the linked source backing it, if one was needed, so disposing the scope releases that source.</summary>
/// <param name="Cts">The linked source to dispose, or <see langword="null"/> if no allocation was needed.</param>
/// <param name="Token">The effective cancellation token for the notification call.</param>
internal readonly record struct LinkedTokenScope(CancellationTokenSource? Cts, CancellationToken Token) : IDisposable
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Cts?.Dispose();
}
