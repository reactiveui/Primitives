// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Disposables;

/// <summary>Owns one asynchronous disposable at a time, replacing it on assignment.</summary>
/// <remarks>Assignments await disposal of the replaced resource; assignments after disposal release their incoming resource.
/// Concurrent calls are supported.</remarks>
[System.Diagnostics.DebuggerDisplay("SingleReplaceableDisposableAsync: Current = {_current}")]
public class SingleReplaceableDisposableAsync : IAsyncDisposable
{
    /// <summary>The tracked disposable, or the sentinel that marks the slot closed.</summary>
    private IAsyncDisposable? _current;

    /// <summary>Takes over from the tracked resource, disposing the one it displaces - or disposing <paramref name="value"/> itself when this instance has been disposed.</summary>
    /// <param name="value">The new <see cref="IAsyncDisposable"/> instance to track. Can be <see langword="null"/> to clear the current
    /// resource.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once whichever resource this call gave up has been
    /// disposed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask SetDisposableAsync(IAsyncDisposable? value) =>
        DisposableAsyncSlot.SwapAsync(ref _current, value);

    /// <summary>Disposes the tracked resource and closes the slot.</summary>
    /// <returns>A <see cref="ValueTask"/> that completes once the tracked resource has been disposed.</returns>
    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return DisposableAsyncSlot.DisposeAsync(ref _current);
    }
}
