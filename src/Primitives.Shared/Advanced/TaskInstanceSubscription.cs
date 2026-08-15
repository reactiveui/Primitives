// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Subscription handle for an existing task-instance signal.</summary>
[System.Diagnostics.DebuggerDisplay("Stopped = {_stopped}")]
public sealed class TaskInstanceSubscription : IDisposable
{
    /// <summary>Non-zero after disposal or terminal notification.</summary>
    private int _stopped;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Interlocked.Exchange(ref _stopped, 1);

    /// <summary>Attempts to mark the subscription as terminal.</summary>
    /// <returns><see langword="true"/> when this call won the terminal transition.</returns>
    public bool TryStop() => Interlocked.Exchange(ref _stopped, 1) == 0;
}
