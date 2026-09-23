// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Owns cancellation for one context start generation.</summary>
internal sealed class StartGeneration : IDisposable
{
    /// <summary>Stores the source owned by this generation.</summary>
    private readonly CancellationTokenSource _source = new();

    /// <summary>Stores the shared cancellation completion.</summary>
    private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Gets the shared cancellation completion.</summary>
    internal Task CancellationTask => _completion.Task;

    /// <summary>Gets whether stop intent was captured under the owning gate.</summary>
    internal bool StopRequested { get; private set; }

    /// <summary>Gets the token canceled by stop or dispose.</summary>
    internal CancellationToken Token => _source.Token;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _source.Dispose();

    /// <summary>Records cancellation intent while the owning gate is held.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordStop() => StopRequested = true;

    /// <summary>Cancels the generation and completes its shared wait.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Cancel()
    {
        _source.Cancel();
        _ = _completion.TrySetResult(true);
    }
}
