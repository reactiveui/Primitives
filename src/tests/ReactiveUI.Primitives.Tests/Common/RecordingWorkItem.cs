// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>A cancellable work item that counts its executions.</summary>
internal sealed class RecordingWorkItem : IWorkItem, IsDisposed
{
    /// <summary>The number of executions.</summary>
    private int _executeCount;

    /// <summary>Non-zero after cancellation.</summary>
    private int _isDisposed;

    /// <inheritdoc/>
    public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

    /// <summary>Gets the number of times the item executed.</summary>
    internal int ExecuteCount => Volatile.Read(ref _executeCount);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Volatile.Write(ref _isDisposed, 1);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Execute() => Interlocked.Increment(ref _executeCount);
}
