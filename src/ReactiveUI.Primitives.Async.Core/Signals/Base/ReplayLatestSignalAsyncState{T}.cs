// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Mutable state for a replay-latest async signal.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
internal sealed class ReplayLatestSignalAsyncState<T> : IDisposable
{
    /// <summary>The asynchronous gate used to synchronize mutable state.</summary>
    [SuppressMessage(
        "Style",
        "SST1401:Field should be private",
        Justification = "Gate fields are intentionally direct readonly state for helper access.")]
    internal readonly AsyncSerialGate Gate = new();

    /// <summary>Initializes a new instance of the <see cref="ReplayLatestSignalAsyncState{T}"/> class.</summary>
    /// <param name="initialValue">The initial value to replay, if any.</param>
    public ReplayLatestSignalAsyncState(Optional<T> initialValue)
    {
        LastValue = initialValue;
        DisposedCancellationToken = DisposedCts.Token;
    }

    /// <summary>Gets the cancellation token source that is cancelled when this instance is disposed.</summary>
    internal CancellationTokenSource DisposedCts { get; } = new();

    /// <summary>Gets the token cancelled when this instance is disposed. Captured while the source is still
    /// alive because <see cref="Dispose"/> disposes that source, and reading
    /// <see cref="CancellationTokenSource.Token"/> from a disposed source throws
    /// <see cref="ObjectDisposedException"/>. Disposal always cancels before it disposes, so this token is
    /// already cancelled by the time anyone can observe it post-disposal.</summary>
    internal CancellationToken DisposedCancellationToken { get; }

    /// <summary>Gets or sets the most recently published value, replayed to new subscribers upon subscription.</summary>
    internal Optional<T> LastValue { get; set; }

    /// <summary>Gets or sets the currently subscribed observers.</summary>
    internal ImmutableArray<IObserverAsync<T>> Observers { get; set; } = [];

    /// <summary>Gets or sets the completion result, or null if the signal has not completed.</summary>
    internal Result? Result { get; set; }

    /// <summary>Gets or sets a value indicating whether this instance has been disposed.</summary>
    internal bool IsDisposed { get; set; }

    /// <inheritdoc/>
    public void Dispose()
    {
        Gate.Dispose();
        DisposedCts.Dispose();
    }
}
