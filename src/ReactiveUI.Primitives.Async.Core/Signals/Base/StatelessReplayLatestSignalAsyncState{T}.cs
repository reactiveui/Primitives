// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Mutable state for a stateless replay-latest async signal.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <param name="InitialValue">The initial value to replay, if any.</param>
[SuppressMessage(
    "Style",
    "SST1802:Replace set accessor with init",
    Justification = "This record is the mutable state container for the flat helper implementation.")]
internal sealed record StatelessReplayLatestSignalAsyncState<T>(Optional<T> InitialValue) : IDisposable
{
    /// <summary>The asynchronous gate used to synchronize mutable state.</summary>
    [SuppressMessage(
        "Style",
        "SST1401:Field should be private",
        Justification = "Gate fields are intentionally direct readonly state for helper access.")]
    public readonly AsyncSerialGate Gate = new();

    /// <summary>Gets the cancellation token source that is cancelled when this instance is disposed.</summary>
    public CancellationTokenSource DisposedCts { get; } = new();

    /// <summary>Gets the most recently published value, or the initial value after reset.</summary>
    public Optional<T> Value { get; internal set; } = InitialValue;

    /// <summary>Gets the currently subscribed observers.</summary>
    public ImmutableArray<IObserverAsync<T>> Observers { get; internal set; } = [];

    /// <summary>Gets a value indicating whether this instance has been disposed.</summary>
    public bool IsDisposed { get; internal set; }

    /// <inheritdoc/>
    public void Dispose()
    {
        Gate.Dispose();
        DisposedCts.Dispose();
    }
}
