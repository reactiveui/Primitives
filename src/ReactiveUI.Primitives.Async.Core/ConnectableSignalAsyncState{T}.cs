// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Async.Advanced;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async;

/// <summary>Stores the mutable state for a connectable signal without requiring a base class.</summary>
/// <typeparam name="T">The type of elements produced by the source sequence.</typeparam>
/// <param name="source">The cold source sequence that is subscribed when the signal connects.</param>
/// <param name="signal">The signal that multicasts source notifications to subscribed observers.</param>
internal sealed class ConnectableSignalAsyncState<T>(IObservableAsync<T> source, ISignalAsync<T> signal) : IDisposable
{
    /// <summary>The asynchronous gate that serializes connection changes.</summary>
    [SuppressMessage(
        "Style",
        "SST1401:Field should be private",
        Justification = "Gate fields are intentionally direct readonly state for helper access.")]
    public readonly AsyncSerialGate Gate = new();

    /// <summary>The monitor used to make synchronous disposal idempotent.</summary>
    private readonly Lock _disposalGate = new();

    /// <summary>Gets the cold source sequence that is subscribed when the signal connects.</summary>
    public IObservableAsync<T> Source { get; } = source;

    /// <summary>Gets the signal that multicasts source notifications to subscribed observers.</summary>
    public ISignalAsync<T> Signal { get; } = signal;

    /// <summary>Gets the cancellation source that is canceled when the connectable signal is disposed.</summary>
    public CancellationTokenSource DisposedCts { get; } = new();

    /// <summary>Gets the active source subscription, if connected.</summary>
    public SingleAssignmentDisposableAsync? Connection { get; internal set; }

    /// <summary>Gets a value indicating whether synchronous disposal has run.</summary>
    public bool IsDisposed { get; internal set; }

    /// <summary>Gets the token canceled when the connectable signal is disposed.</summary>
    public CancellationToken DisposedCancellationToken => DisposedCts.Token;

    /// <summary>Marks the state as disposed if disposal has not already started.</summary>
    /// <returns><see langword="true"/> when this call owns disposal; otherwise, <see langword="false"/>.</returns>
    public bool TryMarkDisposed()
    {
        lock (_disposalGate)
        {
            if (IsDisposed)
            {
                return false;
            }

            IsDisposed = true;
            return true;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Gate.Dispose();
        DisposedCts.Dispose();
    }
}
