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
internal sealed class ConnectableSignalAsyncState<T> : IDisposable
{
    /// <summary>The asynchronous gate that serializes connection changes.</summary>
    [SuppressMessage(
        "Style",
        "SST1401:Field should be private",
        Justification = "The helper class that drives this state enters the gate directly.")]
    internal readonly AsyncSerialGate Gate = new();

    /// <summary>Guards the disposal latch so only the first caller tears the state down.</summary>
    private readonly Lock _disposalGate = new();

    /// <summary>Initializes a new instance of the <see cref="ConnectableSignalAsyncState{T}"/> class.</summary>
    /// <param name="source">The cold source sequence that is subscribed when the signal connects.</param>
    /// <param name="signal">The signal that multicasts source notifications to subscribed observers.</param>
    public ConnectableSignalAsyncState(IObservableAsync<T> source, ISignalAsync<T> signal)
    {
        Source = source;
        Signal = signal;
        DisposedCancellationToken = DisposedCts.Token;
    }

    /// <summary>Gets the cold source sequence that is subscribed when the signal connects.</summary>
    internal IObservableAsync<T> Source { get; }

    /// <summary>Gets the signal that multicasts source notifications to subscribed observers.</summary>
    internal ISignalAsync<T> Signal { get; }

    /// <summary>Gets the cancellation source that is canceled when the connectable signal is disposed.</summary>
    internal CancellationTokenSource DisposedCts { get; } = new();

    /// <summary>Gets or sets the active source subscription, if connected.</summary>
    internal SingleAssignmentDisposableAsync? Connection { get; set; }

    /// <summary>Gets or sets a value indicating whether disposal has been claimed.</summary>
    internal bool IsDisposed { get; set; }

    /// <summary>Gets the disposal token, captured at construction because <see cref="Dispose"/> makes <see cref="CancellationTokenSource.Token"/> throw.</summary>
    internal CancellationToken DisposedCancellationToken { get; }

    /// <inheritdoc/>
    public void Dispose()
    {
        Gate.Dispose();
        DisposedCts.Dispose();
    }

    /// <summary>Claims disposal for the first caller to ask.</summary>
    /// <returns><see langword="true"/> when this call owns disposal; otherwise, <see langword="false"/>.</returns>
    internal bool TryMarkDisposed()
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
}
