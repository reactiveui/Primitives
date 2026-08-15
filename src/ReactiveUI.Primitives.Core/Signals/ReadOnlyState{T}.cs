// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Read-only latest-value signal for projected or externally owned state.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ReadOnlyState<T> : IObservable<T>, IDisposable
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly StateSignal<T> _inner;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly IDisposable _subscription;

    /// <summary>Initializes a new instance of the <see cref="ReadOnlyState{T}"/> class.</summary>
    /// <param name="source">The source values to mirror.</param>
    /// <param name="initialValue">The current value before source notifications arrive.</param>
    public ReadOnlyState(IObservable<T> source, T initialValue)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _inner = new(initialValue);
        _subscription = source.Subscribe(_inner);
    }

    /// <summary>Gets the current value.</summary>
    public T Value => _inner.Value;

    /// <summary>Gets the stream of current and subsequent values.</summary>
    public IObservable<T> Changed => _inner;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Notifies the provider that an observer is to receive notifications.</summary>
    /// <param name="observer">The object that is to receive notifications.</param>
    /// <returns>A reference to an interface that allows observers to stop receiving notifications before the provider has
    /// finished sending them.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) => _inner.Subscribe(observer);

    /// <summary>Executes the Dispose operation.</summary>
    public void Dispose()
    {
        _subscription.Dispose();
        _inner.Dispose();
    }
}
