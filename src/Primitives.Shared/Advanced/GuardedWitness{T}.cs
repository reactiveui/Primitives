// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>
/// Forwarding observer that releases its upstream cancel resource when the sequence terminates or a downstream
/// <c>OnNext</c> throws. The shared guard behind the scheduled factory signals (Empty, Return, Throw, Defer),
/// usable by any signal implementation that needs terminate-and-release semantics around a downstream observer.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("GuardedWitness: Disposed = {_disposed}, Observer = {_observer}")]
public sealed class GuardedWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>Stores the downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Stores the upstream subscription.</summary>
    private IDisposable? _cancel;

    /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="GuardedWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The upstream cancel resource released on termination.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="cancel"/> is <see langword="null"/>.</exception>
    public GuardedWitness(IObserver<T> observer, IDisposable cancel)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        _cancel = cancel ?? throw new ArgumentNullException(nameof(cancel));
        _observer = observer;
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        try
        {
            _observer.OnNext(value);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        try
        {
            _observer.OnError(error);
        }
        finally
        {
            Dispose();
        }
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        try
        {
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => WitnessTeardown.Dispose(ref _disposed, ref _cancel);
}
