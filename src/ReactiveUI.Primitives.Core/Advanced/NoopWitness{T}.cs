// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>No-op observer used by <c>SubscribeAndComplete</c> to absorb signals without allocating a delegate trio.</summary>
/// <typeparam name="T">The element type of the source.</typeparam>
[System.Diagnostics.DebuggerDisplay("NoopWitness<{typeof(T).Name,nq}>")]
public sealed class NoopWitness<T> : IObserver<T>
{
    /// <summary>Singleton instance to avoid per-call allocation.</summary>
    public static readonly NoopWitness<T> Instance = new();

    /// <summary>Initializes a new instance of the <see cref="NoopWitness{T}"/> class.</summary>
    private NoopWitness()
    {
    }

    /// <summary>Subscribes to a synchronously terminating <paramref name="source"/> and disposes the subscription immediately.</summary>
    /// <param name="source">The observable to subscribe to.</param>
    public static void SubscribeAndComplete(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        using var subscription = source.Subscribe(Instance);
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
    }
}
