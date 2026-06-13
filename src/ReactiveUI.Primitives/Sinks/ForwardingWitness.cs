// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer that forwards notifications to a standard observer.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
public sealed class ForwardingWitness<T> : IObserver<T>
{
    /// <summary>Wrapped observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Initializes a new instance of the <see cref="ForwardingWitness{T}"/> class.</summary>
    /// <param name="observer">Wrapped observer.</param>
    public ForwardingWitness(IObserver<T> observer) => _observer = observer ?? throw new ArgumentNullException(nameof(observer));

    /// <inheritdoc/>
    public void OnCompleted() => _observer.OnCompleted();

    /// <inheritdoc/>
    public void OnError(Exception error) => _observer.OnError(error);

    /// <inheritdoc/>
    public void OnNext(T value) => _observer.OnNext(value);
}
