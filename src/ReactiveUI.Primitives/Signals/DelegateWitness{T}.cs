// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// A lightweight <see cref="IObserver{T}"/> that forwards each notification to the supplied delegates.
/// Use it to subscribe to an <see cref="IObservable{T}"/> without allocating a bespoke observer class.
/// The <paramref name="onError"/> and <paramref name="onCompleted"/> delegates are optional; when omitted
/// the corresponding terminal notification is ignored.
/// </summary>
/// <typeparam name="T">The type of the value being observed.</typeparam>
/// <param name="onNext">The delegate invoked with each value pushed to <see cref="IObserver{T}.OnNext"/>.</param>
/// <param name="onError">The optional delegate invoked with the exception when the sequence faults via <see cref="IObserver{T}.OnError"/>.</param>
/// <param name="onCompleted">The optional delegate invoked when the sequence finishes via <see cref="IObserver{T}.OnCompleted"/>.</param>
public sealed class DelegateWitness<T>(
    Action<T> onNext,
    Action<Exception>? onError = null,
    Action? onCompleted = null) : IObserver<T>
{
    private readonly Action<T> _onNext = onNext ?? throw new ArgumentNullException(nameof(onNext));

    /// <inheritdoc/>
    public void OnNext(T value) => _onNext(value);

    /// <inheritdoc/>
    public void OnError(Exception error) => onError?.Invoke(error);

    /// <inheritdoc/>
    public void OnCompleted() => onCompleted?.Invoke();
}
