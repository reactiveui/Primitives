// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Forwards notifications to supplied delegates and ignores terminal notifications whose delegate is omitted.</summary>
/// <typeparam name="T">The type of the value being observed.</typeparam>
/// <param name="onNext">The delegate invoked with each value pushed to <see cref="IObserver{T}.OnNext"/>.</param>
/// <param name="onError">The optional delegate invoked with the exception when the sequence faults via <see cref="IObserver{T}.OnError"/>.</param>
/// <param name="onCompleted">The optional delegate invoked when the sequence finishes via <see cref="IObserver{T}.OnCompleted"/>.</param>
[System.Diagnostics.DebuggerDisplay("DelegateWitness: OnNext = {_onNext}")]
public sealed class DelegateWitness<T>(
    Action<T> onNext,
    Action<Exception>? onError = null,
    Action? onCompleted = null) : IObserver<T>
{
    /// <summary>The delegate invoked for each observed value.</summary>
    private readonly Action<T> _onNext = onNext ?? throw new ArgumentNullException(nameof(onNext));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => _onNext(value);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => onError?.Invoke(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => onCompleted?.Invoke();
}
