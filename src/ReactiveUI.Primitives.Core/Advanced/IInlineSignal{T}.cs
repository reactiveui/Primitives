// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>A signal that accepts callbacks directly, so a subscriber needs no intermediate observer.</summary>
/// <typeparam name="T">The value type.</typeparam>
public interface IInlineSignal<T> : IObservable<T>
{
    /// <summary>Subscribes the supplied callbacks to the signal.</summary>
    /// <param name="onNext">Invoked for each value.</param>
    /// <param name="onError">Invoked with the terminal error.</param>
    /// <param name="onCompleted">Invoked when the signal completes.</param>
    /// <returns>A handle that detaches the callbacks when disposed.</returns>
    IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted);
}
