// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Represents the IInlineSignal interface.</summary>
/// <typeparam name="T">The T type.</typeparam>
public interface IInlineSignal<T> : IObservable<T>
{
    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="onNext">The onNext value.</param>
    /// <param name="onError">The onError value.</param>
    /// <param name="onCompleted">The onCompleted value.</param>
    /// <returns>The result.</returns>
    IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted);
}
