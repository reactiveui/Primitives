// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals.Core;

internal interface IInlineSignal<T> : IObservable<T>
{
    IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted);
}
