// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>A signal that observes one type and emits another.</summary>
/// <typeparam name="TSource">The observed value type.</typeparam>
/// <typeparam name="TResult">The emitted value type.</typeparam>
public interface ISignal<in TSource, out TResult> : IObserver<TSource>, IObservable<TResult>, IsDisposed
{
    /// <summary>Gets a value indicating whether this instance has observers.</summary>
    bool HasObservers { get; }
}
