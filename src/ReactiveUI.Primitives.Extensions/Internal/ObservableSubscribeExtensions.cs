// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>
/// Internal subscribe helpers that adapt delegate triples to a proper observer.
/// </summary>
internal static class ObservableSubscribeExtensions
{
    /// <summary>
    /// Subscribes using delegate callbacks for OnNext / OnError / OnCompleted. Unique name to
    /// avoid the System.Reactive <c>Subscribe(onNext, onError, onCompleted)</c> ambiguity; the
    /// delegates are wrapped by the core <see cref="SubscribeMixins"/> sink rather than a
    /// duplicated observer.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">Per-value callback.</param>
    /// <param name="onError">Error callback.</param>
    /// <param name="onCompleted">Completion callback.</param>
    /// <returns>The subscription disposable.</returns>
    public static IDisposable SubscribeCallbacks<T>(
        this IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted) =>
        source.Subscribe(onNext, onError, onCompleted);
}
