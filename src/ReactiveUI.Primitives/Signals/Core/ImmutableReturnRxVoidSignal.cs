// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the ImmutableReturnRxVoidSignal class.
/// </summary>
internal sealed class ImmutableReturnRxVoidSignal : IRequireCurrentThread<RxVoid>, IInlineSignal<RxVoid>
{
#pragma warning disable SA1401 // Fields should be private

    /// <summary>
    /// Executes the new operation.
    /// </summary>
    /// <returns>The result.</returns>
    internal static ImmutableReturnRxVoidSignal Instance = new();
#pragma warning restore SA1401 // Fields should be private

    /// <summary>
    /// Initializes a new instance of the <see cref="ImmutableReturnRxVoidSignal"/> class.
    /// </summary>
    private ImmutableReturnRxVoidSignal()
    {
    }

    /// <summary>
    /// Executes the IsRequiredSubscribeOnCurrentThread operation.
    /// </summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>
    /// Executes the Subscribe operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        observer.OnNext(RxVoid.Default);
        observer.OnCompleted();
        return Disposable.Empty;
    }

    /// <summary>
    /// Executes the Subscribe operation.
    /// </summary>
    /// <param name="onNext">The onNext value.</param>
    /// <param name="onError">The onError value.</param>
    /// <param name="onCompleted">The onCompleted value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(Action<RxVoid> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(RxVoid.Default);
        onCompleted();
        return Disposable.Empty;
    }
}
