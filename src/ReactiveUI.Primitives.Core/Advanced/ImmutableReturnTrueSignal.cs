// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Represents the ImmutableReturnTrueSignal class.</summary>
public sealed class ImmutableReturnTrueSignal : IRequireCurrentThread<bool>, IInlineSignal<bool>
{
    /// <summary>Executes the new operation.</summary>
    /// <returns>The result.</returns>
    public static readonly ImmutableReturnTrueSignal Instance = new();

    /// <summary>Initializes a new instance of the <see cref="ImmutableReturnTrueSignal"/> class.</summary>
    private ImmutableReturnTrueSignal()
    {
    }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        observer.OnNext(true);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="onNext">The onNext value.</param>
    /// <param name="onError">The onError value.</param>
    /// <param name="onCompleted">The onCompleted value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(Action<bool> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(true);
        onCompleted();
        return EmptyDisposable.Instance;
    }
}
