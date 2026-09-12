// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Signal that emits <see langword="false"/> and completes synchronously inside <c>Subscribe</c>.</summary>
public sealed class ImmutableReturnFalseSignal : IRequireCurrentThread<bool>, IInlineSignal<bool>
{
    /// <summary>The shared instance; the signal carries no per-subscription state.</summary>
    public static readonly ImmutableReturnFalseSignal Instance = new();

    /// <summary>Initializes a new instance of the <see cref="ImmutableReturnFalseSignal"/> class.</summary>
    private ImmutableReturnFalseSignal()
    {
    }

    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Emits <see langword="false"/> to <paramref name="observer"/> and completes it before returning.</summary>
    /// <param name="observer">The observer to notify.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        observer.OnNext(false);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Invokes <paramref name="onNext"/> with <see langword="false"/>, then <paramref name="onCompleted"/>.</summary>
    /// <param name="onNext">Invoked with <see langword="false"/>.</param>
    /// <param name="onError">Never invoked.</param>
    /// <param name="onCompleted">Invoked after the value.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    public IDisposable Subscribe(Action<bool> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(false);
        onCompleted();
        return EmptyDisposable.Instance;
    }
}
