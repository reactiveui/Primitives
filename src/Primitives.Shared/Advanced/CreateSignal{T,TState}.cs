// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Creates a signal from a subscribe delegate that receives a caller-supplied state value.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TState">The state type handed to the subscribe delegate.</typeparam>
[System.Diagnostics.DebuggerDisplay("CreateSignal<{typeof(T).Name,nq},{typeof(TState).Name,nq}>")]
public sealed class CreateSignal<T, TState> : IRequireCurrentThread<T>
{
    /// <summary>The state handed to the subscribe delegate.</summary>
    private readonly TState _state;

    /// <summary>The delegate invoked for each subscription.</summary>
    private readonly Func<TState, IObserver<T>, IDisposable> _subscribe;

    /// <summary>Whether subscription must be dispatched through the current-thread sequencer.</summary>
    private readonly bool _currentThreadRequired;

    /// <summary>Initializes a new instance of the <see cref="CreateSignal{T,TState}"/> class.</summary>
    /// <param name="state">The state handed to the subscribe delegate.</param>
    /// <param name="subscribe">The delegate invoked for each subscription.</param>
    public CreateSignal(TState state, Func<TState, IObserver<T>, IDisposable> subscribe)
    {
        _state = state;
        _subscribe = subscribe;
    }

    /// <summary>Initializes a new instance of the <see cref="CreateSignal{T,TState}"/> class.</summary>
    /// <param name="state">The state handed to the subscribe delegate.</param>
    /// <param name="subscribe">The delegate invoked for each subscription.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">Whether subscription must be dispatched through the current-thread sequencer.</param>
    public CreateSignal(
        TState state,
        Func<TState, IObserver<T>, IDisposable> subscribe,
        bool isRequiredSubscribeOnCurrentThread)
    {
        _state = state;
        _subscribe = subscribe;
        _currentThreadRequired = isRequiredSubscribeOnCurrentThread;
    }

    /// <summary>Reports whether subscription must be dispatched through the current-thread sequencer.</summary>
    /// <returns><see langword="true"/> when current-thread dispatch is required.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => _currentThreadRequired;

    /// <summary>Invokes the subscribe delegate with the state and a wrapper around the observer.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that releases the subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _currentThreadRequired
            ? SubscriptionScheduling.OnCurrentThread(
                (Self: this, observer),
                static s => s.Self.SubscribeDirect(s.observer))
            : SubscribeDirect(observer);
    }

    /// <summary>Subscribes directly using a single <see cref="CreateSink{T}"/>.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription sink.</returns>
    private CreateSink<T> SubscribeDirect(IObserver<T> observer)
    {
        CreateSink<T> sink = new(observer, false);
        sink.SetCancel(_subscribe(_state, sink) ?? EmptyDisposable.Instance);
        return sink;
    }
}
