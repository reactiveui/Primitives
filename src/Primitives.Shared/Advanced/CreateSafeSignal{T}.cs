// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Creates a signal from a subscribe delegate whose sink releases the subscription when a downstream <c>OnNext</c> throws.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CreateSafeSignal<{typeof(T).Name,nq}>")]
public sealed class CreateSafeSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The delegate invoked for each subscription.</summary>
    private readonly Func<IObserver<T>, IDisposable> _subscribe;

    /// <summary>Whether subscription must be dispatched through the current-thread sequencer.</summary>
    private readonly bool _isRequiredSubscribeOnCurrentThread;

    /// <summary>Initializes a new instance of the <see cref="CreateSafeSignal{T}"/> class.</summary>
    /// <param name="subscribe">The delegate invoked for each subscription.</param>
    public CreateSafeSignal(Func<IObserver<T>, IDisposable> subscribe) => _subscribe = subscribe;

    /// <summary>Initializes a new instance of the <see cref="CreateSafeSignal{T}"/> class.</summary>
    /// <param name="subscribe">The delegate invoked for each subscription.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">Whether subscription must be dispatched through the current-thread sequencer.</param>
    public CreateSafeSignal(Func<IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
    {
        _subscribe = subscribe;
        _isRequiredSubscribeOnCurrentThread = isRequiredSubscribeOnCurrentThread;
    }

    /// <summary>Projects <paramref name="source"/> into a <see cref="ReadOnlyState{T}"/> backed by a safe signal.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="initialValue">The initial projected value.</param>
    /// <param name="selector">The projection function.</param>
    /// <returns>A read-only projected state.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static ReadOnlyState<T> CreateReadOnlyState<TSource>(
        IObservable<TSource> source,
        T initialValue,
        Func<TSource, T> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        return new(
            new CreateSafeSignal<T>(observer => source.Subscribe(
                value => observer.OnNext(selector(value)),
                observer.OnError,
                observer.OnCompleted)),
            initialValue);
    }

    /// <summary>Reports whether subscription must be dispatched through the current-thread sequencer.</summary>
    /// <returns><see langword="true"/> when current-thread dispatch is required.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => _isRequiredSubscribeOnCurrentThread;

    /// <summary>Invokes the subscribe delegate with a sink wrapping the observer.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that releases the subscription.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_isRequiredSubscribeOnCurrentThread)
        {
            return SignalSubscription.Subscribe(observer, true, SubscribeCore);
        }

        CreateSink<T> sink = new(observer, true);
        sink.SetCancel(_subscribe(sink) ?? EmptyDisposable.Instance);
        return sink;
    }

    /// <summary>Invokes the subscribe delegate with a sink that owns <paramref name="cancel"/>.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The outer subscription handle.</param>
    /// <returns>The disposable returned by the subscribe delegate.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        CreateSink<T> sink = new(observer, cancel, true);
        return _subscribe(sink) ?? EmptyDisposable.Instance;
    }
}
