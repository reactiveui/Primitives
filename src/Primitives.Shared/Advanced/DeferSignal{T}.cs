// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Represents the DeferSignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class DeferSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Func<IObservable<T>> _observableFactory;

    /// <summary>Initializes a new instance of the <see cref="DeferSignal{T}"/> class.</summary>
    /// <param name="observableFactory">The observableFactory value.</param>
    public DeferSignal(Func<IObservable<T>> observableFactory) => _observableFactory = observableFactory;

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, false, SubscribeCore);

    /// <summary>Executes the SubscribeCore operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new GuardedWitness<T>(observer, cancel);

        IObservable<T> source;
        try
        {
            source = _observableFactory();
        }
        catch (Exception ex)
        {
            source = Signal.Fail<T>(ex);
        }

        return source.Subscribe(observer);
    }
}
