// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Invokes a factory per subscription and subscribes the observer to the observable it returns.</summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class DeferSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The factory invoked for each subscription.</summary>
    private readonly Func<IObservable<T>> _observableFactory;

    /// <summary>Initializes a new instance of the <see cref="DeferSignal{T}"/> class.</summary>
    /// <param name="observableFactory">The factory invoked for each subscription.</param>
    public DeferSignal(Func<IObservable<T>> observableFactory) => _observableFactory = observableFactory;

    /// <summary>Reports that subscription needs no current-thread dispatch.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Subscribes the observer to the observable produced for this subscription.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that releases the subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, false, SubscribeCore);

    /// <summary>Invokes the factory and subscribes, turning a factory throw into an error notification.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The outer subscription handle.</param>
    /// <returns>The subscription to the produced observable.</returns>
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
