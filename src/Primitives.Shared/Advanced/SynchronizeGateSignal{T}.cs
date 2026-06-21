// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that serializes source notifications behind a shared gate.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class SynchronizeGateSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="SynchronizeGateSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="gate">The gate shared across subscriptions and other synchronized sequences.</param>
    public SynchronizeGateSignal(IObservable<T> source, Lock gate)
    {
        Source = source;
        Gate = gate;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <summary>Gets the shared gate.</summary>
    private Lock Gate { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SynchronizeWitness<T> sink = new(observer, Gate);
        sink.SetSubscription(Source.Subscribe(sink));
        return sink;
    }
}
