// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Object-gated <c>Synchronize</c> compatibility signal.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="gate">The gate shared across subscriptions and other synchronized sequences.</param>
[System.Diagnostics.DebuggerDisplay("Source = {_source}, Gate = {_gate}")]
public sealed class SynchronizeObjectSignal<T>(IObservable<T> source, object gate) : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The shared gate.</summary>
    private readonly object _gate = gate;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SynchronizeObjectWitness<T> sink = new(observer, _gate);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
