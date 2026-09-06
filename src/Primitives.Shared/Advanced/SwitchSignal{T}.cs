// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mirrors only the latest inner observable from an outer source.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SwitchSignal: Sources = {Sources}")]
public sealed class SwitchSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="SwitchSignal{T}"/> class.</summary>
    /// <param name="sources">The outer source of inner observables.</param>
    public SwitchSignal(IObservable<IObservable<T>> sources) => Sources = sources;

    /// <summary>Gets the outer source of inner observables.</summary>
    private IObservable<IObservable<T>> Sources { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new SwitchWitness<T>(observer).Run(Sources);
    }
}
