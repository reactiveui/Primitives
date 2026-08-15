// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Merges enumerable observable sources concurrently.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Sources = {Sources}")]
public sealed class EnumerableBlendSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="EnumerableBlendSignal{T}"/> class.</summary>
    /// <param name="sources">The sources to merge.</param>
    public EnumerableBlendSignal(IEnumerable<IObservable<T>> sources) => Sources = sources;

    /// <summary>Gets the sources to merge.</summary>
    private IEnumerable<IObservable<T>> Sources { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new BlendWitness<T>(observer).Run(Sources);
    }
}
