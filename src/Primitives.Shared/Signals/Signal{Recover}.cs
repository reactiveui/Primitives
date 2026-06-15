// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Provides static factory and operator methods for signals.</summary>
public static partial class Signal
{
    /// <summary>Continues an observable sequence that is terminated by an exception with the next observable sequence.</summary>
    /// <typeparam name="TSource">The type of the elements in the source and handler sequences.</typeparam>
    /// <param name="sources">Observable sequences to catch exceptions for.</param>
    /// <returns>An observable sequence containing elements from consecutive source sequences until a source sequence terminates successfully.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="sources"/> is null.</exception>
    public static IObservable<TSource> Recover<TSource>(params IObservable<TSource>[] sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return new CatchSignal<TSource>(sources);
    }
}
