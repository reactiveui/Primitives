// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for creating asynchronous observables from asynchronous operations.</summary>
public static partial class SignalAsync
{
    /// <summary>Creates an asynchronous observable sequence that emits a single value produced by the specified factory function.</summary>
    /// <typeparam name="T">The type of the value produced by the factory and emitted by the observable sequence.</typeparam>
    /// <param name="factory">A function that asynchronously produces a value of type <typeparamref name="T"/> when invoked with a <see
    /// cref="CancellationToken"/>. Cannot be null.</param>
    /// <returns>An observable that emits the value returned by the factory function and then completes.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="factory"/> is null.</exception>
    /// <remarks><paramref name="factory"/> runs once per subscriber, started by the subscribe call rather than awaited
    /// by it.</remarks>
    public static IObservableAsync<T> FromAsync<T>(Func<CancellationToken, ValueTask<T>> factory)
    {
        ArgumentExceptionHelper.ThrowIfNull(factory);

        return new FromAsyncSignal<T>(factory);
    }
}
