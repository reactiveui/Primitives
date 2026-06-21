// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for creating asynchronous observables from asynchronous operations.</summary>
/// <remarks>The methods in this class allow integration of asynchronous tasks or functions into the observable
/// pattern, enabling consumers to subscribe to results produced by asynchronous operations. All created observables
/// execute the provided asynchronous factory as a background job and emit the result to subscribers upon
/// completion.</remarks>
public static partial class SignalAsync
{
    /// <summary>Creates an asynchronous observable sequence that emits a single value produced by the specified factory function.</summary>
    /// <remarks>The observable sequence will emit the value produced by the factory and then signal
    /// completion. The factory function is invoked when the sequence is subscribed to, and supports cancellation via
    /// the provided <see cref="CancellationToken"/>.</remarks>
    /// <typeparam name="T">The type of the value produced by the factory and emitted by the observable sequence.</typeparam>
    /// <param name="factory">A function that asynchronously produces a value of type <typeparamref name="T"/> when invoked with a <see
    /// cref="CancellationToken"/>. Cannot be null.</param>
    /// <returns>An observable that emits the value returned by the factory function and then completes.</returns>
    /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="factory"/> is null.</exception>
    [SuppressMessage(
        "Roslynator",
        "RCS1047:Non-asynchronous method name should not end with \'Async\'",
        Justification = "This is an existing method")]
    public static IObservableAsync<T> FromAsync<T>(Func<CancellationToken, ValueTask<T>> factory)
    {
        ArgumentExceptionHelper.ThrowIfNull(factory);

        return CreateAsBackgroundJob<T>(
            async (obs, token) =>
            {
                var result = await factory(token).ConfigureAwait(false);
                await obs.OnNextAsync(result, token).ConfigureAwait(false);
                await obs.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            },
            true);
    }
}
