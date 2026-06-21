// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
using FromAsyncSignal = ReactiveUI.Primitives.Async.Reactive.Advanced.FromAsyncSignal;
#else
using FromAsyncSignal = ReactiveUI.Primitives.Async.Advanced.FromAsyncSignal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Provides factory methods for creating shim-typed observables from asynchronous operations.</summary>
public static partial class SignalAsyncReactiveExtensions
{
    /// <summary>
    /// Creates an asynchronous observable sequence that executes the specified factory function and signals completion
    /// when the operation finishes.
    /// </summary>
    /// <remarks>The returned observable executes the factory function as a background job. The sequence emits
    /// <see cref="RxVoid"/> after the factory completes and then signals completion. Cancellation is supported
    /// via the provided token.</remarks>
    /// <param name="factory">A function that performs the asynchronous operation. The function receives a cancellation token that can be used
    /// to cancel the operation.</param>
    /// <returns>An observable sequence that emits a single value of <see cref="RxVoid"/> when the factory function completes,
    /// followed by a completion notification.</returns>
    /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="factory"/> is <see langword="null"/>.</exception>
    [SuppressMessage(
        "Roslynator",
        "RCS1047:Non-asynchronous method name should not end with \'Async\'",
        Justification = "This is an existing method")]
    public static IObservableAsync<RxVoid> FromAsync(Func<CancellationToken, ValueTask> factory)
    {
        ArgumentExceptionHelper.ThrowIfNull(factory);

        return new FromAsyncSignal(factory);
    }
}
