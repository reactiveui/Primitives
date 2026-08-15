// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
using StartSignal = ReactiveUI.Primitives.Async.Reactive.Advanced.StartSignal;

#else
using StartSignal = ReactiveUI.Primitives.Async.Advanced.StartSignal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Provides shim-typed factory methods that execute actions and emit <see cref="RxVoid"/>.</summary>
public static partial class SignalAsyncReactiveExtensions
{
    /// <summary>Factory operators that execute an action and emit <see cref="RxVoid"/>.</summary>
    /// <param name="action">The action to execute.</param>
    extension(Action action)
    {
        /// <summary>Creates an observable sequence that executes the supplied action and emits <see cref="RxVoid.Default"/>.</summary>
        /// <returns>An observable sequence that completes after the action has run.</returns>
        public IObservableAsync<RxVoid> Start()
        {
            ArgumentExceptionHelper.ThrowIfNull(action);

            return new StartSignal(action, null);
        }

        /// <summary>Creates an observable sequence that executes the supplied action and emits <see cref="RxVoid.Default"/>.</summary>
        /// <param name="taskScheduler">An optional scheduler used to start the action.</param>
        /// <returns>An observable sequence that completes after the action has run.</returns>
        public IObservableAsync<RxVoid> Start(TaskScheduler? taskScheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(action);

            return new StartSignal(action, taskScheduler);
        }
    }
}
