// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if PRIMITIVES_CORE
namespace ReactiveUI.Primitives.Core.Internal;
#elif REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Pairs a caller-supplied state value with a delegate so synchronous emission helpers run without closures.</summary>
/// <typeparam name="TState">The caller-supplied state type.</typeparam>
/// <typeparam name="TDelegate">The delegate type invoked with the state.</typeparam>
/// <param name="State">The caller-supplied state value.</param>
/// <param name="Callback">The delegate invoked for each element.</param>
internal readonly record struct StateCallbackPair<TState, TDelegate>(TState State, TDelegate Callback);
