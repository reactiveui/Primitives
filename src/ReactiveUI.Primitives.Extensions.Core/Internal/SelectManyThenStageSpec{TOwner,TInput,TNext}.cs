// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>Groups the stage projection, null-result error message, and callbacks for one stage of <c>SelectManyThen</c>.</summary>
/// <typeparam name="TOwner">The owning coordinator type.</typeparam>
/// <typeparam name="TInput">The stage input element type.</typeparam>
/// <typeparam name="TNext">The stage output element type.</typeparam>
/// <param name="Selector">Projects an input value into the stage's observable sequence.</param>
/// <param name="NullProjectionMessage">Exception message raised when <paramref name="Selector"/> returns <see langword="null"/>.</param>
/// <param name="OnNext">Forwards each element emitted by the stage.</param>
/// <param name="OnError">Forwards a terminal error from the stage.</param>
/// <param name="OnStageCompleted">Detaches a completed stage subscription from the coordinator.</param>
internal readonly record struct SelectManyThenStageSpec<TOwner, TInput, TNext>(
    Func<TInput, IObservable<TNext>> Selector,
    string NullProjectionMessage,
    Action<TOwner, TNext> OnNext,
    Action<TOwner, Exception> OnError,
    Action<TOwner, IDisposable> OnStageCompleted);
