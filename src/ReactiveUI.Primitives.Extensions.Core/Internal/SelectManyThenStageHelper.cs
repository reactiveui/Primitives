// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>Projects and subscribes one stage of <c>SelectManyThen</c> using a <see cref="SelectManyThenStageSpec{TOwner, TInput, TNext}"/>.</summary>
internal static class SelectManyThenStageHelper
{
    /// <summary>Projects <paramref name="input"/> via <paramref name="spec"/> and attaches a <see cref="SelectManyThenStageArm{TOwner, TItem}"/> to <paramref name="subscriptions"/>.</summary>
    /// <typeparam name="TOwner">The coordinator type.</typeparam>
    /// <typeparam name="TInput">The input element type.</typeparam>
    /// <typeparam name="TNext">The stage sequence element type.</typeparam>
    /// <param name="owner">The owning coordinator.</param>
    /// <param name="input">The incoming value.</param>
    /// <param name="isDone">Whether the coordinator has already terminated.</param>
    /// <param name="subscriptions">The mutable subscription set owned by the coordinator.</param>
    /// <param name="activeCount">The active sequence counter incremented before subscribing.</param>
    /// <param name="spec">The stage projection and callback specification.</param>
    internal static void SubscribeProjectedStage<TOwner, TInput, TNext>(
        TOwner owner,
        TInput input,
        bool isDone,
        ref DisposableSet subscriptions,
        ref int activeCount,
        in SelectManyThenStageSpec<TOwner, TInput, TNext> spec)
    {
        if (isDone || subscriptions.IsDisposed)
        {
            return;
        }

        IObservable<TNext> nextSequence;
        try
        {
            nextSequence = spec.Selector(input);
        }
        catch (Exception error)
        {
            spec.OnError(owner, error);
            return;
        }

        if (nextSequence is null)
        {
            spec.OnError(owner, new InvalidOperationException(spec.NullProjectionMessage));
            return;
        }

        _ = Interlocked.Increment(ref activeCount);
        SelectManyThenStageArm<TOwner, TNext> arm = new(owner, spec.OnNext, spec.OnError, spec.OnStageCompleted);
        subscriptions.Add(arm);
        if (!arm.IsDisposed)
        {
            arm.Attach(nextSequence.Subscribe(arm));
        }
    }
}
