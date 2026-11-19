// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <see cref="SelectManyThenStageHelper"/>, <see cref="SelectManyThenStageSpec{TOwner, TInput, TNext}"/>, and <see cref="SelectManyThenStageArm{TOwner, TItem}"/>.</summary>
public sealed class SelectManyThenStageHelperTests
{
    /// <summary>Expected active or error count of two.</summary>
    private const int Two = 2;

    /// <summary>Multiplier for stage projection.</summary>
    private const int Multiplier = 4;

    /// <summary>Input value.</summary>
    private const int InputValue = 5;

    /// <summary>Null-projection error message.</summary>
    private const string NullStageMessage = "Stage selector returned null.";

    /// <summary>
    /// Verifies <see cref="SelectManyThenStageHelper.SubscribeProjectedStage{TOwner, TInput, TNext}"/> subscribes projected
    /// sequences, tracks active count, and removes completed arms.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SubscribeProjectedStage_ForwardsItemsAndDetachesOnSingleCompletion()
    {
        using StageHarness harness = new();
        var activeCount = 1;
        Subject<int> stageSubject = new();

        SelectManyThenStageSpec<StageHarness, int, int> spec = new(
            _ => stageSubject,
            NullStageMessage,
            static (owner, item) => owner.Items.Add(item),
            static (owner, error) => owner.Errors.Add(error),
            static (owner, arm) =>
            {
                _ = owner.Subscriptions.Remove(arm);
                owner.Completions++;
            });

        SelectManyThenStageHelper.SubscribeProjectedStage(
            harness,
            InputValue,
            isDone: false,
            ref harness.Subscriptions,
            ref activeCount,
            in spec);

        await Assert.That(activeCount).IsEqualTo(Two);
        await Assert.That(harness.Subscriptions.Count).IsEqualTo(1);

        stageSubject.OnNext(InputValue * Multiplier);
        stageSubject.OnCompleted();
        stageSubject.OnCompleted();

        await Assert.That(harness.Items).IsCollectionEqualTo([InputValue * Multiplier]);
        await Assert.That(harness.Completions).IsEqualTo(1);
        await Assert.That(harness.Subscriptions.Count).IsEqualTo(0);
    }

    /// <summary>Verifies short-circuit when done, selector exception forwarding, and null-sequence fault reporting.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SubscribeProjectedStage_ShortCircuitsWhenDoneAndForwardsSelectorAndNullFaults()
    {
        using StageHarness harness = new();
        var activeCount = 1;

        SelectManyThenStageSpec<StageHarness, int, int> nullSpec = new(
            static _ => null!,
            NullStageMessage,
            static (owner, item) => owner.Items.Add(item),
            static (owner, error) => owner.Errors.Add(error),
            static (_, _) => { });

        SelectManyThenStageHelper.SubscribeProjectedStage(
            harness,
            InputValue,
            isDone: true,
            ref harness.Subscriptions,
            ref activeCount,
            in nullSpec);
        await Assert.That(harness.Errors.Count).IsEqualTo(0);

        SelectManyThenStageHelper.SubscribeProjectedStage(
            harness,
            InputValue,
            isDone: false,
            ref harness.Subscriptions,
            ref activeCount,
            in nullSpec);
        await Assert.That(harness.Errors.Count).IsEqualTo(1);
        await Assert.That(harness.Errors[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(harness.Errors[0].Message).IsEqualTo(NullStageMessage);

        InvalidOperationException selectorBoom = new("selector boom");
        SelectManyThenStageSpec<StageHarness, int, int> throwingSpec = new(
            _ => throw selectorBoom,
            NullStageMessage,
            static (_, _) => { },
            static (owner, error) => owner.Errors.Add(error),
            static (_, _) => { });

        SelectManyThenStageHelper.SubscribeProjectedStage(
            harness,
            InputValue,
            isDone: false,
            ref harness.Subscriptions,
            ref activeCount,
            in throwingSpec);
        await Assert.That(harness.Errors.Count).IsEqualTo(Two);
        await Assert.That(harness.Errors[1]).IsSameReferenceAs(selectorBoom);
        await Assert.That(activeCount).IsEqualTo(1);

        var armErrorHandled = 0;
        SelectManyThenStageArm<StageHarness, int> arm = new(
            harness,
            static (_, _) => { },
            (_, _) => armErrorHandled++,
            static (_, _) => { });
        arm.Attach(null);
        arm.OnError(selectorBoom);
        arm.Dispose();
        await Assert.That(arm.IsDisposed).IsTrue();
        await Assert.That(armErrorHandled).IsEqualTo(1);
    }

    /// <summary>Captures callbacks and mutable <see cref="DisposableSet"/> state for <see cref="SelectManyThenStageHelper"/> tests.</summary>
    private sealed class StageHarness : IDisposable
    {
        /// <summary>Mutable subscription set owned by this harness.</summary>
        private DisposableSet _subscriptions;

        /// <summary>Gets a reference to the mutable subscription set.</summary>
        public ref DisposableSet Subscriptions => ref _subscriptions;

        /// <summary>Gets forwarded items.</summary>
        public List<int> Items { get; } = [];

        /// <summary>Gets forwarded errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets or sets the completion count.</summary>
        public int Completions { get; set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _subscriptions.Dispose();
    }
}
