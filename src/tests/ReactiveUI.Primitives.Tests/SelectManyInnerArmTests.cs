// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="SelectManyInnerArm{TOwner, TState, TValue}"/>.</summary>
public sealed class SelectManyInnerArmTests
{
    /// <summary>State multiplier.</summary>
    private const int Multiplier = 10;

    /// <summary>Sample inner value.</summary>
    private const int InnerValue = 6;

    /// <summary>Verifies <see cref="SelectManyInnerArm{TOwner, TState, TValue}"/> forwards state-paired values, errors, single completion, and subscription disposal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ArmCallbacks_ForwardStateEnforceSingleCompletionAndDispose()
    {
        ArmHarness harness = new();
        var innerDisposals = 0;
        SelectManyInnerArm<ArmHarness, int, int> arm = new(
            harness,
            Multiplier,
            static (owner, factor, value) => owner.Values.Add(factor * value),
            static (owner, error) => owner.Errors.Add(error),
            static (owner, self) =>
            {
                owner.CompletedArms.Add(self);
                self.Dispose();
            });
        arm.Attach(new ActionDisposable(() => innerDisposals++));

        arm.OnNext(InnerValue);
        InvalidOperationException expected = new("inner fault");
        arm.OnError(expected);
        arm.OnCompleted();
        arm.OnCompleted();

        await Assert.That(harness.Values.SequenceEqual([Multiplier * InnerValue])).IsTrue();
        await Assert.That(harness.Errors.SequenceEqual([expected])).IsTrue();
        await Assert.That(harness.CompletedArms.Count).IsEqualTo(1);
        await Assert.That(harness.CompletedArms[0]).IsSameReferenceAs(arm);
        await Assert.That(innerDisposals).IsEqualTo(1);
    }

    /// <summary>Captures callbacks delivered by <see cref="SelectManyInnerArm{TOwner, TState, TValue}"/>.</summary>
    private sealed class ArmHarness
    {
        /// <summary>Gets observed projected values.</summary>
        public List<int> Values { get; } = [];

        /// <summary>Gets observed errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets completed arm instances.</summary>
        public List<IDisposable> CompletedArms { get; } = [];
    }
}
