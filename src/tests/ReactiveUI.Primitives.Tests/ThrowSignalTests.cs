// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests scheduled error delivery.</summary>
public sealed class ThrowSignalTests
{
    /// <summary>An immediate error is forwarded inline without a second terminal notification.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Subscribe_WhenImmediate_ThenForwardsTheErrorInline()
    {
        InvalidOperationException error = new("source failure");
        ThrowSignal<int> source = new(error, Sequencer.Immediate);
        RecordingWitness<int> observer = new();

        using var subscription = source.Subscribe(observer);

        await Assert.That(source.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }
}
