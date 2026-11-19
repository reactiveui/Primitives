// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="KeepWithSyncSignal{T, TState}"/>.</summary>
public sealed class KeepWithSyncSignalTests
{
    /// <summary>Verifies empty and never sources pass through and any other source gets a filter stage.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Create_ConstantOrLiveSource_OnlyLiveSourceIsWrapped()
    {
        var empty = SignalAsync.Empty<int>();
        var never = SignalAsync.Never<int>();

        await Assert.That(KeepWithSyncSignal<int, int>.Create(empty, 0, static (_, _) => true)).IsSameReferenceAs(empty);
        await Assert.That(KeepWithSyncSignal<int, int>.Create(never, 0, static (_, _) => true)).IsSameReferenceAs(never);
        await Assert.That(KeepWithSyncSignal<int, int>.Create(SignalAsync.Return(1), 0, static (_, _) => true))
            .IsTypeOf<KeepWithSyncSignal<int, int>>();
    }
}
