// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="WhereFalseSignal"/>.</summary>
public sealed class WhereFalseSignalTests
{
    /// <summary>Verifies empty, never and already-filtered sources pass through and any other source gets a filter stage.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Create_ConstantFilteredOrLiveSource_OnlyLiveSourceIsWrapped()
    {
        var empty = SignalAsync.Empty<bool>();
        var never = SignalAsync.Never<bool>();

        await Assert.That(WhereFalseSignal.Create(empty)).IsSameReferenceAs(empty);
        await Assert.That(WhereFalseSignal.Create(never)).IsSameReferenceAs(never);
        var whereFalse = WhereFalseSignal.Create(SignalAsync.Return(false));
        await Assert.That(whereFalse).IsTypeOf<WhereFalseSignal>();
        await Assert.That(WhereFalseSignal.Create(whereFalse)).IsSameReferenceAs(whereFalse);
    }
}
