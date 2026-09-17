// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Helpers;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests disposal ownership claims.</summary>
public sealed class DisposalHelperTests
{
    /// <summary>Verifies that disposal ownership is granted only once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenDisposalClaimedTwice_ThenSecondClaimFindsItDisposed()
    {
        var disposed = 0;
        await Assert.That(DisposalHelper.HasDisposed(disposed)).IsFalse();
        await Assert.That(DisposalHelper.TrySetDisposed(ref disposed)).IsFalse();
        await Assert.That(DisposalHelper.HasDisposed(disposed)).IsTrue();
        await Assert.That(DisposalHelper.TrySetDisposed(ref disposed)).IsTrue();
    }
}
