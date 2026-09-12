// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Tests synchronous wait and action ordering.</summary>
public class ScheduledWaitTests
{
    /// <summary>The action observes the state left by the wait operation.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_WaitReturns_ActionObservesWaitState()
    {
        var expectedDelay = TimeSpan.FromSeconds(1);
        TimeSpan? observedDelay = null;
        TimeSpan? actionDelay = null;
        using var result = ScheduledWait.Run(expectedDelay, () => actionDelay = observedDelay, delay => observedDelay = delay);
        await Assert.That(observedDelay).IsEqualTo(expectedDelay);
        await Assert.That(actionDelay).IsEqualTo(expectedDelay);
    }

    /// <summary>A failed wait prevents the action and preserves the original exception.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Run_WaitThrows_DoesNotInvokeAction()
    {
        var ran = false;
        InvalidOperationException error = new("wait failed");
        var caught = await Assert.That(() => ScheduledWait.Run(TimeSpan.Zero, () => ran = true, _ => throw error))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(caught).IsSameReferenceAs(error);
        await Assert.That(ran).IsFalse();
    }
}
