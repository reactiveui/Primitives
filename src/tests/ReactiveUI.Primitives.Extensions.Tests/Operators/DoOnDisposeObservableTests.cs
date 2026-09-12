// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests that the disposal action runs once after upstream disposal.</summary>
public class DoOnDisposeObservableTests
{
    /// <summary>Verifies the dispose action fires once and the upstream is detached on first dispose, and a
    /// second dispose is a no-op via the latch.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedTwice_ThenActionFiresOnce()
    {
        var executed = 0;
        Subject<int> source = new();

        var sub = source.DoOnDispose(() => executed++).Subscribe();
        await Assert.That(source.HasObservers).IsTrue();

        sub.Dispose();
        sub.Dispose();

        await Assert.That(executed).IsEqualTo(1);
        await Assert.That(source.HasObservers).IsFalse();
    }
}
