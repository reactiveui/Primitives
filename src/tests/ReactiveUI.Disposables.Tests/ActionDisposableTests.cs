// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests for <see cref="ActionDisposable"/> — verifies the dispose action runs exactly once regardless of how many times <c>Dispose</c> is called.</summary>
public class ActionDisposableTests
{
    /// <summary>Verifies the action is invoked exactly once across repeated <c>Dispose</c> calls.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenActionDisposableDisposedTwice_ThenActionInvokedExactlyOnce()
    {
        var invocations = 0;
        ActionDisposable disposable = new(() => invocations++);

        await Assert.That(disposable.IsDisposed).IsFalse();

        disposable.Dispose();
        disposable.Dispose();
        disposable.Dispose();

        await Assert.That(disposable.IsDisposed).IsTrue();
        await Assert.That(invocations).IsEqualTo(1);
    }
}
