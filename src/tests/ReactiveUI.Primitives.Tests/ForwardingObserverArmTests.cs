// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="ForwardingObserverArm{TOwner, T}"/> and <see cref="BinaryArmCallbacks{TOwner, T}"/>.</summary>
public sealed class ForwardingObserverArmTests
{
    /// <summary>Sample value.</summary>
    private const int Nine = 9;

    /// <summary>Verifies <see cref="ForwardingObserverArm{TOwner, T}"/> forwards notifications and honors <see cref="BinaryArmCallbacks{TOwner, T}.DisposeArmOnCompleted"/>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ForwardingObserverArm_ForwardsNotificationsAndHonorsDisposeOnCompleted()
    {
        List<int> received = [];
        List<Exception> errors = [];
        var completions = 0;
        var disposed = 0;

        BinaryArmCallbacks<List<int>, int> callbacks = new(
            static (sink, v) => sink.Add(v),
            (_, ex) => errors.Add(ex),
            _ => completions++,
            DisposeArmOnCompleted: true);

        ForwardingObserverArm<List<int>, int> arm = new(received, callbacks);
        arm.Attach(new ActionDisposable(() => disposed++));

        arm.OnNext(Nine);
        InvalidOperationException err = new("arm error");
        arm.OnError(err);
        arm.OnCompleted();

        await Assert.That(received.SequenceEqual([Nine])).IsTrue();
        await Assert.That(errors.SequenceEqual([err])).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(disposed).IsEqualTo(1);
    }
}
