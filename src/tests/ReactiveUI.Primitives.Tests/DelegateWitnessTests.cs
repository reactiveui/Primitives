// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Coverage for the public <see cref="DelegateWitness{T}"/> surface.</summary>
public class DelegateWitnessTests
{
    /// <summary>The literal one.</summary>
    private const int One = 1;

    /// <summary>The literal two.</summary>
    private const int Two = 2;

    /// <summary>The witness forwards every notification to the supplied delegates.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelegateWitnessForwardsEachNotificationToItsDelegates()
    {
        List<int> values = [];
        Exception? captured = null;
        var completed = 0;
        DelegateWitness<int> witness = new(values.Add, e => captured = e, () => completed++);
        witness.OnNext(One);
        witness.OnNext(Two);
        InvalidOperationException error = new("boom");
        witness.OnError(error);
        witness.OnCompleted();
        await Assert.That(values.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(captured!).IsSameReferenceAs(error);
        await Assert.That(completed).IsEqualTo(One);
    }

    /// <summary>The optional onError/onCompleted delegates default to no-ops, and onNext is required.</summary>
    [Test]
    public void DelegateWitnessOptionalHandlersAreNoOpsAndOnNextIsRequired()
    {
        DelegateWitness<int> witness = new(_ => { });

        // No onError/onCompleted supplied: terminal notifications are ignored without throwing.
        witness.OnError(new InvalidOperationException("ignored"));
        witness.OnCompleted();
        Assert.Throws<ArgumentNullException>(() => _ = new DelegateWitness<int>(null!));
    }
}
