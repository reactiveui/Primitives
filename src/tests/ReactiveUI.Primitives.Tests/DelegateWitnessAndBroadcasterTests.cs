// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Coverage for the public <see cref="DelegateWitness{T}"/> and <see cref="Broadcaster{T}"/> equality surface.</summary>
public class DelegateWitnessAndBroadcasterTests
{
    private const int One = 1;

    private const int Two = 2;

    /// <summary>The witness forwards every notification to the supplied delegates.</summary>
    [Test]
    public void DelegateWitnessForwardsEachNotificationToItsDelegates()
    {
        var values = new List<int>();
        Exception? captured = null;
        var completed = 0;
        var witness = new DelegateWitness<int>(values.Add, e => captured = e, () => completed++);

        witness.OnNext(One);
        witness.OnNext(Two);
        var error = new InvalidOperationException("boom");
        witness.OnError(error);
        witness.OnCompleted();

        Assert.Equal<int>([One, Two], values);
        Assert.Same(error, captured!);
        Assert.Equal(One, completed);
    }

    /// <summary>The optional onError/onCompleted delegates default to no-ops, and onNext is required.</summary>
    [Test]
    public void DelegateWitnessOptionalHandlersAreNoOpsAndOnNextIsRequired()
    {
        var witness = new DelegateWitness<int>(_ => { });

        // No onError/onCompleted supplied: terminal notifications are ignored without throwing.
        witness.OnError(new InvalidOperationException("ignored"));
        witness.OnCompleted();

        Assert.Throws<ArgumentNullException>(() => _ = new DelegateWitness<int>(null!));
    }

    /// <summary>The equality operators compare the underlying observer set by reference.</summary>
    [Test]
    public void BroadcasterEqualityOperatorsCompareTheObserverSet()
    {
        Broadcaster<int> left = default;
        Broadcaster<int> right = default;

        // Both empty -> same (null) observer set.
        Assert.True(left == right);
        Assert.False(left != right);

        left.Add(new DelegateWitness<int>(_ => { }));

        // Left now references an observer set; right is still empty.
        Assert.True(left != right);
        Assert.False(left == right);
    }
}
