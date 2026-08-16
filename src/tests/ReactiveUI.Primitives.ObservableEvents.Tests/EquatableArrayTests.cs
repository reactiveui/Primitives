// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies the value equality that the generator's per-target caching is built on.</summary>
public sealed class EquatableArrayTests
{
    /// <summary>Verifies a defaulted array reports itself empty and equal to another defaulted one.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EquatableArrayTreatsDefaultAsEmpty()
    {
        var first = EquatableArray<string>.Empty;
        var second = default(EquatableArray<string>);

        await Assert.That(first.IsEmpty).IsTrue();
        await Assert.That(first.AsArray()).IsEmpty();
        await Assert.That(first == second).IsTrue();
        await Assert.That(first.GetHashCode()).IsEqualTo(second.GetHashCode());
    }

    /// <summary>Verifies an array wrapping no elements matches a defaulted one, hash included.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EquatableArrayMatchesDefaultWhenWrappingNoElements()
    {
        var wrapped = new EquatableArray<string>([]);

        await Assert.That(wrapped.IsEmpty).IsTrue();
        await Assert.That(wrapped == EquatableArray<string>.Empty).IsTrue();
        await Assert.That(wrapped.GetHashCode()).IsEqualTo(EquatableArray<string>.Empty.GetHashCode());
    }

    /// <summary>Verifies equal contents in separate arrays compare equal, which is what keeps a step cached.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EquatableArrayComparesEqualContentsAcrossSeparateArrays()
    {
        var first = new EquatableArray<string>(["a", "b"]);
        var second = new EquatableArray<string>(["a", "b"]);

        await Assert.That(first == second).IsTrue();
        await Assert.That(first != second).IsFalse();
        await Assert.That(first.Equals((object)second)).IsTrue();
        await Assert.That(first.GetHashCode()).IsEqualTo(second.GetHashCode());
    }

    /// <summary>Verifies a differing element, a differing length, and an empty counterpart all compare unequal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EquatableArrayComparesUnequalWhenContentsDiffer()
    {
        var reference = new EquatableArray<string>(["a", "b"]);

        await Assert.That(reference != new EquatableArray<string>(["a", "c"])).IsTrue();
        await Assert.That(reference != new EquatableArray<string>(["a"])).IsTrue();
        await Assert.That(reference != EquatableArray<string>.Empty).IsTrue();
        await Assert.That(EquatableArray<string>.Empty != reference).IsTrue();
    }

    /// <summary>Verifies comparing against an unrelated object is unequal rather than throwing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EquatableArrayComparesUnequalToAnUnrelatedObject()
    {
        var array = new EquatableArray<string>(["a"]);

        await Assert.That(array.Equals("a")).IsFalse();
    }

    /// <summary>Verifies the wrapped elements come back out in order for the emitters to walk.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EquatableArrayReturnsItsElementsInOrder()
    {
        var array = new EquatableArray<string>(["first", "second"]);

        await Assert.That(array.IsEmpty).IsFalse();
        await Assert.That(string.Join(",", array.AsArray())).IsEqualTo("first,second");
    }
}
