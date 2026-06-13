// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Optional{T}"/> empty and value contracts.</summary>
public class OptionalTests
{
    /// <summary>The first expected value.</summary>
    private const int First = 1;

    /// <summary>The second expected value.</summary>
    private const int Second = 2;

    /// <summary>Covers optional value creation and empty value access.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OptionalCoversEmptyAndValueContracts()
    {
        Optional<int> defaultOptional = new();
        await Assert.That(defaultOptional.HasValue).IsFalse();
        Assert.Throws<InvalidOperationException>(() => _ = defaultOptional.Value);
        await Assert.That(Optional<int>.Empty.HasValue).IsFalse();
        await Assert.That(Optional<int>.None.HasValue).IsFalse();
        Optional<int> constructed = new(First);
        await Assert.That(constructed.HasValue).IsTrue();
        await Assert.That(constructed.Value).IsEqualTo(First);
        var some = Optional<int>.Some(Second);
        await Assert.That(some.HasValue).IsTrue();
        await Assert.That(some.Value).IsEqualTo(Second);
    }
}
