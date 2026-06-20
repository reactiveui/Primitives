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
        Optional<string?> nullOptional = new(null);
        await Assert.That(nullOptional.HasValue).IsFalse();
        var some = Optional<int>.Some(Second);
        await Assert.That(some.HasValue).IsTrue();
        await Assert.That(some.Value).IsEqualTo(Second);
    }

    /// <summary>Covers optional conversion helpers and operators.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OptionalSupportsConversionHelpers()
    {
        var created = Optional<int>.Create(First);
        var converted = Optional<int>.ToOptional(Second);
        Optional<int> implicitOptional = First;
        var explicitValue = (int?)converted;
        var none = Optional<int>.None;
        var some = Optional.Some(Second);

        await Assert.That(created.HasValue).IsTrue();
        await Assert.That(created.Value).IsEqualTo(First);
        await Assert.That(Optional<int>.FromOptional(created)).IsEqualTo(First);
        await Assert.That(converted.HasValue).IsTrue();
        await Assert.That(explicitValue).IsEqualTo(Second);
        await Assert.That(implicitOptional.Value).IsEqualTo(First);
        await Assert.That(none.HasValue).IsFalse();
        await Assert.That(some.Value).IsEqualTo(Second);
    }

    /// <summary>Covers optional string formatting for values and empty values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OptionalToStringFormatsValueAndNone()
    {
        await Assert.That(Optional<int>.Some(First).ToString()).IsEqualTo("1");
        await Assert.That(Optional<int>.None.ToString()).IsEqualTo("<None>");
        await Assert.That(Optional<string?>.Some(null).HasValue).IsFalse();
        await Assert.That(Optional<string?>.Some(null).ToString()).IsEqualTo("<None>");
    }
}
