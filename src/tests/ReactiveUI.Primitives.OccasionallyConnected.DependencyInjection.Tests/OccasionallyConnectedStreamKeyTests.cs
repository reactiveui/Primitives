// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStreamKey{TState,TInput}" />.</summary>
public sealed class OccasionallyConnectedStreamKeyTests
{
    /// <summary>The stream name used by equality tests.</summary>
    private const string StreamName = "counter";

    /// <summary>Verifies stream keys expose their closed state and input types.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamKeyExposesTypedStateAndInputTypes()
    {
        var key = CreateCounterKey(StreamName);

        await Assert.That(key.StateType).IsEqualTo(typeof(DependencyInjectionTestDoubles.CounterState));
        await Assert.That(key.InputType).IsEqualTo(typeof(DependencyInjectionTestDoubles.CounterInput));
    }

    /// <summary>Verifies stream key equality is ordinal within the same closed generic key type.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamKeyEqualityUsesNameWithinClosedGenericType()
    {
        var first = CreateCounterKey(StreamName);
        var second = CreateCounterKey(StreamName);
        var differentName = CreateCounterKey("counter-other");
        var differentClosedType = new OccasionallyConnectedStreamKey<
            DependencyInjectionTestDoubles.OtherState,
            DependencyInjectionTestDoubles.CounterInput>(StreamName);

        await Assert.That(first == second).IsTrue();
        await Assert.That(first != second).IsFalse();
        await Assert.That(first.Equals((object)second)).IsTrue();
        await Assert.That(first.GetHashCode()).IsEqualTo(second.GetHashCode());
        await Assert.That(first == differentName).IsFalse();
        await Assert.That(first != differentName).IsTrue();
        await Assert.That(first.Equals((object)differentClosedType)).IsFalse();
    }

    /// <summary>Verifies the default stream key can be used by hash-based collections.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DefaultStreamKeyHashCodeDoesNotThrowAndSupportsDictionaryUse()
    {
        var values = CreateCounterDictionary();

        values[default] = 1;

        await Assert.That(values[default]).IsEqualTo(1);
    }

    /// <summary>Creates a counter stream key dictionary.</summary>
    /// <returns>The dictionary.</returns>
    private static Dictionary<
        OccasionallyConnectedStreamKey<
            DependencyInjectionTestDoubles.CounterState,
            DependencyInjectionTestDoubles.CounterInput>,
        int> CreateCounterDictionary() => [];

    /// <summary>Creates a counter stream key.</summary>
    /// <param name="name">The stream name.</param>
    /// <returns>The stream key.</returns>
    private static OccasionallyConnectedStreamKey<
        DependencyInjectionTestDoubles.CounterState,
        DependencyInjectionTestDoubles.CounterInput> CreateCounterKey(string name) => new(name);
}
