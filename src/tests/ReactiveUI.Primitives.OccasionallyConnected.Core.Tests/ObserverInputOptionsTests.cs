// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ObserverInputOptions"/>.</summary>
public sealed class ObserverInputOptionsTests
{
    /// <summary>Defines the expected default observer item capacity.</summary>
    private const int DefaultBufferCapacity = 256;

    /// <summary>Defines the expected default observer byte capacity.</summary>
    private const long DefaultBufferCapacityBytes = 4_194_304;

    /// <summary>Defines an enum value outside the supported range.</summary>
    private const int UndefinedEnumValue = 42;

    /// <summary>Verifies the default observer bridge options are valid.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task DefaultOptionsValidate()
    {
        var options = new ObserverInputOptions();

        options.Validate();

        await Assert.That(options.BufferStrategy).IsEqualTo(BufferStrategy.Reject);
        await Assert.That(options.BufferCapacity).IsEqualTo(DefaultBufferCapacity);
        await Assert.That(options.BufferCapacityBytes).IsEqualTo(DefaultBufferCapacityBytes);
    }

    /// <summary>Verifies non-positive capacities are rejected.</summary>
    /// <param name="bufferCapacity">The observer queue item capacity.</param>
    /// <param name="bufferCapacityBytes">The observer queue byte capacity.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    [Arguments(0, 1L)]
    [Arguments(1, 0L)]
    [Arguments(-1, 1L)]
    [Arguments(1, -1L)]
    public async Task NonPositiveCapacitiesThrow(int bufferCapacity, long bufferCapacityBytes)
    {
        var options = new ObserverInputOptions { BufferCapacity = bufferCapacity, BufferCapacityBytes = bufferCapacityBytes };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies undefined observer buffer strategies are rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task UndefinedBufferStrategyThrows()
    {
        var options = new ObserverInputOptions { BufferStrategy = (BufferStrategy)UndefinedEnumValue };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies synchronous observer bridges reject asynchronous blocking backpressure.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task BlockBufferStrategyThrows()
    {
        var options = new ObserverInputOptions { BufferStrategy = BufferStrategy.Block };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies custom strategies remain unsupported on synchronous observer bridges.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task CustomBufferStrategyThrowsEvenWithCapability()
    {
        var options = new ObserverInputOptions { BufferStrategy = BufferStrategy.Custom };

        Action unsupported = options.Validate;
        Action supported = () => options.Validate(supportsCustomPolicy: true);

        await Assert.That(unsupported).ThrowsExactly<InvalidOperationException>();
        await Assert.That(supported).ThrowsExactly<InvalidOperationException>();
    }
}
