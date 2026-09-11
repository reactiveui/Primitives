// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteSubscriptionOptions"/>.</summary>
public sealed class RemoteSubscriptionOptionsTests
{
    /// <summary>Defines the expected default subscription item capacity.</summary>
    private const int DefaultBufferCapacity = 1024;

    /// <summary>Defines the expected default subscription byte capacity.</summary>
    private const long DefaultBufferCapacityBytes = 16_777_216;

    /// <summary>Defines an enum value outside supported ranges.</summary>
    private const int UndefinedEnumValue = 42;

    /// <summary>Defines a valid stream identifier used by subscription option tests.</summary>
    private static readonly StreamId ValidStreamId = new("sensor/temperature");

    /// <summary>Verifies valid default subscription options pass structural validation.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task ValidDefaultOptionsValidate()
    {
        var options = new RemoteSubscriptionOptions { StreamId = ValidStreamId };

        options.Validate();

        await Assert.That(options.SubscriptionId).IsNull();
        await Assert.That(options.StartPosition).IsSameReferenceAs(StartPosition.Latest);
        await Assert.That(options.DeliveryGuarantee).IsEqualTo(DeliveryGuarantee.AtLeastOnce);
        await Assert.That(options.BufferStrategy).IsEqualTo(BufferStrategy.Block);
        await Assert.That(options.BufferCapacity).IsEqualTo(DefaultBufferCapacity);
        await Assert.That(options.BufferCapacityBytes).IsEqualTo(DefaultBufferCapacityBytes);
    }

    /// <summary>Verifies invalid stream identifiers are rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task DefaultStreamIdThrows()
    {
        var options = new RemoteSubscriptionOptions { StreamId = default };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies an empty optional subscription identifier is rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EmptySubscriptionIdThrows()
    {
        var options = new RemoteSubscriptionOptions { StreamId = ValidStreamId, SubscriptionId = default(SubscriptionId) };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a missing start position is rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task NullStartPositionThrows()
    {
        var options = new RemoteSubscriptionOptions { StreamId = ValidStreamId, StartPosition = null! };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies non-positive capacities are rejected.</summary>
    /// <param name="bufferCapacity">The subscription item buffer capacity.</param>
    /// <param name="bufferCapacityBytes">The subscription byte buffer capacity.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    [Arguments(0, 1L)]
    [Arguments(1, 0L)]
    [Arguments(-1, 1L)]
    [Arguments(1, -1L)]
    public async Task NonPositiveCapacitiesThrow(int bufferCapacity, long bufferCapacityBytes)
    {
        var options = new RemoteSubscriptionOptions { StreamId = ValidStreamId, BufferCapacity = bufferCapacity, BufferCapacityBytes = bufferCapacityBytes };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies undefined delivery guarantees are rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task UndefinedDeliveryGuaranteeThrows()
    {
        var options = new RemoteSubscriptionOptions { StreamId = ValidStreamId, DeliveryGuarantee = (DeliveryGuarantee)UndefinedEnumValue };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies undefined buffer strategies are rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task UndefinedBufferStrategyThrows()
    {
        var options = new RemoteSubscriptionOptions { StreamId = ValidStreamId, BufferStrategy = (BufferStrategy)UndefinedEnumValue };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies custom policies require explicit capability support.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task CustomBufferStrategyRequiresCapability()
    {
        var options = new RemoteSubscriptionOptions { StreamId = ValidStreamId, BufferStrategy = BufferStrategy.Custom };

        Action unsupported = options.Validate;
        var supported = () => options.Validate(supportsCustomPolicy: true);

        await Assert.That(unsupported).ThrowsExactly<InvalidOperationException>();
        supported();
    }

    /// <summary>Verifies a recovered subscription can preserve its opaque initial position and identity.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ExplicitSubscriptionIdentityAndCursorArePreserved()
    {
        var subscriptionId = SubscriptionId.New();
        var position = StartPosition.FromCursor("opaque/resume==");
        var options = new RemoteSubscriptionOptions { StreamId = ValidStreamId, SubscriptionId = subscriptionId, StartPosition = position };

        options.Validate();

        await Assert.That(options.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(options.StartPosition).IsSameReferenceAs(position);
    }
}
