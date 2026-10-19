// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemotePublishOptions"/>.</summary>
public sealed class RemotePublishOptionsTests
{
    /// <summary>Defines the lowest valid priority.</summary>
    private const int PriorityBelowRange = -11;

    /// <summary>Defines the highest invalid priority.</summary>
    private const int PriorityAboveRange = 11;

    /// <summary>Defines a custom minimum priority.</summary>
    private const int CustomMinimumPriority = -2;

    /// <summary>Defines a custom maximum priority.</summary>
    private const int CustomMaximumPriority = 3;

    /// <summary>Defines an enum value outside supported ranges.</summary>
    private const int UndefinedEnumValue = 42;

    /// <summary>Defines a valid stream identifier used by publish option tests.</summary>
    private static readonly StreamId ValidStreamId = new("sensor/temperature");

    /// <summary>Verifies valid default publish options pass structural validation.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task ValidDefaultOptionsValidate()
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId };

        options.Validate();

        await Assert.That(options.Durable).IsTrue();
        await Assert.That(options.Priority).IsEqualTo(0);
        await Assert.That(options.ConflictPolicy).IsEqualTo(ConflictPolicy.Merge);
        await Assert.That(options.DeliveryGuarantee).IsEqualTo(DeliveryGuarantee.AtLeastOnce);
        await Assert.That(options.AdmissionStrategy).IsEqualTo(BufferStrategy.Block);
    }

    /// <summary>Verifies a default stream identifier is rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task DefaultStreamIdThrows()
    {
        var options = new RemotePublishOptions { StreamId = default };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies undefined delivery guarantees are rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task UndefinedDeliveryGuaranteeThrows()
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, DeliveryGuarantee = (DeliveryGuarantee)UndefinedEnumValue };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies undefined admission strategies are rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task UndefinedAdmissionStrategyThrows()
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, AdmissionStrategy = (BufferStrategy)UndefinedEnumValue };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies undefined conflict policies are rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task UndefinedConflictPolicyThrows()
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, ConflictPolicy = (ConflictPolicy)UndefinedEnumValue };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies priority is restricted to the configured fair scheduling range.</summary>
    /// <param name="priority">The priority value.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    [Arguments(PriorityBelowRange)]
    [Arguments(PriorityAboveRange)]
    public async Task PriorityOutsideRangeThrows(int priority)
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, Priority = priority };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies custom priority bounds include both endpoints.</summary>
    /// <param name="priority">The priority value.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    [Arguments(CustomMinimumPriority)]
    [Arguments(CustomMaximumPriority)]
    public async Task CustomPriorityRangeAcceptsEndpoints(int priority)
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, Priority = priority };

        options.Validate(false, CustomMinimumPriority, CustomMaximumPriority);

        await Assert.That(options.Priority).IsEqualTo(priority);
    }

    /// <summary>Verifies custom priority bounds reject values outside the configured range.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task CustomPriorityRangeRejectsOutsideValue()
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, Priority = PriorityAboveRange };

        Action action = () => options.Validate(false, CustomMinimumPriority, CustomMaximumPriority);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies custom priority bounds require a valid range.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task CustomPriorityRangeRejectsInvertedBounds()
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId };

        Action action = () => options.Validate(false, CustomMaximumPriority, CustomMinimumPriority);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies durable publishing rejects built-in strategies that drop work.</summary>
    /// <param name="admissionStrategy">The dropping admission strategy.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    [Arguments(BufferStrategy.DropOldest)]
    [Arguments(BufferStrategy.DropNewest)]
    public async Task DurablePublishingRejectsDroppingAdmissionStrategies(BufferStrategy admissionStrategy)
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, AdmissionStrategy = admissionStrategy };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies non-durable publishing may use lossy admission.</summary>
    /// <param name="admissionStrategy">The dropping admission strategy.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    [Arguments(BufferStrategy.DropOldest)]
    [Arguments(BufferStrategy.DropNewest)]
    public async Task NonDurablePublishingAllowsDroppingAdmissionStrategies(BufferStrategy admissionStrategy)
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, Durable = false, AdmissionStrategy = admissionStrategy };

        options.Validate();

        await Assert.That(options.AdmissionStrategy).IsEqualTo(admissionStrategy);
    }

    /// <summary>Verifies exactly-once publishing requires durable local storage.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task ExactlyOnceRequiresDurablePublishing()
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, Durable = false, DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce };

        Action action = options.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies custom policies require explicit capability support.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task CustomPoliciesRequireCapability()
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, ConflictPolicy = ConflictPolicy.Custom, AdmissionStrategy = BufferStrategy.Custom };

        Action unsupported = options.Validate;
        var supported = () => options.Validate(supportsCustomPolicy: true);

        await Assert.That(unsupported).ThrowsExactly<InvalidOperationException>();
        supported();
    }

    /// <summary>Verifies durable auditing supports every guarantee with either non-dropping built-in policy.</summary>
    /// <param name="guarantee">The requested guarantee.</param>
    /// <param name="strategy">The durable admission strategy.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(DeliveryGuarantee.AtMostOnce, BufferStrategy.Block)]
    [Arguments(DeliveryGuarantee.AtMostOnce, BufferStrategy.Reject)]
    [Arguments(DeliveryGuarantee.AtLeastOnce, BufferStrategy.Block)]
    [Arguments(DeliveryGuarantee.AtLeastOnce, BufferStrategy.Reject)]
    [Arguments(DeliveryGuarantee.ExactlyOnce, BufferStrategy.Block)]
    [Arguments(DeliveryGuarantee.ExactlyOnce, BufferStrategy.Reject)]
    public async Task DurablePublishingAcceptsNonDroppingStrategies(DeliveryGuarantee guarantee, BufferStrategy strategy)
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, DeliveryGuarantee = guarantee, AdmissionStrategy = strategy };

        options.Validate();

        await Assert.That(options.DeliveryGuarantee).IsEqualTo(guarantee);
        await Assert.That(options.AdmissionStrategy).IsEqualTo(strategy);
    }

    /// <summary>Verifies selecting either custom policy independently requires registration.</summary>
    /// <param name="conflictPolicy">The conflict policy.</param>
    /// <param name="admissionStrategy">The admission strategy.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(ConflictPolicy.Custom, BufferStrategy.Block)]
    [Arguments(ConflictPolicy.Merge, BufferStrategy.Custom)]
    public async Task EachCustomPolicyRequiresRegistration(ConflictPolicy conflictPolicy, BufferStrategy admissionStrategy)
    {
        var options = new RemotePublishOptions { StreamId = ValidStreamId, ConflictPolicy = conflictPolicy, AdmissionStrategy = admissionStrategy };

        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>();
        options.Validate(supportsCustomPolicy: true);
    }

    /// <summary>Verifies per-operation overrides do not mutate shared defaults.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PerOperationOverridesPreserveDefaults()
    {
        var defaults = new RemotePublishOptions { StreamId = ValidStreamId };
        var edit = defaults with { BaseVersion = "etag-v2", ConflictPolicy = ConflictPolicy.LastWriterWins };

        edit.Validate();

        await Assert.That(defaults.BaseVersion).IsNull();
        await Assert.That(defaults.ConflictPolicy).IsEqualTo(ConflictPolicy.Merge);
        await Assert.That(edit.BaseVersion).IsEqualTo("etag-v2");
        await Assert.That(edit.StreamId).IsEqualTo(ValidStreamId);
    }
}
