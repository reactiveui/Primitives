// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="StreamDefinition{TState, TInput}"/>.</summary>
public sealed class StreamDefinitionTests
{
    /// <summary>Defines the publication priority outside the custom range.</summary>
    private const int PriorityOutsideCustomRange = 3;

    /// <summary>Defines a valid stream identifier used by stream definition tests.</summary>
    private static readonly StreamId ValidStreamId = new("sensor/temperature");

    /// <summary>Defines the projection used by valid stream definitions.</summary>
    private static readonly ILocalProjection<int, string> Projection = new TestProjection();

    /// <summary>Verifies the required properties and recovery defaults form a valid definition.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task ValidDefinitionUsesRecoveryDefaults()
    {
        var definition = CreateValidDefinition();

        definition.Validate();

        await Assert.That(definition.StreamId).IsEqualTo(ValidStreamId);
        await Assert.That(definition.SubscriptionId).IsNull();
        await Assert.That(definition.Projection).IsEqualTo(Projection);
        await Assert.That(definition.InputContractId).IsEqualTo("temperature-input");
        await Assert.That(definition.StateContractId).IsEqualTo("temperature-state");
        await Assert.That(definition.InputSchemaVersion).IsEqualTo(1);
        await Assert.That(definition.StateSchemaVersion).IsEqualTo(1);
        await Assert.That(definition.SnapshotFormatVersion).IsEqualTo(1);
        await Assert.That(definition.Subscription).IsNull();
        await Assert.That(definition.Publish).IsNull();
        await Assert.That(definition.Input).IsNull();
    }

    /// <summary>Verifies a default stream identifier is rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task DefaultStreamIdThrows()
    {
        var definition = CreateValidDefinition() with { StreamId = default };
        Action action = definition.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a default optional subscription identifier is rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task EmptySubscriptionIdThrows()
    {
        var definition = CreateValidDefinition() with { SubscriptionId = default(SubscriptionId) };
        Action action = definition.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a missing projection is rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task NullProjectionThrows()
    {
        var definition = CreateValidDefinition() with { Projection = null! };
        Action action = definition.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing wire contract identifiers are rejected.</summary>
    /// <param name="inputContractId">The candidate input contract identifier.</param>
    /// <param name="stateContractId">The candidate state contract identifier.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    [Arguments(" ", "temperature-state")]
    [Arguments("temperature-input", "\t")]
    public async Task WhitespaceContractIdThrows(string inputContractId, string stateContractId)
    {
        var definition = CreateValidDefinition() with { InputContractId = inputContractId, StateContractId = stateContractId };
        Action action = definition.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies serialization and recovery versions must be positive.</summary>
    /// <param name="inputSchemaVersion">The candidate input schema version.</param>
    /// <param name="stateSchemaVersion">The candidate state schema version.</param>
    /// <param name="snapshotFormatVersion">The candidate snapshot format version.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    [Arguments(0, 1, 1)]
    [Arguments(1, -1, 1)]
    [Arguments(1, 1, 0)]
    public async Task NonPositiveVersionThrows(int inputSchemaVersion, int stateSchemaVersion, int snapshotFormatVersion)
    {
        var definition = CreateValidDefinition() with
        {
            InputSchemaVersion = inputSchemaVersion,
            StateSchemaVersion = stateSchemaVersion,
            SnapshotFormatVersion = snapshotFormatVersion,
        };
        Action action = definition.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies nested options must use the definition stream identity.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task NestedStreamIdMismatchThrows()
    {
        var otherStreamId = new StreamId("sensor/humidity");
        var subscriptionDefinition = CreateValidDefinition() with { Subscription = new() { StreamId = otherStreamId } };
        var publishDefinition = CreateValidDefinition() with { Publish = new() { StreamId = otherStreamId } };
        Action subscriptionAction = subscriptionDefinition.Validate;
        Action publishAction = publishDefinition.Validate;

        await Assert.That(subscriptionAction).ThrowsExactly<InvalidOperationException>();
        await Assert.That(publishAction).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies conflicting explicit subscription identities are rejected.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task ConflictingSubscriptionIdThrows()
    {
        var definition = CreateValidDefinition() with
        {
            SubscriptionId = SubscriptionId.New(),
            Subscription = new() { StreamId = ValidStreamId, SubscriptionId = SubscriptionId.New() },
        };
        Action action = definition.Validate;

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies nested custom policies are forwarded to their capability validators.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task CustomNestedPoliciesRequireSupport()
    {
        var definition = CreateValidDefinition() with
        {
            Subscription = new() { StreamId = ValidStreamId, BufferStrategy = BufferStrategy.Custom },
            Publish = new() { StreamId = ValidStreamId, AdmissionStrategy = BufferStrategy.Custom, ConflictPolicy = ConflictPolicy.Custom },
            Input = new() { BufferStrategy = BufferStrategy.Custom },
        };
        Action unsupported = definition.Validate;
        void ValidateWithCustomPolicies() => definition.Validate(supportsCustomPolicy: true);

        await Assert.That(unsupported).ThrowsExactly<InvalidOperationException>();
        ValidateWithCustomPolicies();
    }

    /// <summary>Verifies publish priority validation uses caller supplied bounds.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task CustomPriorityBoundsAreForwardedToPublish()
    {
        var definition = CreateValidDefinition() with { Publish = new() { StreamId = ValidStreamId, Priority = PriorityOutsideCustomRange } };
        Action action = () => definition.Validate(supportsCustomPolicy: false, minimumPriority: -2, maximumPriority: 2);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies neither contract identifier can be omitted despite required-member initialization.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task NullContractIdsThrow()
    {
        var inputMissing = CreateValidDefinition() with { InputContractId = null! };
        var stateMissing = CreateValidDefinition() with { StateContractId = null! };
        await Assert.That(inputMissing.Validate).ThrowsExactly<InvalidOperationException>();
        await Assert.That(stateMissing.Validate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies an existing durable identity is compatible when configured on either or both option levels.</summary>
    /// <param name="location">The location at which the identity is supplied.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments("definition")]
    [Arguments("subscription")]
    [Arguments("both")]
    public async Task AcceptsConsistentDurableSubscriptionIdentity(string location)
    {
        var identity = SubscriptionId.New();
        var definition = CreateValidDefinition() with
        {
            SubscriptionId = location == "subscription" ? null : identity,
            Subscription = new() { StreamId = ValidStreamId, SubscriptionId = location == "definition" ? null : identity },
        };
        definition.Validate();
        await Assert.That(definition.SubscriptionId ?? definition.Subscription?.SubscriptionId).IsEqualTo(identity);
    }

    /// <summary>Verifies each nested custom-policy requirement is checked even when other options are absent.</summary>
    /// <param name="location">The independently configured option.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments("publish")]
    [Arguments("input")]
    public async Task IsolatedCustomPoliciesRequireRegistration(string location)
    {
        var definition = CreateValidDefinition();
        definition = location == "publish"
            ? definition with { Publish = new() { StreamId = ValidStreamId, AdmissionStrategy = BufferStrategy.Custom } }
            : definition with { Input = new() { BufferStrategy = BufferStrategy.Custom } };
        await Assert.That(definition.Validate).ThrowsExactly<InvalidOperationException>();
        definition.Validate(supportsCustomPolicy: true);
    }

    /// <summary>Verifies an invalid input bridge cannot hide behind otherwise valid stream settings.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task InputBridgeRejectsBlockingObserverAdmission()
    {
        var definition = CreateValidDefinition() with { Input = new() { BufferStrategy = BufferStrategy.Block } };
        await Assert.That(definition.Validate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies invalid priority bounds fail even before publication options are added.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task InvalidPriorityRangeWithoutPublishThrows()
    {
        var definition = CreateValidDefinition();
        await Assert.That(() => definition.Validate(false, 1, 0)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a valid stream definition for testing.</summary>
    /// <returns>A valid stream definition.</returns>
    private static StreamDefinition<int, string> CreateValidDefinition() => new()
    {
        StreamId = ValidStreamId,
        Projection = Projection,
        InputContractId = "temperature-input",
        StateContractId = "temperature-state",
    };

    /// <summary>Provides a deterministic projection for validating the definition contract.</summary>
    private sealed class TestProjection : ILocalProjection<int, string>
    {
        /// <summary>Gets the initial projection state.</summary>
        public int InitialState => 0;

        /// <summary>Returns the unchanged local state.</summary>
        /// <param name="state">The current state.</param>
        /// <param name="input">The local input.</param>
        /// <param name="operation">The local operation.</param>
        /// <returns>The supplied reducer state value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ApplyLocal(int state, string input, SyncOperation operation) => state;

        /// <summary>Returns the unchanged remote state.</summary>
        /// <param name="state">The current state.</param>
        /// <param name="input">The remote input.</param>
        /// <param name="remoteEvent">The remote event.</param>
        /// <returns>The supplied reducer state value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ApplyRemote(int state, string input, RemoteEvent remoteEvent) => state;

        /// <summary>Returns the unchanged reconciled state.</summary>
        /// <param name="state">The current state.</param>
        /// <param name="result">The conflict resolution result.</param>
        /// <returns>The supplied reducer state value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Reconcile(int state, ConflictResolutionResult result) => state;
    }
}
