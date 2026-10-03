// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading.Tasks;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests the immutable, complete occasionally connected context configuration.</summary>
public sealed class OccasionallyConnectedOptionsTests
{
    /// <summary>The documented default maximum number of concurrent streams.</summary>
    private const int DefaultMaxConcurrentStreams = 4;

    /// <summary>The documented default conflict-resolution round count.</summary>
    private const int DefaultMaxConflictResolutionRounds = 3;

    /// <summary>The smallest default scheduling priority.</summary>
    private const int DefaultMinimumPriority = -10;

    /// <summary>The largest default scheduling priority.</summary>
    private const int DefaultMaximumPriority = 10;

    /// <summary>The invalid exact-once behavior value.</summary>
    private const int UndefinedExactlyOnceExpiryBehavior = 999;

    /// <summary>The smaller bound used by ordering tests.</summary>
    private const int SmallerBound = 1;

    /// <summary>The larger bound used by ordering tests.</summary>
    private const int LargerBound = 2;

    /// <summary>The documented default outbox byte capacity.</summary>
    private const long DefaultOutboxBytes = 64L * 1024 * 1024;

    /// <summary>Verifies the shared default is complete and valid.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task DefaultIsACompleteValidConfiguration()
    {
        var options = OccasionallyConnectedOptions.Default;

        options.Validate();

        await Assert.That(options.AutoStart).IsFalse();
        await Assert.That(options.MaxConcurrentStreams).IsEqualTo(DefaultMaxConcurrentStreams);
        await Assert.That(options.MaxConflictResolutionRounds).IsEqualTo(DefaultMaxConflictResolutionRounds);
        await Assert.That(options.MinimumPriority).IsEqualTo(DefaultMinimumPriority);
        await Assert.That(options.MaximumPriority).IsEqualTo(DefaultMaximumPriority);
        await Assert.That(options.ExactlyOnceExpiryBehavior).IsEqualTo(ExactlyOnceExpiryBehavior.StopAndReport);
        await Assert.That(options.Outbox).IsNotNull();
        await Assert.That(options.Inbox).IsNotNull();
        await Assert.That(options.Batching).IsNotNull();
        await Assert.That(options.Retry).IsNotNull();
        await Assert.That(options.CircuitBreaker).IsNotNull();
        await Assert.That(options.Retention).IsNotNull();
        await Assert.That(options.Security).IsNotNull();
        await Assert.That(options.Diagnostics).IsNotNull();
    }

    /// <summary>Verifies that stream concurrency remains positive.</summary>
    /// <param name="value">The invalid positive-only value.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveMaxConcurrentStreamsIsRejected(int value)
    {
        var options = OccasionallyConnectedOptions.Default with { MaxConcurrentStreams = value };

        await AssertParentFailure(options, nameof(OccasionallyConnectedOptions.MaxConcurrentStreams), value);
    }

    /// <summary>Verifies that conflict resolution rounds remain positive.</summary>
    /// <param name="value">The invalid positive-only value.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveMaxConflictResolutionRoundsIsRejected(int value)
    {
        var options = OccasionallyConnectedOptions.Default with { MaxConflictResolutionRounds = value };

        await AssertParentFailure(options, nameof(OccasionallyConnectedOptions.MaxConflictResolutionRounds), value);
    }

    /// <summary>Verifies the inclusive scheduling range has a valid order.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task ReversedPriorityRangeIsRejected()
    {
        var options = OccasionallyConnectedOptions.Default with
        {
            MinimumPriority = LargerBound,
            MaximumPriority = SmallerBound,
        };

        await AssertParentFailure(options, nameof(OccasionallyConnectedOptions.MinimumPriority), LargerBound);
    }

    /// <summary>Verifies undefined exactly-once expiry behavior cannot be used.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task UndefinedExactlyOnceExpiryBehaviorIsRejected()
    {
        const ExactlyOnceExpiryBehavior behavior = (ExactlyOnceExpiryBehavior)UndefinedExactlyOnceExpiryBehavior;
        var options = OccasionallyConnectedOptions.Default with { ExactlyOnceExpiryBehavior = behavior };

        await AssertParentFailure(options, nameof(OccasionallyConnectedOptions.ExactlyOnceExpiryBehavior), behavior);
    }

    /// <summary>Verifies every mandatory nested options record must be supplied.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    /// <exception cref="InvalidOperationException">An expected options property is unavailable.</exception>
    [Test]
    public async Task NullNestedOptionsAreRejected()
    {
        string[] propertyNames =
        [
            nameof(OccasionallyConnectedOptions.Outbox),
            nameof(OccasionallyConnectedOptions.Inbox),
            nameof(OccasionallyConnectedOptions.Batching),
            nameof(OccasionallyConnectedOptions.Retry),
            nameof(OccasionallyConnectedOptions.CircuitBreaker),
            nameof(OccasionallyConnectedOptions.Retention),
            nameof(OccasionallyConnectedOptions.Security),
            nameof(OccasionallyConnectedOptions.Diagnostics),
        ];
        foreach (var propertyName in propertyNames)
        {
            var options = OccasionallyConnectedOptions.Default with { };
            var property = typeof(OccasionallyConnectedOptions).GetProperty(propertyName)
                ?? throw new InvalidOperationException("The required options property is unavailable.");
            property.SetValue(options, null);

            await AssertNestedNullFailure(options, propertyName);
        }
    }

    /// <summary>Verifies parent validation delegates to existing nested validators.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task InvalidNestedOptionsAreRejected()
    {
        await AssertParentFailure(
            OccasionallyConnectedOptions.Default with { Outbox = new OutboxOptions { MaxOperations = 0 } },
            nameof(OutboxOptions.MaxOperations),
            0);
        await AssertParentFailure(
            OccasionallyConnectedOptions.Default with { Inbox = new InboxOptions { MaxEvents = 0 } },
            nameof(InboxOptions.MaxEvents),
            0);
        await AssertParentFailure(
            OccasionallyConnectedOptions.Default with { Batching = new BatchingOptions { MaximumOperations = 0 } },
            "MaximumOperations must be positive.");
        await AssertParentFailure(
            OccasionallyConnectedOptions.Default with { Retry = new RetryOptions { MinimumDelay = TimeSpan.Zero } },
            nameof(RetryOptions.MinimumDelay),
            TimeSpan.Zero);
        await AssertParentFailure(
            OccasionallyConnectedOptions.Default with { CircuitBreaker = new CircuitBreakerOptions { FailureThreshold = 0 } },
            "FailureThreshold must be positive.");
        await AssertParentFailure(
            OccasionallyConnectedOptions.Default with { Retention = new RetentionOptions { SnapshotRetention = TimeSpan.Zero } },
            nameof(RetentionOptions.SnapshotRetention),
            TimeSpan.Zero);
        await AssertParentFailure(
            OccasionallyConnectedOptions.Default with { Security = new SecurityOptions { MaximumPayloadBytes = 0 } },
            nameof(SecurityOptions.MaximumPayloadBytes),
            0);
        await AssertParentFailure(
            OccasionallyConnectedOptions.Default with { Diagnostics = new DiagnosticsOptions { MaximumQueuedFaults = 0 } },
            nameof(DiagnosticsOptions.MaximumQueuedFaults),
            0);
    }

    /// <summary>Verifies copied parent configuration accepts smaller outbox capacity than batch capacity.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task CopiedOptionsAreIndependentAndPermitSmallerOutboxes()
    {
        var source = OccasionallyConnectedOptions.Default;
        var copy = source with
        {
            AutoStart = true,
            Outbox = source.Outbox with { MaxBytes = SmallerBound },
            Batching = source.Batching with { MaximumBytes = LargerBound },
        };

        copy.Validate();

        await Assert.That(source.AutoStart).IsFalse();
        await Assert.That(copy.AutoStart).IsTrue();
        await Assert.That(source.Outbox.MaxBytes).IsEqualTo(DefaultOutboxBytes);
        await Assert.That(copy.Outbox.MaxBytes).IsEqualTo(SmallerBound);
    }

    /// <summary>Verifies a locally constructed batch fits within the security message bound.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task BatchLargerThanTheMaximumMessageIsRejected()
    {
        var options = OccasionallyConnectedOptions.Default with
        {
            Batching = new BatchingOptions { MaximumBytes = LargerBound },
            Security = new SecurityOptions { MaximumPayloadBytes = SmallerBound, MaximumMessageBytes = SmallerBound },
        };

        await AssertParentFailure(options, nameof(OccasionallyConnectedOptions.Batching), (long)LargerBound);
    }

    /// <summary>Asserts a parent option invalid-operation failure.</summary>
    /// <param name="options">The options to validate.</param>
    /// <param name="message">The expected exception message.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    private static async Task AssertParentFailure(OccasionallyConnectedOptions options, string message)
    {
        var exception = Assert.ThrowsExactly<InvalidOperationException>(options.Validate);

        await Assert.That(exception.Message).IsEqualTo(message);
    }

    /// <summary>Asserts a parent option range failure.</summary>
    /// <param name="options">The options to validate.</param>
    /// <param name="parameterName">The expected parameter name.</param>
    /// <param name="actualValue">The expected actual value.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    private static async Task AssertParentFailure(
        OccasionallyConnectedOptions options,
        string parameterName,
        object actualValue)
    {
        var exception = Assert.ThrowsExactly<ArgumentOutOfRangeException>(options.Validate);

        await Assert.That(exception.ParamName).IsEqualTo(parameterName);
        await Assert.That(exception.ActualValue).IsEqualTo(actualValue);
    }

    /// <summary>Asserts a nested option null failure.</summary>
    /// <param name="options">The options to validate.</param>
    /// <param name="parameterName">The expected parameter name.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    private static async Task AssertNestedNullFailure(OccasionallyConnectedOptions options, string parameterName)
    {
        var exception = Assert.ThrowsExactly<ArgumentNullException>(options.Validate);

        await Assert.That(exception.ParamName).IsEqualTo(parameterName);
    }
}
