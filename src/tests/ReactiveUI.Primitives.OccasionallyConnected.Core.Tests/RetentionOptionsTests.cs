// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading.Tasks;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests durable data retention configuration.</summary>
public sealed class RetentionOptionsTests
{
    /// <summary>The documented terminal outbox retention in days.</summary>
    private const int OutboxTerminalRetentionDays = 1;

    /// <summary>The documented common retention period in days.</summary>
    private const int CommonRetentionDays = 7;

    /// <summary>The documented dead-letter retention period in days.</summary>
    private const int DeadLetterRetentionDays = 30;

    /// <summary>The documented compaction interval in hours.</summary>
    private const int CompactionIntervalHours = 1;

    /// <summary>Verifies that defaults are finite and preserve the documented retention policy.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task DefaultsAreFiniteAndPositive()
    {
        var options = new RetentionOptions();

        options.Validate();

        await Assert.That(options.OutboxTerminalRetention).IsEqualTo(TimeSpan.FromDays(OutboxTerminalRetentionDays));
        await Assert.That(options.InboxDeduplicationRetention).IsEqualTo(TimeSpan.FromDays(CommonRetentionDays));
        await Assert.That(options.DeadLetterRetention).IsEqualTo(TimeSpan.FromDays(DeadLetterRetentionDays));
        await Assert.That(options.ServerIdempotencyRetention).IsEqualTo(TimeSpan.FromDays(CommonRetentionDays));
        await Assert.That(options.SnapshotRetention).IsEqualTo(TimeSpan.FromDays(CommonRetentionDays));
        await Assert.That(options.CompactionInterval).IsEqualTo(TimeSpan.FromHours(CompactionIntervalHours));
    }

    /// <summary>Verifies retention cannot be disabled or made unbounded.</summary>
    /// <param name="value">The invalid retention value.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    [Arguments(long.MaxValue)]
    public async Task InvalidRetentionValuesAreRejected(long value)
    {
        var retention = TimeSpan.FromTicks(value);

        await AssertRetentionFailure(
            new RetentionOptions { OutboxTerminalRetention = retention },
            nameof(RetentionOptions.OutboxTerminalRetention),
            retention);
        await AssertRetentionFailure(
            new RetentionOptions { InboxDeduplicationRetention = retention },
            nameof(RetentionOptions.InboxDeduplicationRetention),
            retention);
        await AssertRetentionFailure(
            new RetentionOptions { DeadLetterRetention = retention },
            nameof(RetentionOptions.DeadLetterRetention),
            retention);
        await AssertRetentionFailure(
            new RetentionOptions { ServerIdempotencyRetention = retention },
            nameof(RetentionOptions.ServerIdempotencyRetention),
            retention);
        await AssertRetentionFailure(
            new RetentionOptions { SnapshotRetention = retention },
            nameof(RetentionOptions.SnapshotRetention),
            retention);
        await AssertRetentionFailure(
            new RetentionOptions { CompactionInterval = retention },
            nameof(RetentionOptions.CompactionInterval),
            retention);
    }

    /// <summary>Verifies records retain immutable copy semantics.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task CopiedRetentionOptionsDoNotMutateTheSource()
    {
        var source = new RetentionOptions();
        var copy = source with { CompactionInterval = TimeSpan.FromTicks(OutboxTerminalRetentionDays) };

        copy.Validate();

        await Assert.That(source.CompactionInterval).IsEqualTo(TimeSpan.FromHours(CompactionIntervalHours));
        await Assert.That(copy.CompactionInterval).IsEqualTo(TimeSpan.FromTicks(OutboxTerminalRetentionDays));
    }

    /// <summary>Asserts a retention option failure.</summary>
    /// <param name="options">The options to validate.</param>
    /// <param name="parameterName">The expected parameter name.</param>
    /// <param name="actualValue">The expected actual value.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    private static async Task AssertRetentionFailure(
        RetentionOptions options,
        string parameterName,
        TimeSpan actualValue)
    {
        var exception = Assert.ThrowsExactly<ArgumentOutOfRangeException>(options.Validate);

        await Assert.That(exception.ParamName).IsEqualTo(parameterName);
        await Assert.That(exception.ActualValue).IsEqualTo(actualValue);
    }
}
