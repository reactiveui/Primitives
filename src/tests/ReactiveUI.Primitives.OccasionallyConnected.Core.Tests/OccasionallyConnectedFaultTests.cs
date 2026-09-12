// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedFault"/>.</summary>
public sealed class OccasionallyConnectedFaultTests
{
    /// <summary>Verifies the fault code is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsCode()
    {
        StreamId streamId = new("sensor/temperature");
        var operationId = OperationId.New();
        var exception = new InvalidOperationException("failure");
        var occurredAt = DateTimeOffset.UnixEpoch;
        var fault = new OccasionallyConnectedFault(
            "OC.Fault",
            "message",
            occurredAt,
            streamId,
            operationId,
            exception);

        await Assert.That(fault.Code).IsEqualTo("OC.Fault");
        await Assert.That(fault.Message).IsEqualTo("message");
        await Assert.That(fault.OccurredAtUtc).IsEqualTo(occurredAt);
        await Assert.That(fault.StreamId).IsEqualTo(streamId);
        await Assert.That(fault.OperationId).IsEqualTo(operationId);
        await Assert.That(fault.Exception).IsSameReferenceAs(exception);
        await Assert.That(fault.Category).IsEqualTo(FaultCategory.InternalInvariant);
        await Assert.That(fault.Severity).IsEqualTo(FaultSeverity.Error);
        await Assert.That(fault.IsTransient).IsFalse();
        fault.Validate();
    }

    /// <summary>Verifies an authentication fault retains actionable category and recovery information.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AuthenticationFaultPreservesRetryClassification()
    {
        var fault = new OccasionallyConnectedFault("OC.CredentialsExpired", "Renew credentials.", DateTimeOffset.UnixEpoch, null, null, null)
        {
            Category = FaultCategory.Authentication,
            Severity = FaultSeverity.Warning,
            IsTransient = true,
        };
        fault.Validate();
        await Assert.That(fault.Category).IsEqualTo(FaultCategory.Authentication);
        await Assert.That(fault.Severity).IsEqualTo(FaultSeverity.Warning);
        await Assert.That(fault.IsTransient).IsTrue();
        await Assert.That(fault.StreamId).IsNull();
        await Assert.That(fault.OperationId).IsNull();
        await Assert.That(fault.Exception).IsNull();
    }

    /// <summary>Verifies malformed faults cannot enter a diagnostic stream.</summary>
    /// <param name="scenario">The malformed field.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments("empty-code")]
    [Arguments("whitespace-code")]
    [Arguments("null-code")]
    [Arguments("null-message")]
    [Arguments("category")]
    [Arguments("category-high")]
    [Arguments("severity")]
    [Arguments("severity-high")]
    [Arguments("stream")]
    [Arguments("operation")]
    public async Task RejectsInvalidDiagnosticRecord(string scenario)
    {
        var fault = new OccasionallyConnectedFault("OC.Invalid", string.Empty, DateTimeOffset.UnixEpoch, null, null, null);
        fault = scenario switch
        {
            "empty-code" => fault with { Code = string.Empty },
            "whitespace-code" => fault with { Code = " " },
            "null-code" => WithoutProperty(fault, nameof(OccasionallyConnectedFault.Code)),
            "null-message" => WithoutProperty(fault, nameof(OccasionallyConnectedFault.Message)),
            "category" => fault with { Category = (FaultCategory)(-1) },
            "category-high" => fault with { Category = (FaultCategory)int.MaxValue },
            "severity" => fault with { Severity = (FaultSeverity)(-1) },
            "severity-high" => fault with { Severity = (FaultSeverity)int.MaxValue },
            "stream" => fault with { StreamId = default(StreamId) },
            _ => fault with { OperationId = default(OperationId) },
        };
        await Assert.That(fault.Validate).Throws<InvalidOperationException>();
    }

    /// <summary>Creates a malformed diagnostic record with a missing required property.</summary>
    /// <param name="fault">The valid diagnostic record.</param>
    /// <param name="propertyName">The property to omit.</param>
    /// <returns>The malformed record.</returns>
    /// <exception cref="InvalidOperationException">The expected property is unavailable.</exception>
    private static OccasionallyConnectedFault WithoutProperty(OccasionallyConnectedFault fault, string propertyName)
    {
        var copy = fault with { };
        var property = typeof(OccasionallyConnectedFault).GetProperty(propertyName)
            ?? throw new InvalidOperationException("The diagnostic property is unavailable.");
        property.SetValue(copy, null);
        return copy;
    }
}
