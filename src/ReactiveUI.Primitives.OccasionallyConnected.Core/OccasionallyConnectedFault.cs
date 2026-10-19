// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a typed operational fault without exposing payload contents by default.</summary>
/// <param name="Code">The stable fault code.</param>
/// <param name="Message">The diagnostic message.</param>
/// <param name="OccurredAtUtc">The time the fault was observed.</param>
/// <param name="StreamId">The optional related stream identifier.</param>
/// <param name="OperationId">The optional related operation identifier.</param>
/// <param name="Exception">The optional local exception.</param>
[System.Diagnostics.DebuggerDisplay("{Code,nq}")]
public sealed record OccasionallyConnectedFault(
    string Code,
    string Message,
    DateTimeOffset OccurredAtUtc,
    StreamId? StreamId,
    OperationId? OperationId,
    Exception? Exception)
{
    /// <summary>Gets the fault's component category.</summary>
    public FaultCategory Category { get; init; } = FaultCategory.InternalInvariant;

    /// <summary>Gets the impact of the fault.</summary>
    public FaultSeverity Severity { get; init; } = FaultSeverity.Error;

    /// <summary>Gets whether retry or renewed credentials may recover the failed operation.</summary>
    public bool IsTransient { get; init; }

    /// <summary>Validates a fault before it is admitted to a diagnostic stream.</summary>
    /// <exception cref="InvalidOperationException">A fault field is malformed.</exception>
    public void Validate()
    {
        ValidateText();
        ValidateClassification();
        ValidateIdentifiers();
    }

    /// <summary>Validates the stable fault code and diagnostic message.</summary>
    /// <exception cref="InvalidOperationException">Required diagnostic text is missing.</exception>
    private void ValidateText()
    {
        if (string.IsNullOrWhiteSpace(Code))
        {
            throw new InvalidOperationException("A stable fault code is required.");
        }

        if (Message is not null)
        {
            return;
        }

        throw new InvalidOperationException("A diagnostic message cannot be null.");
    }

    /// <summary>Validates the defined category and severity values.</summary>
    /// <exception cref="InvalidOperationException">The category or severity is undefined.</exception>
    private void ValidateClassification()
    {
#if NET8_0_OR_GREATER
        var categoryDefined = Enum.IsDefined(Category);
#else
        var categoryDefined = Enum.IsDefined(typeof(FaultCategory), Category);
#endif
        if (!categoryDefined)
        {
            throw new InvalidOperationException("Fault category must be a defined value.");
        }

        if (Severity is FaultSeverity.Information or FaultSeverity.Warning or FaultSeverity.Error or FaultSeverity.Critical)
        {
            return;
        }

        throw new InvalidOperationException("Fault severity must be a defined value.");
    }

    /// <summary>Validates identifiers when a fault is correlated to a stream or operation.</summary>
    /// <exception cref="InvalidOperationException">An optional identifier is present but invalid.</exception>
    private void ValidateIdentifiers()
    {
        if (StreamId is { } streamId)
        {
            OccasionallyConnectedOptionsValidation.ValidateStreamId(streamId, nameof(StreamId));
        }

        if (OperationId is not { Value: var operationId } || operationId != Guid.Empty)
        {
            return;
        }

        throw new InvalidOperationException("OperationId must be non-empty when supplied.");
    }
}
