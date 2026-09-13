// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Represents a corrupt persisted payload row that should quarantine its stream.</summary>
internal sealed class SqlitePayloadQuarantineException : InvalidOperationException
{
    /// <summary>Empty evidence used by conventional exception constructors.</summary>
    private static readonly LocalPayloadQuarantineEvidence EmptyEvidence = new(null, null, null, 0, null, ReadOnlyMemory<byte>.Empty);

    /// <summary>Initializes a new instance of the <see cref="SqlitePayloadQuarantineException"/> class.</summary>
    internal SqlitePayloadQuarantineException()
        : base("The SQLite payload row is invalid.")
    {
        Evidence = EmptyEvidence;
    }

    /// <summary>Initializes a new instance of the <see cref="SqlitePayloadQuarantineException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    internal SqlitePayloadQuarantineException(string message)
        : base(message)
    {
        Evidence = EmptyEvidence;
    }

    /// <summary>Initializes a new instance of the <see cref="SqlitePayloadQuarantineException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal SqlitePayloadQuarantineException(string message, Exception innerException)
        : base(message, innerException)
    {
        Evidence = EmptyEvidence;
    }

    /// <summary>Initializes a new instance of the <see cref="SqlitePayloadQuarantineException"/> class.</summary>
    /// <param name="evidence">The bounded raw payload evidence.</param>
    /// <param name="innerException">The read failure that identified the corrupt row.</param>
    internal SqlitePayloadQuarantineException(LocalPayloadQuarantineEvidence evidence, Exception innerException)
        : this(evidence, null, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SqlitePayloadQuarantineException"/> class.</summary>
    /// <param name="evidence">The bounded raw payload evidence.</param>
    /// <param name="operationId">The affected operation identifier, when known.</param>
    /// <param name="innerException">The read failure that identified the corrupt row.</param>
    internal SqlitePayloadQuarantineException(LocalPayloadQuarantineEvidence evidence, OperationId? operationId, Exception innerException)
        : base("The SQLite payload row is invalid.", innerException)
    {
        Evidence = evidence;
        OperationId = operationId;
    }

    /// <summary>Gets the bounded raw payload evidence.</summary>
    internal LocalPayloadQuarantineEvidence Evidence { get; }

    /// <summary>Gets the affected operation identifier, when known.</summary>
    internal OperationId? OperationId { get; }

    /// <summary>Resolves the affected operation identifier for a corrupt leased row.</summary>
    /// <param name="fallback">The selected lease operation identifier to use when the corrupt row did not identify itself.</param>
    /// <returns>The affected operation identifier.</returns>
    internal OperationId ResolveOperationId(OperationId fallback) => OperationId ?? fallback;
}
