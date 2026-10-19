// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies why a remote synchronization result failed batch validation.</summary>
public enum SyncBatchValidationError
{
    /// <summary>The batch or result is malformed.</summary>
    MalformedBatch = 0,

    /// <summary>The result batch identifier does not match the pushed batch.</summary>
    MismatchingBatchId = 1,

    /// <summary>A result item is malformed.</summary>
    MalformedOperationResult = 2,

    /// <summary>The batch contains duplicate operation identifiers.</summary>
    DuplicateOperation = 3,

    /// <summary>The result contains duplicate operation identifiers.</summary>
    DuplicateOperationResult = 4,

    /// <summary>The result omitted an operation from the pushed batch.</summary>
    OmittedOperationResult = 5,

    /// <summary>The result references an operation that was not in the pushed batch.</summary>
    UnknownOperationResult = 6,

    /// <summary>The batch contains operations for multiple streams.</summary>
    MixedStreams = 7,

    /// <summary>The batch contains duplicate client sequence values.</summary>
    DuplicateClientSequence = 8,
}
