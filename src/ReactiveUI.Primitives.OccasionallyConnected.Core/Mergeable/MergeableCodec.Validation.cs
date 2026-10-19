// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

/// <summary>Validates closed mutation field sets for the CRDT wire format.</summary>
public static partial class MergeableCodec
{
    /// <summary>Identifies populated mutation fields.</summary>
    [Flags]
    private enum MutationFields
    {
        /// <summary>No populated fields.</summary>
        None = 0,
        /// <summary>The authenticated counter actor.</summary>
        Actor = 1 << 0,
        /// <summary>The G-counter component.</summary>
        IncrementOnlyCounter = 1 << 1,
        /// <summary>The PN-counter positive component.</summary>
        Positive = 1 << 2,
        /// <summary>The PN-counter negative component.</summary>
        Negative = 1 << 3,
        /// <summary>Element or register bytes.</summary>
        Bytes = 1 << 4,
        /// <summary>Observed removal dots.</summary>
        Dots = 1 << 5,
        /// <summary>A register stamp.</summary>
        Stamp = 1 << 6,
    }

    /// <summary>Rejects unknown kinds and fields belonging to another mutation kind.</summary>
    /// <param name="kind">The wire mutation kind.</param>
    /// <param name="fields">The populated wire fields.</param>
    /// <exception cref="InvalidOperationException">The mutation contains unsupported fields.</exception>
    private static void ValidateMutationShape(MergeableChangeKind kind, MutationFields fields)
    {
        var allowed = kind switch
        {
            MergeableChangeKind.IncrementOnlyCounterSet => MutationFields.Actor | MutationFields.IncrementOnlyCounter,
            MergeableChangeKind.CounterSet => MutationFields.Actor | MutationFields.Positive | MutationFields.Negative,
            MergeableChangeKind.AddWinsSetAdd => MutationFields.Bytes,
            MergeableChangeKind.AddWinsSetRemove => MutationFields.Bytes | MutationFields.Dots,
            MergeableChangeKind.LastWriterWinsValueSet => MutationFields.Bytes | MutationFields.Stamp,
            _ => throw new InvalidOperationException("The CRDT mutation kind is not supported."),
        };
        if ((fields & ~allowed) == MutationFields.None)
        {
            return;
        }

        throw new InvalidOperationException("The CRDT mutation contains fields for another kind.");
    }

    /// <summary>Computes which optional mutation fields carry data.</summary>
    /// <param name="mutation">The wire payload fields.</param>
    /// <returns>The populated field set.</returns>
    private static MutationFields GetPopulatedMutationFields(MutationPayload mutation)
    {
        var fields = mutation.ClientId is null ? MutationFields.None : MutationFields.Actor;
        fields |= mutation.IncrementOnlyTotal == 0 ? MutationFields.None : MutationFields.IncrementOnlyCounter;
        fields |= mutation.IncrementTotal == 0 ? MutationFields.None : MutationFields.Positive;
        fields |= mutation.DecrementTotal == 0 ? MutationFields.None : MutationFields.Negative;
        fields |= mutation.Bytes.IsEmpty ? MutationFields.None : MutationFields.Bytes;
        fields |= mutation.TagsToRemove.Count == 0 ? MutationFields.None : MutationFields.Dots;
        fields |= mutation.LatestWriteStamp is null ? MutationFields.None : MutationFields.Stamp;
        return fields;
    }

    /// <summary>Holds untrusted wire slots until their kind and shape have been checked.</summary>
    /// <param name="ClientId">The optional counter actor.</param>
    /// <param name="IncrementOnlyTotal">The increment-only component.</param>
    /// <param name="IncrementTotal">The positive counter component.</param>
    /// <param name="DecrementTotal">The negative counter component.</param>
    /// <param name="Bytes">The element or register bytes.</param>
    /// <param name="TagsToRemove">The observed removal dots.</param>
    /// <param name="LatestWriteStamp">The optional register stamp.</param>
    private sealed record MutationPayload(
        string? ClientId,
        long IncrementOnlyTotal,
        long IncrementTotal,
        long DecrementTotal,
        ReadOnlyMemory<byte> Bytes,
        IReadOnlyList<ElementTag> TagsToRemove,
        ConflictWriteStamp? LatestWriteStamp);
}
