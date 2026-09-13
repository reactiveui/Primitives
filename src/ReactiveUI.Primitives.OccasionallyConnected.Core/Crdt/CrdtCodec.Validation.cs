// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Validates closed mutation field sets for the CRDT wire format.</summary>
public static partial class CrdtCodec
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
        GCounter = 1 << 1,
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
    /// <param name="mutation">The mutation to validate.</param>
    /// <exception cref="InvalidOperationException">The mutation contains unsupported fields.</exception>
    private static void ValidateMutationShape(CrdtMutation mutation)
    {
        var allowed = mutation.Kind switch
        {
            CrdtMutationKind.GCounterSet => MutationFields.Actor | MutationFields.GCounter,
            CrdtMutationKind.PNCounterSet => MutationFields.Actor | MutationFields.Positive | MutationFields.Negative,
            CrdtMutationKind.ORSetAdd => MutationFields.Bytes,
            CrdtMutationKind.ORSetRemove => MutationFields.Bytes | MutationFields.Dots,
            CrdtMutationKind.LwwRegisterSet => MutationFields.Bytes | MutationFields.Stamp,
            _ => throw new InvalidOperationException("The CRDT mutation kind is not supported."),
        };
        if ((GetPopulatedMutationFields(mutation) & ~allowed) == MutationFields.None)
        {
            return;
        }

        throw new InvalidOperationException("The CRDT mutation contains fields for another kind.");
    }

    /// <summary>Computes which optional mutation fields carry data.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <returns>The populated field set.</returns>
    private static MutationFields GetPopulatedMutationFields(CrdtMutation mutation)
    {
        var fields = mutation.ActorId is null ? MutationFields.None : MutationFields.Actor;
        fields |= mutation.GCounterComponent == 0 ? MutationFields.None : MutationFields.GCounter;
        fields |= mutation.PNCounterPositiveComponent == 0 ? MutationFields.None : MutationFields.Positive;
        fields |= mutation.PNCounterNegativeComponent == 0 ? MutationFields.None : MutationFields.Negative;
        fields |= mutation.ByteLength == 0 ? MutationFields.None : MutationFields.Bytes;
        fields |= mutation.ObservedDots.Count == 0 ? MutationFields.None : MutationFields.Dots;
        fields |= mutation.RegisterStamp is null ? MutationFields.None : MutationFields.Stamp;
        return fields;
    }
}
