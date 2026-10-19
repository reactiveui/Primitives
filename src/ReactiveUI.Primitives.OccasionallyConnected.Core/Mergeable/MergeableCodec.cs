// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

/// <summary>Encodes and decodes built-in CRDT contracts using a deterministic bounded binary format.</summary>
public static partial class MergeableCodec
{
    /// <summary>The first magic byte.</summary>
    private const byte Magic0 = (byte)'R';

    /// <summary>The second magic byte.</summary>
    private const byte Magic1 = (byte)'C';

    /// <summary>The third magic byte.</summary>
    private const byte Magic2 = (byte)'D';

    /// <summary>The fourth magic byte.</summary>
    private const byte Magic3 = (byte)'T';

    /// <summary>The supported payload version.</summary>
    private const byte Version = 1;

    /// <summary>The state payload version carrying OR-set checkpoint knowledge.</summary>
    private const byte CheckpointVersion = 2;

    /// <summary>The payload discriminator for state.</summary>
    private const byte StatePayload = 1;

    /// <summary>The payload discriminator for input.</summary>
    private const byte InputPayload = 2;

    /// <summary>The number of bits in one byte.</summary>
    private const int BitsPerByte = 8;

    /// <summary>The number of bytes in a 16-bit integer.</summary>
    private const int Int16ByteCount = 2;

    /// <summary>The number of bytes in a 32-bit integer.</summary>
    private const int Int32ByteCount = 4;

    /// <summary>The number of bytes in a 64-bit integer.</summary>
    private const int Int64ByteCount = 8;

    /// <summary>Strict UTF-8 used by the codec.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>Encodes a CRDT state.</summary>
    /// <param name="state">The state.</param>
    /// <returns>The encoded bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[] EncodeState(MergeableState state) =>
        EncodeState(state, MergeableBounds.Default);

    /// <summary>Encodes a CRDT state.</summary>
    /// <param name="state">The state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The encoded bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT state is invalid.</exception>
    public static byte[] EncodeState(MergeableState state, MergeableBounds bounds)
    {
        MergeableFunctions.ValidateState(state, bounds);
        Writer writer = new(bounds);
        writer.WriteHeader(StatePayload, state.AddWinsSetFrontier.Count != 0);
        writer.WriteStateBody(state);
        return writer.ToArray();
    }

    /// <summary>Decodes a CRDT state.</summary>
    /// <param name="payload">The encoded payload.</param>
    /// <returns>The decoded state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MergeableState DecodeState(ReadOnlyMemory<byte> payload) =>
        DecodeState(payload, MergeableBounds.Default);

    /// <summary>Decodes a CRDT state.</summary>
    /// <param name="payload">The encoded payload.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The decoded state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT payload is invalid.</exception>
    public static MergeableState DecodeState(ReadOnlyMemory<byte> payload, MergeableBounds bounds)
    {
        Reader reader = new(payload, bounds);
        reader.ReadHeader(StatePayload);
        var state = reader.ReadStateBody();
        reader.ThrowIfTrailingData();
        MergeableFunctions.ValidateState(state, bounds);
        return state;
    }

    /// <summary>Encodes a CRDT input.</summary>
    /// <param name="input">The input.</param>
    /// <returns>The encoded bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[] EncodeInput(MergeableInput input) =>
        EncodeInput(input, MergeableBounds.Default);

    /// <summary>Encodes a CRDT input.</summary>
    /// <param name="input">The input.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The encoded bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT input is invalid.</exception>
    public static byte[] EncodeInput(MergeableInput input, MergeableBounds bounds)
    {
        ValidateInput(input, bounds);
        Writer writer = new(bounds);
        writer.WriteHeader(InputPayload, input.Value is MergeableState { AddWinsSetFrontier.Count: > 0 });
        writer.WriteInput(input);
        return writer.ToArray();
    }

    /// <summary>Decodes a CRDT input.</summary>
    /// <param name="payload">The encoded payload.</param>
    /// <returns>The decoded input.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MergeableInput DecodeInput(ReadOnlyMemory<byte> payload) =>
        DecodeInput(payload, MergeableBounds.Default);

    /// <summary>Decodes a CRDT input.</summary>
    /// <param name="payload">The encoded payload.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The decoded input.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT payload is invalid.</exception>
    public static MergeableInput DecodeInput(ReadOnlyMemory<byte> payload, MergeableBounds bounds)
    {
        Reader reader = new(payload, bounds);
        reader.ReadHeader(InputPayload);
        var input = reader.ReadInput();
        reader.ThrowIfTrailingData();
        ValidateInput(input, bounds);
        return input;
    }

    /// <summary>Validates a CRDT input.</summary>
    /// <param name="input">The input.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ValidateInput(MergeableInput input) =>
        ValidateInput(input, MergeableBounds.Default);

    /// <summary>Validates a CRDT input.</summary>
    /// <param name="input">The input.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT input is invalid.</exception>
    public static void ValidateInput(MergeableInput input, MergeableBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(input);
        if (input.Value is MergeableChange change)
        {
            ValidateMutation(change, bounds);
            return;
        }

        MergeableFunctions.ValidateState((MergeableState)input.Value, bounds);
    }

    /// <summary>Validates a mutation.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateMutation(MergeableChange mutation, MergeableBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(mutation);
        ArgumentExceptionHelper.ThrowIfNull(bounds);
        bounds.Validate();
        ValidateCounterMutation(mutation, bounds);
        ValidateMutationBytes(mutation, bounds);
        ValidateObservedDots(mutation, bounds);
        MergeableFunctions.ValidateWriteStamp(mutation.LatestWriteStamp, bounds);
    }

    /// <summary>Validates counter actor identity against the configured bounds.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The identity bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateCounterMutation(MergeableChange mutation, MergeableBounds bounds)
    {
        if (mutation.ToKind() is not (MergeableChangeKind.IncrementOnlyCounterSet or MergeableChangeKind.CounterSet))
        {
            return;
        }

        MergeableFunctions.ValidateClientId(mutation.ClientId, bounds);
    }

    /// <summary>Validates mutation byte bounds.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateMutationBytes(MergeableChange mutation, MergeableBounds bounds)
    {
        if (mutation.ByteLength <= bounds.MaximumRegisterBytes && (mutation.ToKind() == MergeableChangeKind.LastWriterWinsValueSet || mutation.ByteLength <= bounds.MaximumElementBytes))
        {
            return;
        }

        throw new InvalidOperationException("The CRDT mutation bytes exceed configured bounds.");
    }

    /// <summary>Validates observed dot count bounds.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateObservedDots(MergeableChange mutation, MergeableBounds bounds)
    {
        if (mutation.TagsToRemove.Count <= bounds.MaximumTombstones)
        {
            HashSet<ElementTag> observed = [];
            foreach (var dot in mutation.TagsToRemove)
            {
                MergeableFunctions.ValidateDot(dot, bounds);
                if (!observed.Add(dot))
                {
                    throw new InvalidOperationException("CRDT observed dots must be unique.");
                }
            }

            return;
        }

        throw new InvalidOperationException("The CRDT observed dot count exceeds configured bounds.");
    }
}
