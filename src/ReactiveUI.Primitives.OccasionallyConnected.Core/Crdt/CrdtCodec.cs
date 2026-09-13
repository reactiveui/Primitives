// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Encodes and decodes built-in CRDT contracts using a deterministic bounded binary format.</summary>
public static partial class CrdtCodec
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
    public static byte[] EncodeState(CrdtState state) =>
        EncodeState(state, CrdtBounds.Default);

    /// <summary>Encodes a CRDT state.</summary>
    /// <param name="state">The state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The encoded bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT state is invalid.</exception>
    public static byte[] EncodeState(CrdtState state, CrdtBounds bounds)
    {
        CrdtFunctions.ValidateState(state, bounds);
        Writer writer = new(bounds);
        writer.WriteHeader(StatePayload);
        writer.WriteByte((byte)state.Kind);
        writer.WriteComponents(state.GCounterComponents);
        writer.WriteComponents(state.PNCounterPositiveComponents);
        writer.WriteComponents(state.PNCounterNegativeComponents);
        writer.WriteDotElements(state.DotBindings);
        writer.WriteDotElements(state.Tombstones);
        writer.WriteBytes(state.RegisterValueSpan);
        writer.WriteStamp(state.RegisterStamp);
        return writer.ToArray();
    }

    /// <summary>Decodes a CRDT state.</summary>
    /// <param name="payload">The encoded payload.</param>
    /// <returns>The decoded state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CrdtState DecodeState(ReadOnlyMemory<byte> payload) =>
        DecodeState(payload, CrdtBounds.Default);

    /// <summary>Decodes a CRDT state.</summary>
    /// <param name="payload">The encoded payload.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The decoded state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT payload is invalid.</exception>
    public static CrdtState DecodeState(ReadOnlyMemory<byte> payload, CrdtBounds bounds)
    {
        Reader reader = new(payload, bounds);
        reader.ReadHeader(StatePayload);
        var state = reader.ReadStateBody();
        reader.ThrowIfTrailingData();
        CrdtFunctions.ValidateState(state, bounds);
        return state;
    }

    /// <summary>Encodes a CRDT input.</summary>
    /// <param name="input">The input.</param>
    /// <returns>The encoded bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[] EncodeInput(CrdtInput input) =>
        EncodeInput(input, CrdtBounds.Default);

    /// <summary>Encodes a CRDT input.</summary>
    /// <param name="input">The input.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The encoded bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT input is invalid.</exception>
    public static byte[] EncodeInput(CrdtInput input, CrdtBounds bounds)
    {
        ValidateInput(input, bounds);
        Writer writer = new(bounds);
        writer.WriteHeader(InputPayload);
        writer.WriteInput(input);
        return writer.ToArray();
    }

    /// <summary>Decodes a CRDT input.</summary>
    /// <param name="payload">The encoded payload.</param>
    /// <returns>The decoded input.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CrdtInput DecodeInput(ReadOnlyMemory<byte> payload) =>
        DecodeInput(payload, CrdtBounds.Default);

    /// <summary>Decodes a CRDT input.</summary>
    /// <param name="payload">The encoded payload.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The decoded input.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT payload is invalid.</exception>
    public static CrdtInput DecodeInput(ReadOnlyMemory<byte> payload, CrdtBounds bounds)
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
    public static void ValidateInput(CrdtInput input) =>
        ValidateInput(input, CrdtBounds.Default);

    /// <summary>Validates a CRDT input.</summary>
    /// <param name="input">The input.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT input is invalid.</exception>
    public static void ValidateInput(CrdtInput input, CrdtBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(input);
        if (input.Kind == CrdtInputKind.Mutation && input.Mutation is not null && input.State is null)
        {
            ValidateMutation(input.Mutation, bounds);
            return;
        }

        if (input.Kind == CrdtInputKind.AuthoritativeState && input.State is not null && input.Mutation is null)
        {
            CrdtFunctions.ValidateState(input.State, bounds);
            return;
        }

        throw new InvalidOperationException("The CRDT input shape is invalid.");
    }

    /// <summary>Validates a mutation.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateMutation(CrdtMutation mutation, CrdtBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(mutation);
        ArgumentExceptionHelper.ThrowIfNull(bounds);
        bounds.Validate();
        ValidateMutationShape(mutation);
        ValidateCounterMutation(mutation, bounds);
        ValidateMutationBytes(mutation, bounds);
        ValidateObservedDots(mutation, bounds);
        CrdtFunctions.ValidateWriteStamp(mutation.RegisterStamp, bounds);
    }

    /// <summary>Validates counter mutation actor and component shape.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The identity bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateCounterMutation(CrdtMutation mutation, CrdtBounds bounds)
    {
        if (mutation.Kind is not (CrdtMutationKind.GCounterSet or CrdtMutationKind.PNCounterSet))
        {
            return;
        }

        CrdtFunctions.ValidateClientId(mutation.ActorId, bounds);

        if (mutation.GCounterComponent >= 0 && mutation.PNCounterPositiveComponent >= 0 && mutation.PNCounterNegativeComponent >= 0)
        {
            return;
        }

        throw new InvalidOperationException("CRDT counter mutation components must be nonnegative.");
    }

    /// <summary>Validates mutation byte bounds.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateMutationBytes(CrdtMutation mutation, CrdtBounds bounds)
    {
        if (mutation.ByteLength <= bounds.MaximumRegisterBytes && (mutation.Kind == CrdtMutationKind.LwwRegisterSet || mutation.ByteLength <= bounds.MaximumElementBytes))
        {
            return;
        }

        throw new InvalidOperationException("The CRDT mutation bytes exceed configured bounds.");
    }

    /// <summary>Validates observed dot count bounds.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateObservedDots(CrdtMutation mutation, CrdtBounds bounds)
    {
        if (mutation.ObservedDots.Count <= bounds.MaximumTombstones)
        {
            HashSet<CrdtDot> observed = [];
            foreach (var dot in mutation.ObservedDots)
            {
                CrdtFunctions.ValidateDot(dot, bounds);
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
