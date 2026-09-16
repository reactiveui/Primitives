// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>The witnesses a <see cref="SerializedBroadcaster{T}"/> posted a notification to, delivered by <see cref="Flush"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>The default instance holds no witnesses and flushes nothing.</remarks>
[DebuggerDisplay("SerializedBroadcast: {_witnesses}, Claimed = {_claimed}")]
public readonly record struct SerializedBroadcast<T>
{
    /// <summary>The number of leading witnesses whose claim fits in the claim mask; later witnesses always have the value queued.</summary>
    internal const int ClaimCapacity = 64;

    /// <summary>Null, one witness, or a witness array.</summary>
    private readonly object? _witnesses;

    /// <summary>The value delivered directly to the claimed witnesses.</summary>
    private readonly T _value;

    /// <summary>A bit per leading witness that was claimed and receives <see cref="_value"/> directly.</summary>
    private readonly ulong _claimed;

    /// <summary>Initializes a new instance of the <see cref="SerializedBroadcast{T}"/> struct.</summary>
    /// <param name="witnesses">Null, one witness, or a witness array.</param>
    internal SerializedBroadcast(object? witnesses)
    {
        _witnesses = witnesses;
        _value = default!;
    }

    /// <summary>Initializes a new instance of the <see cref="SerializedBroadcast{T}"/> struct.</summary>
    /// <param name="witnesses">One witness or a witness array.</param>
    /// <param name="value">The value delivered directly to the claimed witnesses.</param>
    /// <param name="claimed">A bit per leading witness that was claimed.</param>
    internal SerializedBroadcast(object witnesses, T value, ulong claimed)
    {
        _witnesses = witnesses;
        _value = value;
        _claimed = claimed;
    }

    /// <summary>Delivers the posted notifications through each witness, on the calling thread or the thread already delivering to it.</summary>
    /// <remarks>
    /// Call it after releasing the lock the notifications were posted under. Every witness is served even when one observer
    /// throws, since a claimed delivery left undelivered would stall that subscriber; the first failure is rethrown afterwards.
    /// </remarks>
    public void Flush()
    {
        if (_witnesses is SerializedWitness<T> single)
        {
            FlushWitness(single, 0);
            return;
        }

        if (_witnesses is not SerializedWitness<T>[] many)
        {
            return;
        }

        ExceptionDispatchInfo? failure = null;
        for (var i = 0; i < many.Length; i++)
        {
            try
            {
                FlushWitness(many[i], i);
            }
            catch (Exception error)
            {
                failure ??= ExceptionDispatchInfo.Capture(error);
            }
        }

        failure?.Throw();
    }

    /// <summary>Delivers the value to a claimed witness, or flushes what was queued on it.</summary>
    /// <param name="witness">The witness.</param>
    /// <param name="index">The witness position, which selects its claim bit.</param>
    private void FlushWitness(SerializedWitness<T> witness, int index)
    {
        if (index < ClaimCapacity && (_claimed & (1UL << index)) != 0)
        {
            witness.DeliverClaimed(_value);
            return;
        }

        witness.Flush();
    }
}
