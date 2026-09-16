// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>The subscribers a subject fans its notifications out to, each delivering in the order the subject posted to it.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// Hold the field as a mutable struct and change it only under the owner's lock. Post each notification while holding that
/// lock, so every subscriber's order matches the order the owner fixed, then flush the returned
/// <see cref="SerializedBroadcast{T}"/> after releasing it; no observer runs while the lock is held. Notifications posted to a
/// subscriber before it is added, such as a replay, are delivered ahead of anything posted afterwards.
/// </remarks>
[DebuggerDisplay("SerializedBroadcaster: HasObservers = {HasObservers}")]
public record struct SerializedBroadcaster<T>
{
    /// <summary>Null, one witness, or a copy-on-write witness array.</summary>
    private object? _witnesses;

    /// <summary>Gets a value indicating whether at least one witness is registered.</summary>
    public bool HasObservers => Volatile.Read(ref _witnesses) is not null;

    /// <summary>Registers a witness; call it under the owner's lock.</summary>
    /// <param name="witness">The witness to register.</param>
    /// <exception cref="ArgumentNullException"><paramref name="witness"/> is <see langword="null"/>.</exception>
    public void Add(SerializedWitness<T> witness)
    {
        ArgumentExceptionHelper.ThrowIfNull(witness);

        var current = Volatile.Read(ref _witnesses);
        if (current is SerializedWitness<T>[] many)
        {
            var copy = new SerializedWitness<T>[many.Length + 1];
            Array.Copy(many, copy, many.Length);
            copy[many.Length] = witness;
            Volatile.Write(ref _witnesses, copy);
            return;
        }

        object next = current is SerializedWitness<T> single ? new[] { single, witness } : witness;
        Volatile.Write(ref _witnesses, next);
    }

    /// <summary>Unregisters a witness; call it under the owner's lock. A witness that is not registered is ignored.</summary>
    /// <param name="witness">The witness to unregister.</param>
    public void Remove(SerializedWitness<T> witness)
    {
        var current = Volatile.Read(ref _witnesses);
        if (ReferenceEquals(current, witness))
        {
            Volatile.Write(ref _witnesses, null);
            return;
        }

        if (current is not SerializedWitness<T>[] many)
        {
            return;
        }

        var index = Array.IndexOf(many, witness);
        if (index < 0)
        {
            return;
        }

        if (many.Length == 2)
        {
            Volatile.Write(ref _witnesses, many[index == 0 ? 1 : 0]);
            return;
        }

        var copy = new SerializedWitness<T>[many.Length - 1];
        Array.Copy(many, 0, copy, 0, index);
        Array.Copy(many, index + 1, copy, index, many.Length - index - 1);
        Volatile.Write(ref _witnesses, copy);
    }

    /// <summary>Unregisters every witness; call it under the owner's lock.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear() => Volatile.Write(ref _witnesses, null);

    /// <summary>Posts a value to every registered witness; call it under the owner's lock.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The batch to flush after releasing the lock.</returns>
    /// <remarks>
    /// A witness with nothing queued or delivering is claimed and receives the value directly on flush; a busy witness has
    /// the value queued behind its running delivery. Either way each witness receives values in the order they were posted.
    /// Always flush the returned batch: a claimed witness delivers nothing else until its batch is flushed.
    /// </remarks>
    public SerializedBroadcast<T> PostNext(T value)
    {
        var witnesses = Volatile.Read(ref _witnesses);
        if (witnesses is SerializedWitness<T> single)
        {
            if (single.TryClaim())
            {
                return new(single, value, 1UL);
            }

            _ = single.Post(value);
            return new(single);
        }

        if (witnesses is not SerializedWitness<T>[] many)
        {
            return default;
        }

        var claimed = 0UL;
        for (var i = 0; i < many.Length; i++)
        {
            if (i < SerializedBroadcast<T>.ClaimCapacity && many[i].TryClaim())
            {
                claimed |= 1UL << i;
                continue;
            }

            _ = many[i].Post(value);
        }

        return new(many, value, claimed);
    }

    /// <summary>Queues an error as the terminal notification on every registered witness; call it under the owner's lock.</summary>
    /// <param name="error">The error.</param>
    /// <returns>The batch to flush after releasing the lock.</returns>
    public SerializedBroadcast<T> PostError(Exception error)
    {
        var witnesses = Volatile.Read(ref _witnesses);
        if (witnesses is SerializedWitness<T> single)
        {
            _ = single.PostError(error);
        }
        else if (witnesses is SerializedWitness<T>[] many)
        {
            for (var i = 0; i < many.Length; i++)
            {
                _ = many[i].PostError(error);
            }
        }

        return new(witnesses);
    }

    /// <summary>Queues completion as the terminal notification on every registered witness; call it under the owner's lock.</summary>
    /// <returns>The batch to flush after releasing the lock.</returns>
    public SerializedBroadcast<T> PostCompleted()
    {
        var witnesses = Volatile.Read(ref _witnesses);
        if (witnesses is SerializedWitness<T> single)
        {
            _ = single.PostCompleted();
        }
        else if (witnesses is SerializedWitness<T>[] many)
        {
            for (var i = 0; i < many.Length; i++)
            {
                _ = many[i].PostCompleted();
            }
        }

        return new(witnesses);
    }
}
