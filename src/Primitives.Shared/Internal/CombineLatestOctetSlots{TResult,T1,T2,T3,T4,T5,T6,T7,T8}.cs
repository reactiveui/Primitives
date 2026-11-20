// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;
#else
using ReactiveUI.Primitives.Advanced;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Holds the eight attached <see cref="CombineLatestSlot{TResult, T}"/> instances corresponding to a <see cref="CombineLatestOctet{T1, T2, T3, T4, T5, T6, T7, T8}"/>.</summary>
/// <typeparam name="TResult">The combined result type.</typeparam>
/// <typeparam name="T1">The first slot element type.</typeparam>
/// <typeparam name="T2">The second slot element type.</typeparam>
/// <typeparam name="T3">The third slot element type.</typeparam>
/// <typeparam name="T4">The fourth slot element type.</typeparam>
/// <typeparam name="T5">The fifth slot element type.</typeparam>
/// <typeparam name="T6">The sixth slot element type.</typeparam>
/// <typeparam name="T7">The seventh slot element type.</typeparam>
/// <typeparam name="T8">The eighth slot element type.</typeparam>
/// <param name="Slot1">The first attached slot.</param>
/// <param name="Slot2">The second attached slot.</param>
/// <param name="Slot3">The third attached slot.</param>
/// <param name="Slot4">The fourth attached slot.</param>
/// <param name="Slot5">The fifth attached slot.</param>
/// <param name="Slot6">The sixth attached slot.</param>
/// <param name="Slot7">The seventh attached slot.</param>
/// <param name="Slot8">The eighth attached slot.</param>
[SuppressMessage(
    "Maintainability",
    "SST1472:Signatures should not declare too many parameters",
    Justification = "Holds the eight typed slots for an octet of wide-arity combine-latest sources.")]
internal readonly record struct CombineLatestOctetSlots<TResult, T1, T2, T3, T4, T5, T6, T7, T8>(
    CombineLatestSlot<TResult, T1> Slot1,
    CombineLatestSlot<TResult, T2> Slot2,
    CombineLatestSlot<TResult, T3> Slot3,
    CombineLatestSlot<TResult, T4> Slot4,
    CombineLatestSlot<TResult, T5> Slot5,
    CombineLatestSlot<TResult, T6> Slot6,
    CombineLatestSlot<TResult, T7> Slot7,
    CombineLatestSlot<TResult, T8> Slot8);
