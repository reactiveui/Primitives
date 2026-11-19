// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internal;

/// <summary>Describes the outcome of pairing or queueing one incoming element inside a two-source <c>Zip</c> coordinator.</summary>
/// <typeparam name="TCounterpart">The counterpart queue element type.</typeparam>
/// <param name="HasPair"><see langword="true"/> when a counterpart value was dequeued and a pair should be emitted.</param>
/// <param name="Counterpart">The dequeued counterpart element when <paramref name="HasPair"/> is <see langword="true"/>.</param>
/// <param name="CompleteAfterPair"><see langword="true"/> when emitting this pair drains the completed counterpart queue and terminates the sequence.</param>
internal readonly record struct ZipDequeueOutcome<TCounterpart>(
    bool HasPair,
    TCounterpart Counterpart,
    bool CompleteAfterPair);
