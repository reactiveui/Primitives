// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Captures a synchronous observer input as an owned serialized payload.</summary>
/// <typeparam name="TInput">The input value type.</typeparam>
/// <remarks>
/// The retained byte count must cover the whole captured envelope retained by the observer input bridge, including
/// payload bytes, envelope object overhead, and retained UTF-16 metadata strings. Implementations must calculate this
/// size without retaining caller-owned mutable input graphs.
/// The declared bound must remain valid until <see cref="Capture"/> returns. Callers that mutate shared input
/// concurrently with capture must synchronize that mutation. Mutation after <c>OnNext</c> returns is safe because
/// the bridge retains only the owned serialized payload and bounded metadata.
/// </remarks>
public interface IOccasionallyConnectedInputCapture<in TInput>
{
    /// <summary>Gets the maximum retained bytes needed to capture the supplied input without retaining it.</summary>
    /// <param name="value">The caller-owned input value.</param>
    /// <returns>The retained byte count reserved before <see cref="Capture"/> runs.</returns>
    /// <remarks>Implementations must avoid allocation and must not retain <paramref name="value"/>.</remarks>
    long GetRetainedByteCount(TInput value);

    /// <summary>Serializes the input into an owned payload envelope without retaining caller graphs.</summary>
    /// <param name="value">The caller-owned input value.</param>
    /// <returns>The owned serialized input payload.</returns>
    PayloadEnvelope Capture(TInput value);
}
