// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Decides how a full outbox admits a publication that uses <see cref="BufferStrategy.Custom"/>.</summary>
/// <remarks>
/// A policy must be deterministic: it must return the same decision for the same context, must not perform I/O, and
/// must not modify external state. The context exposes only metadata, never payload bytes. A policy may block the
/// publisher, reject the publication, or evict one of the listed candidates. Selecting an operation that is not listed,
/// or one that is leased for upload when the eviction runs, is a policy error.
/// </remarks>
public interface IBufferOverflowPolicy
{
    /// <summary>Decides how to admit a publication that does not fit the outbox.</summary>
    /// <param name="context">The immutable overflow metadata.</param>
    /// <returns>The overflow decision.</returns>
    BufferOverflowDecision Decide(BufferOverflowContext context);
}
