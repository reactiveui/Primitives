// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Provides a sequencer test double that rejects scheduled work.
/// </summary>
internal sealed class EmptySequencer : ISequencer
{
    /// <summary>
    /// Gets the shared empty sequencer instance.
    /// </summary>
    public static EmptySequencer Instance { get; } = new();

    /// <inheritdoc/>
    public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

    /// <inheritdoc/>
    public long Timestamp => 0;

    /// <inheritdoc/>
    public void Schedule(IWorkItem item) =>
        throw new NotSupportedException();

    /// <inheritdoc/>
    public void Schedule(IWorkItem item, long dueTimestamp) =>
        throw new NotSupportedException();
}
