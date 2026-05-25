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
    public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action) =>
        throw new NotSupportedException();

    /// <inheritdoc/>
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action) =>
        throw new NotSupportedException();

    /// <inheritdoc/>
    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action) =>
        throw new NotSupportedException();
}
