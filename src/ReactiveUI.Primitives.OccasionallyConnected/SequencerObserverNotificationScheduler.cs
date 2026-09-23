// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Adapts public sequencer scheduling to observer notification dispatch.</summary>
internal sealed class SequencerObserverNotificationScheduler : IObserverNotificationScheduler
{
    /// <summary>The public sequencer dependency.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>Initializes a new instance of the <see cref="SequencerObserverNotificationScheduler"/> class.</summary>
    /// <param name="sequencer">The public sequencer dependency.</param>
    internal SequencerObserverNotificationScheduler(ISequencer sequencer)
    {
        ArgumentExceptionHelper.ThrowIfNull(sequencer);
        _sequencer = sequencer;
    }

    /// <inheritdoc />
    public void Schedule(IWorkItem item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);
        _sequencer.Schedule(item);
    }
}
