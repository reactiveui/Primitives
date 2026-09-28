// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>A sequencer that runs its work on one owning thread and can tell whether the calling thread is that thread.</summary>
/// <remarks>
/// UI sequencers implement this interface next to their sequencer interface. A caller that already runs on the
/// owning thread can do its work inline and skip a queued dispatch. The interface lives in the flavour-neutral core,
/// so the lean and the System.Reactive sequencers share it.
/// </remarks>
public interface IThreadAffineSequencer
{
    /// <summary>Returns whether the calling thread may run work for this sequencer inline.</summary>
    /// <returns><see langword="true"/> when the calling thread owns the sequencer; otherwise <see langword="false"/>.</returns>
    bool CheckAccess();
}
