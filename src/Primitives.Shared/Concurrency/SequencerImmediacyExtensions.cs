// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Concurrency;
#else
namespace ReactiveUI.Primitives.Concurrency;
#endif

/// <summary>Reports whether a sequencer runs work inline on the calling thread.</summary>
public static class SequencerImmediacyExtensions
{
    /// <summary>Immediacy check for a sequencer.</summary>
    /// <param name="sequencer">The sequencer to test; may be <see langword="null"/>.</param>
    extension(ISequencer? sequencer)
    {
        /// <summary>Gets a value indicating whether the sequencer is the immediate sequencer.</summary>
        /// <value><see langword="true"/> when the sequencer runs work inline on the calling thread; <see langword="false"/> for any other sequencer or <see langword="null"/>.</value>
        public bool IsImmediate
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get =>
#if REACTIVE_SHIM
                sequencer is global::System.Reactive.Concurrency.ImmediateScheduler;
#else
                sequencer is ImmediateSequencer;
#endif
        }
    }
}
