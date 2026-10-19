// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected;
#endif

/// <summary>Prepares a participant before the engine activates its remote work during global startup.</summary>
internal interface IEngineStartupParticipant
{
    /// <summary>Gets whether direct engine startup should initialize and activate this participant.</summary>
    bool InitializeOnEngineStart { get; }

    /// <summary>Completes durable participant initialization before receive and upload pumps can run.</summary>
    /// <returns>The initialization operation.</returns>
    ValueTask InitializeForEngineStartAsync();
}
