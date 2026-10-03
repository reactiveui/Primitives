// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected;
#endif

/// <summary>Connects the typed stream facade to internal context lifecycle management.</summary>
internal sealed partial class OccasionallyConnectedStream<TState, TInput> : IOccasionallyConnectedStreamLifecycle
{
}
