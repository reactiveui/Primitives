// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected;
#endif

/// <summary>Describes a stream registered with the internal fair scheduler.</summary>
/// <param name="StreamId">The stream identifier.</param>
/// <param name="Weight">The relative scheduling weight.</param>
internal readonly record struct FairStreamRegistration(StreamId StreamId, int Weight);
