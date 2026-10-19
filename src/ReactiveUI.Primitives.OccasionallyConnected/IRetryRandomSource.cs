// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected;
#endif

/// <summary>Provides deterministic random values for retry delay jitter.</summary>
internal interface IRetryRandomSource
{
    /// <summary>Returns a value greater than or equal to zero and less than or equal to one.</summary>
    /// <returns>The next deterministic jitter value.</returns>
    double NextDouble();
}
