// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>The element-type-agnostic view of a latest-value slot, so the coordinator can hold them all.</summary>
internal interface ICombineLatestSlot
{
    /// <summary>Subscribes the slot to the source it holds the latest value of.</summary>
    /// <returns>The source subscription.</returns>
    IDisposable Subscribe();

    /// <summary>Applies the oldest value this slot queued while another thread was delivering.</summary>
    void ApplyQueued();
}
