// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Source that can count values through an operator-specific fast path.</summary>
internal interface ICountSource
{
    /// <summary>Subscribes a count observer directly to the underlying source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription cleanup.</returns>
    IDisposable SubscribeCount(IObserver<int> observer);

    /// <summary>Subscribes a long-count observer directly to the underlying source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription cleanup.</returns>
    IDisposable SubscribeLongCount(IObserver<long> observer);
}
