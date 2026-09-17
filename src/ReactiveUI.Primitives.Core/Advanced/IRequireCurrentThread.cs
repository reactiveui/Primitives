// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>A signal that reports whether it has to be subscribed to from the calling thread.</summary>
/// <typeparam name="T">The value type.</typeparam>
public interface IRequireCurrentThread<out T> : IObservable<T>
{
    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns><see langword="true"/> when the signal is bound to the subscribing thread.</returns>
    bool IsRequiredSubscribeOnCurrentThread();
}
