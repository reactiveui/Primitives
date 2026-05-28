// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Observable adapter backed by an async enumerable.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
internal interface IAsyncEnumerableBackedSignal<out T> : IObservable<T>
{
    /// <summary>
    /// Gets the source async enumerable.
    /// </summary>
    IAsyncEnumerable<T> Values { get; }

    /// <summary>
    /// Gets the cancellation token used by the adapter.
    /// </summary>
    CancellationToken CancellationToken { get; }
}
#endif
