// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// IAwaitSignal.
/// </summary>
/// <typeparam name="T">The Type of Signal.</typeparam>
/// <seealso cref="ReactiveUI.Primitives.Signals.ISignal&lt;T&gt;" />
/// <seealso cref="System.Runtime.CompilerServices.INotifyCompletion" />
public interface IAwaitSignal<T> : ISignal<T>, System.Runtime.CompilerServices.INotifyCompletion
{
    /// <summary>
    /// Gets a value indicating whether this instance is completed.
    /// </summary>
    /// <value>
    ///   <c>true</c> if this instance is completed; otherwise, <c>false</c>.
    /// </value>
    bool IsCompleted { get; }

    /// <summary>
    /// Gets the awaiter.
    /// </summary>
    /// <returns>An IAwaitSignal.</returns>
    IAwaitSignal<T> GetAwaiter();

    /// <summary>
    /// Gets the result.
    /// </summary>
    /// <returns>A value of T.</returns>
    T GetResult();
}
