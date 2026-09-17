// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>A signal whose next value can be awaited.</summary>
/// <typeparam name="T">The value type.</typeparam>
public interface IAwaitSignal<T> : ISignal<T>, System.Runtime.CompilerServices.INotifyCompletion
{
    /// <summary>Gets a value indicating whether the signal has terminated, so an awaiting continuation runs inline.</summary>
    bool IsCompleted { get; }

    /// <summary>Gets the awaiter for this signal.</summary>
    /// <returns>The signal itself, which acts as its own awaiter.</returns>
    IAwaitSignal<T> GetAwaiter();

    /// <summary>Gets the awaited value, rethrowing the terminal error when the signal faulted.</summary>
    /// <returns>The value the signal terminated with.</returns>
    T GetResult();
}
