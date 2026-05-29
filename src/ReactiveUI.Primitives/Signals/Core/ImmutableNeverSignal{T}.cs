// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the ImmutableNeverSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class ImmutableNeverSignal<T> : IRequireCurrentThread<T>
{
#pragma warning disable SA1401 // Fields should be private

    /// <summary>
    /// Executes the new operation.
    /// </summary>
    /// <returns>The result.</returns>
    internal static readonly ImmutableNeverSignal<T> Instance = new();
#pragma warning restore SA1401 // Fields should be private

    /// <summary>
    /// Executes the IsRequiredSubscribeOnCurrentThread operation.
    /// </summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>
    /// Executes the Subscribe operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer) =>
        Disposable.Empty;
}
