// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

/// <summary>
/// Observer that rejects every notification because the subscription has already been disposed.
/// </summary>
/// <typeparam name="T">The observed value type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class DisposedWitness<T> : IObserver<T>
{
    /// <summary>
    /// Gets the shared disposed witness instance.
    /// </summary>
    public static readonly DisposedWitness<T> Instance = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="DisposedWitness{T}"/> class.
    /// </summary>
    private DisposedWitness()
    {
    }

    /// <inheritdoc/>
    public void OnCompleted() => throw new ObjectDisposedException(string.Empty);

    /// <inheritdoc/>
    public void OnError(Exception error) => throw new ObjectDisposedException(string.Empty, error);

    /// <inheritdoc/>
    public void OnNext(T value) => throw new ObjectDisposedException(string.Empty);
}
