// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class DisposedWitness<T> : IObserver<T>
{
    public static readonly DisposedWitness<T> Instance = new();

    private DisposedWitness()
    {
    }

    public void OnCompleted() => throw new ObjectDisposedException(string.Empty);

    public void OnError(Exception error) => throw new ObjectDisposedException(string.Empty, error);

    public void OnNext(T value) => throw new ObjectDisposedException(string.Empty);
}
