// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class ThrowWitness<T> : IObserver<T>
{
    public static readonly ThrowWitness<T> Instance = new();

    private ThrowWitness()
    {
    }

    public void OnCompleted()
    {
    }

    public void OnError(Exception error) => error.Rethrow();

    public void OnNext(T value)
    {
    }
}
