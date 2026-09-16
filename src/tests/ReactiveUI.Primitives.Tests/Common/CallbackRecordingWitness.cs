// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Tests;

/// <summary>An observer that runs a callback for each value and records its terminal notification.</summary>
/// <typeparam name="T">The type of the observed values.</typeparam>
/// <param name="onNext">The callback run for each value.</param>
internal sealed class CallbackRecordingWitness<T>(Action<T> onNext) : IObserver<T>
{
    /// <summary>Gets the recorded error, if any.</summary>
    internal Exception? Error { get; private set; }

    /// <summary>Gets a value indicating whether completion was observed.</summary>
    internal bool IsCompleted => Completions != 0;

    /// <summary>Gets the number of completion callbacks observed.</summary>
    internal int Completions { get; private set; }

    /// <inheritdoc/>
    public void OnCompleted() => Completions++;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => Error = error;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => onNext(value);
}
