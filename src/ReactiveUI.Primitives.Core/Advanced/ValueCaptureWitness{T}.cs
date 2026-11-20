// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer that captures the last value seen during synchronous subscribe.</summary>
/// <typeparam name="T">The element type of the source.</typeparam>
[System.Diagnostics.DebuggerDisplay("ValueCaptureWitness: Value = {Value}")]
public sealed class ValueCaptureWitness<T> : IObserver<T>
{
    /// <summary>Gets the captured value, or <see langword="default"/> if none.</summary>
    public T? Value { get; private set; }

    /// <summary>Subscribes to <paramref name="source"/> and returns the last value emitted during the synchronous subscribe call.</summary>
    /// <param name="source">The observable to subscribe to.</param>
    /// <returns>The last emitted value, or <see langword="default"/> if no value was emitted.</returns>
    public static T? SubscribeGetValue(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ValueCaptureWitness<T> sink = new();
        using var subscription = source.Subscribe(sink);
        return sink.Value;
    }

    /// <inheritdoc/>
    public void OnNext(T value) => Value = value;

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
    }
}
