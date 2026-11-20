// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer that captures the first error seen during synchronous subscribe.</summary>
/// <typeparam name="T">The element type of the source.</typeparam>
[System.Diagnostics.DebuggerDisplay("ErrorCaptureWitness: Error = {Error}")]
public sealed class ErrorCaptureWitness<T> : IObserver<T>
{
    /// <summary>Gets the captured error, or <see langword="null"/> if none.</summary>
    public Exception? Error { get; private set; }

    /// <summary>Subscribes to <paramref name="source"/> and returns the first error emitted during the synchronous subscribe call.</summary>
    /// <param name="source">The observable to subscribe to.</param>
    /// <returns>The captured error, or <see langword="null"/> if none.</returns>
    public static Exception? SubscribeGetError(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ErrorCaptureWitness<T> sink = new();
        using var subscription = source.Subscribe(sink);
        return sink.Error;
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => Error ??= error;

    /// <inheritdoc/>
    public void OnCompleted()
    {
    }
}
