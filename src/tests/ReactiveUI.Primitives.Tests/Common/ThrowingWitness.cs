// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>An observer that can be configured to throw on specific callbacks.</summary>
/// <typeparam name="T">The type of the observed values.</typeparam>
/// <param name="throwOnNext">Configures throwing from the value callback.</param>
/// <param name="throwOnError">Configures throwing from the error callback.</param>
/// <param name="throwOnCompleted">Configures throwing from the completion callback.</param>
internal sealed class ThrowingWitness<T>(
    bool throwOnNext = false,
    bool throwOnError = false,
    bool throwOnCompleted = false) : IObserver<T>
{
    /// <summary>A value indicating whether to throw on <see cref="OnNext"/>.</summary>
    private readonly bool _throwOnNext = throwOnNext;

    /// <summary>A value indicating whether to throw on <see cref="OnError"/>.</summary>
    private readonly bool _throwOnError = throwOnError;

    /// <summary>A value indicating whether to throw on <see cref="OnCompleted"/>.</summary>
    private readonly bool _throwOnCompleted = throwOnCompleted;

    /// <summary>Gets a value indicating whether an error callback has been observed.</summary>
    internal bool SeenError { get; private set; }

    /// <summary>Handles completion, throwing when configured to do so.</summary>
    /// <exception cref="InvalidOperationException">The witness is configured to throw on completion.</exception>
    public void OnCompleted()
    {
        if (!_throwOnCompleted)
        {
            return;
        }

        throw new InvalidOperationException("observer-completed");
    }

    /// <summary>Handles an error, throwing when configured to do so.</summary>
    /// <param name="error">The error to handle.</param>
    /// <exception cref="InvalidOperationException">The witness is configured to throw on errors.</exception>
    public void OnError(Exception error)
    {
        SeenError = true;
        if (!_throwOnError)
        {
            return;
        }

        throw new InvalidOperationException("observer-error");
    }

    /// <summary>Handles a value, throwing when configured to do so.</summary>
    /// <param name="value">The value to handle.</param>
    /// <exception cref="InvalidOperationException">The witness is configured to throw on values.</exception>
    public void OnNext(T value)
    {
        if (!_throwOnNext)
        {
            return;
        }

        throw new InvalidOperationException("observer-next");
    }
}
