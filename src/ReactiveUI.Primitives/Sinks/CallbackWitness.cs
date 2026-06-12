// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives;

/// <summary>Observer that forwards notifications to delegates.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
public sealed class CallbackWitness<T> : IObserver<T>
{
    /// <summary>Next notification callback.</summary>
    private readonly Action<T> _onNext;

    /// <summary>Error notification callback.</summary>
    private readonly Action<Exception>? _onError;

    /// <summary>Completion notification callback.</summary>
    private readonly Action<Result>? _onCompleted;

    /// <summary>Initializes a new instance of the <see cref="CallbackWitness{T}"/> class.</summary>
    /// <param name="onNext">Next notification callback.</param>
    /// <param name="onError">Error notification callback.</param>
    /// <param name="onCompleted">Completion notification callback.</param>
    public CallbackWitness(Action<T> onNext, Action<Exception>? onError, Action<Result>? onCompleted)
    {
        _onNext = onNext ?? throw new ArgumentNullException(nameof(onNext));
        _onError = onError;
        _onCompleted = onCompleted;
    }

    /// <inheritdoc/>
    public void OnCompleted() => _onCompleted?.Invoke(Result.Success);

    /// <inheritdoc/>
    public void OnError(Exception error) => (_onError ?? Rethrow)(error);

    /// <inheritdoc/>
    public void OnNext(T value) => _onNext(value);

    /// <summary>Rethrows the supplied exception without losing its stack information.</summary>
    /// <param name="error">The exception to rethrow.</param>
    /// <remarks>Excluded from coverage: the unreachable sequence point after <see cref="ExceptionDispatchInfo"/> rethrow cannot be credited by cobertura.</remarks>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void Rethrow(Exception error) => ExceptionDispatchInfo.Capture(error).Throw();
}
