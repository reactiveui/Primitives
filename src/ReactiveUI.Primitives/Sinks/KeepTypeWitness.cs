// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that forwards only values assignable to <typeparamref name="TResult"/>.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
public sealed class KeepTypeWitness<TResult> : SingleSourceWitness<object?>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer;

    /// <summary>Initializes a new instance of the <see cref="KeepTypeWitness{TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public KeepTypeWitness(IObserver<TResult> observer) => _observer = observer;

    /// <inheritdoc/>
    public override void OnNext(object? value)
    {
        if (value is not TResult result)
        {
            return;
        }

        try
        {
            _observer.OnNext(result);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public override void OnError(Exception error)
    {
        try
        {
            _observer.OnError(error);
        }
        finally
        {
            Dispose();
        }
    }

    /// <inheritdoc/>
    public override void OnCompleted()
    {
        try
        {
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
