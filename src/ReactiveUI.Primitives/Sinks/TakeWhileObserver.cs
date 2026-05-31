// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Sink that forwards values while the predicate holds, then completes and unsubscribes.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class TakeWhileObserver<T> : SingleSourceObserver<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The predicate that determines whether to keep taking values.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>A value indicating whether completion has been emitted.</summary>
    private bool _completed;

    /// <summary>
    /// Initializes a new instance of the <see cref="TakeWhileObserver{T}"/> class.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="predicate">The predicate that determines whether to keep taking values.</param>
    public TakeWhileObserver(IObserver<T> observer, Func<T, bool> predicate)
    {
        _observer = observer;
        _predicate = predicate;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        if (_completed)
        {
            return;
        }

        if (!_predicate(value))
        {
            Complete();
            return;
        }

        try
        {
            _observer.OnNext(value);
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
        if (_completed)
        {
            return;
        }

        _completed = true;
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
    public override void OnCompleted() => Complete();

    /// <summary>
    /// Completes the downstream observer once and releases the upstream subscription.
    /// </summary>
    private void Complete()
    {
        if (_completed)
        {
            return;
        }

        _completed = true;
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
