// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Observer for predicate count.
/// </summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class CountPredicateObserver<T> : SingleSourceObserver<T>
{
    /// <summary>
    /// The downstream observer.
    /// </summary>
    private readonly IObserver<int> _observer;

    /// <summary>
    /// The predicate.
    /// </summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>
    /// The running count.
    /// </summary>
    private int _count;

    /// <summary>
    /// A value indicating whether the observer has terminated.
    /// </summary>
    private bool _done;

    /// <summary>
    /// Initializes a new instance of the <see cref="CountPredicateObserver{T}"/> class.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="predicate">The predicate.</param>
    public CountPredicateObserver(IObserver<int> observer, Func<T, bool> predicate)
    {
        _observer = observer;
        _predicate = predicate;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        if (_done || !_predicate(value))
        {
            return;
        }

        _count = checked(_count + 1);
    }

    /// <inheritdoc/>
    public override void OnError(Exception error)
    {
        if (_done)
        {
            return;
        }

        _done = true;
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
        if (_done)
        {
            return;
        }

        _done = true;
        try
        {
            _observer.OnNext(_count);
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
