// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that buffers values and emits them as a list on completion.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class CollectListObserver<T> : SingleSourceObserver<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<IList<T>> _observer;

    /// <summary>The accumulated values.</summary>
    private readonly List<T> _values = [];

    /// <summary>Initializes a new instance of the <see cref="CollectListObserver{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public CollectListObserver(IObserver<IList<T>> observer) => _observer = observer;

    /// <inheritdoc/>
    public override void OnNext(T value) => _values.Add(value);

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
            _observer.OnNext(_values);
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
