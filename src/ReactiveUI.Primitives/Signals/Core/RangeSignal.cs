// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the RangeSignal class.
/// </summary>
internal sealed class RangeSignal : IRequireCurrentThread<int>, IInlineSignal<int>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RangeSignal"/> class.
    /// </summary>
    /// <param name="start">The start value.</param>
    /// <param name="count">The count value.</param>
    public RangeSignal(int start, int count)
    {
        Start = start;
        Count = count;
    }

    /// <summary>
    /// Gets the first value emitted by the range.
    /// </summary>
    internal int Start { get; }

    /// <summary>
    /// Gets the number of values emitted by the range.
    /// </summary>
    internal int Count { get; }

    /// <summary>
    /// Executes the IsRequiredSubscribeOnCurrentThread operation.
    /// </summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>
    /// Executes the Subscribe operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<int> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        for (var i = 0; i < Count; i++)
        {
            observer.OnNext(Start + i);
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>
    /// Executes the Subscribe operation.
    /// </summary>
    /// <param name="onNext">The onNext value.</param>
    /// <param name="onError">The onError value.</param>
    /// <param name="onCompleted">The onCompleted value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(Action<int> onNext, Action<Exception> onError, Action onCompleted)
    {
        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        for (var i = 0; i < Count; i++)
        {
            onNext(Start + i);
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }
}
