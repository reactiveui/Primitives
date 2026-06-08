// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>Zips two synchronous integer ranges without coordinator queues.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
internal sealed class RangeZipSignal<TResult> : IRequireCurrentThread<TResult>, IInlineSignal<TResult>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly int _leftStart;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly int _rightStart;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly int _count;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Func<int, int, TResult> _selector;

    /// <summary>Initializes a new instance of the <see cref="RangeZipSignal{TResult}"/> class.</summary>
    /// <param name="left">The left range source.</param>
    /// <param name="right">The right range source.</param>
    /// <param name="selector">The projection function.</param>
    public RangeZipSignal(RangeSignal left, RangeSignal right, Func<int, int, TResult> selector)
    {
        _leftStart = left.Start;
        _rightStart = right.Start;
        _count = Math.Min(left.Count, right.Count);
        _selector = selector;
    }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        if (observer is null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        for (var i = 0; i < _count; i++)
        {
            observer.OnNext(_selector(_leftStart + i, _rightStart + i));
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="onNext">The onNext value.</param>
    /// <param name="onError">The onError value.</param>
    /// <param name="onCompleted">The onCompleted value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(Action<TResult> onNext, Action<Exception> onError, Action onCompleted)
    {
        if (onNext is null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        for (var i = 0; i < _count; i++)
        {
            onNext(_selector(_leftStart + i, _rightStart + i));
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }
}
