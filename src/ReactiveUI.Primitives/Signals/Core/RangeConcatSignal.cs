// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>Concatenates synchronous integer ranges without outer observable/coordinator overhead.</summary>
internal sealed class RangeConcatSignal : IRequireCurrentThread<int>, IInlineSignal<int>
{
    /// <summary>Source ranges to emit in order.</summary>
    private readonly RangeSignal[] _ranges;

    /// <summary>Initializes a new instance of the <see cref="RangeConcatSignal"/> class.</summary>
    /// <param name="ranges">The source ranges.</param>
    public RangeConcatSignal(RangeSignal[] ranges) => _ranges = ranges;

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<int> observer)
    {
        if (observer is null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        for (var rangeIndex = 0; rangeIndex < _ranges.Length; rangeIndex++)
        {
            var range = _ranges[rangeIndex];
            for (var i = 0; i < range.Count; i++)
            {
                observer.OnNext(range.Start + i);
            }
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<int> onNext, Action<Exception> onError, Action onCompleted)
    {
        if (onNext is null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        if (onCompleted is null)
        {
            throw new ArgumentNullException(nameof(onCompleted));
        }

        for (var rangeIndex = 0; rangeIndex < _ranges.Length; rangeIndex++)
        {
            var range = _ranges[rangeIndex];
            for (var i = 0; i < range.Count; i++)
            {
                onNext(range.Start + i);
            }
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }
}
