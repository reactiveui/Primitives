// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Signal that pairs two integer ranges position by position, synchronously inside <c>Subscribe</c>.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("RangeZipSignal: Count = {_count}, LeftStart = {_leftStart}, RightStart = {_rightStart}")]
public sealed class RangeZipSignal<TResult> : IRequireCurrentThread<TResult>, IInlineSignal<TResult>
{
    /// <summary>The first value of the left range.</summary>
    private readonly int _leftStart;

    /// <summary>The first value of the right range.</summary>
    private readonly int _rightStart;

    /// <summary>The number of pairs emitted, the shorter of the two ranges.</summary>
    private readonly int _count;

    /// <summary>The projection applied to each pair.</summary>
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

    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Emits every projected pair to <paramref name="observer"/> and completes it before returning.</summary>
    /// <param name="observer">The observer to notify.</param>
    /// <returns>An empty disposable; both ranges are drained by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        for (var i = 0; i < _count; i++)
        {
            observer.OnNext(_selector(_leftStart + i, _rightStart + i));
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Emits every projected pair to <paramref name="onNext"/>, then invokes <paramref name="onCompleted"/>.</summary>
    /// <param name="onNext">Invoked for each projected pair.</param>
    /// <param name="onError">Never invoked.</param>
    /// <param name="onCompleted">Invoked after the last pair.</param>
    /// <returns>An empty disposable; both ranges are drained by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
    public IDisposable Subscribe(Action<TResult> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        for (var i = 0; i < _count; i++)
        {
            onNext(_selector(_leftStart + i, _rightStart + i));
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }
}
