// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Shared range/latest subscription logic.</summary>
internal static class RangeLatestSignalHelper
{
    /// <summary>Subscribes an observer to range latest-value output.</summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="left">The left range.</param>
    /// <param name="right">The right range.</param>
    /// <param name="selector">The result projection.</param>
    /// <returns>The completed subscription disposable.</returns>
    internal static IDisposable Subscribe<TResult>(
        IObserver<TResult> observer,
        RangeSignal left,
        RangeSignal right,
        Func<int, int, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var leftValue = left.Start + left.Count - 1;
        for (var i = 0; i < right.Count; i++)
        {
            observer.OnNext(selector(leftValue, right.Start + i));
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }
}
