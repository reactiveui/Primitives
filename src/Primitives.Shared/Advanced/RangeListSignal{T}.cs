// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Eager range-backed signal for <c>CollectList</c> (no per-value subscription).</summary>
/// <typeparam name="T">The result element type, which is always <see cref="int"/>.</typeparam>
[System.Diagnostics.DebuggerDisplay("RangeListSignal: Range = {_range}")]
public sealed class RangeListSignal<T> : IObservable<IList<T>>
{
    /// <summary>The source range.</summary>
    private readonly RangeSignal _range;

    /// <summary>Initializes a new instance of the <see cref="RangeListSignal{T}"/> class.</summary>
    /// <param name="range">The source range.</param>
    public RangeListSignal(RangeSignal range) => _range = range;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IList<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        List<int> values = [with(capacity: _range.Count)];
        for (var i = 0; i < _range.Count; i++)
        {
            values.Add(_range.Start + i);
        }

        // This helper requires T to be int.
        observer.OnNext((IList<T>)(object)values);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }
}
