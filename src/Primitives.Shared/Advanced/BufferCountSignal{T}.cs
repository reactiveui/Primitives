// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated cold signal for <c>Buffer</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("BufferCountSignal: Source = {_source}")]
public sealed class BufferCountSignal<T> : IObservable<IList<T>>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The window size.</summary>
    private readonly int _count;

    /// <summary>The number of elements skipped between windows.</summary>
    private readonly int _skip;

    /// <summary>Initializes a new instance of the <see cref="BufferCountSignal{T}"/> class for consecutive non-overlapping windows.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The window size.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is zero or negative.</exception>
    public BufferCountSignal(IObservable<T> source, int count)
        : this(source, count, 0)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="BufferCountSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The window size.</param>
    /// <param name="skip">The number of elements skipped between windows; zero denotes consecutive non-overlapping windows.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is zero or negative, or <paramref name="skip"/> is negative.</exception>
    public BufferCountSignal(IObservable<T> source, int count, int skip)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(count);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(skip);

        _source = source;
        _count = count;
        _skip = skip;
    }

    /// <summary>Creates a <see cref="BufferCountSignal{T}"/> whose windows open every <paramref name="skip"/> elements.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The window size.</param>
    /// <param name="skip">The positive number of elements between window starts.</param>
    /// <returns>The buffered signal.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> or <paramref name="skip"/> is zero or negative.</exception>
    public static BufferCountSignal<T> Create(IObservable<T> source, int count, int skip)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(count);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(skip);

        return new(source, count, skip);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IList<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if ((_skip == 0 || _skip == _count) && typeof(T) == typeof(int) && _source is RangeSignal range)
        {
            EmitNonOverlappingRange(range, _count, observer);
            return EmptyDisposable.Instance;
        }

        BufferWitness<T> sink = new(observer, _count, _skip);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Emits consecutive non-overlapping integer range buffers directly without a witness.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="windowSize">The buffer size.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitNonOverlappingRange(RangeSignal range, int windowSize, IObserver<IList<T>> observer)
    {
        var intObserver = Unsafe.As<IObserver<IList<int>>>(observer);
        var start = range.Start;
        var total = range.Count;
        for (var offset = 0; offset < total; offset += windowSize)
        {
            var length = Math.Min(windowSize, total - offset);
            var window = new int[length];
            for (var i = 0; i < length; i++)
            {
                window[i] = start + offset + i;
            }

            intObserver.OnNext(window);
        }

        observer.OnCompleted();
    }
}
