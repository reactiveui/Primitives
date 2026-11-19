// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>Reattempt</c> (retry on error).</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ReattemptSignal: RetryCount = {_retryCount}, Source = {_source}")]
public sealed class ReattemptSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The maximum number of retries after the initial subscription.</summary>
    private readonly int _retryCount;

    /// <summary>Initializes a new instance of the <see cref="ReattemptSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="retryCount">The maximum number of retries after the initial subscription.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public ReattemptSignal(IObservable<T> source, int retryCount)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(retryCount);

        _source = source;
        _retryCount = retryCount;
    }

    /// <summary>Creates a sequence that retries <paramref name="source"/> up to <paramref name="retryCount"/> extra times after an error.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="retryCount">The maximum number of extra attempts after the initial subscription.</param>
    /// <returns>The retrying observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="retryCount"/> is negative.</exception>
    public static IObservable<T> Create(IObservable<T> source, int retryCount)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(retryCount);

        return retryCount == 0
               || source is ImmutableEmptySignal<T>
                   or ImmutableNeverSignal<T>
                   or RangeSignal
                   or ImmediateReturnSignal<T>
            ? source
            : new ReattemptSignal<T>(source, retryCount);
    }

    /// <summary>Creates a sequence that runs <paramref name="source"/> up to <paramref name="retryCount"/> total times.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="retryCount">The total number of runs; zero completes immediately without subscribing.</param>
    /// <returns>The retrying observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="retryCount"/> is negative.</exception>
    public static IObservable<T> CreateTotalRuns(IObservable<T> source, int retryCount)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(retryCount);

        if (retryCount == 0)
        {
            return ImmutableEmptySignal<T>.Instance;
        }

        return retryCount == 1
               || source is ImmutableEmptySignal<T>
                   or ImmutableNeverSignal<T>
                   or RangeSignal
                   or ImmediateReturnSignal<T>
            ? source
            : new ReattemptSignal<T>(source, retryCount - 1);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _retryCount == 0
            || _source is RangeSignal or ImmediateReturnSignal<T> or ImmutableEmptySignal<T> or RepeatSignal<T>
            ? _source.Subscribe(observer)
            : new ReattemptCoordinator<T>(_source, _retryCount, observer).Run();
    }
}
