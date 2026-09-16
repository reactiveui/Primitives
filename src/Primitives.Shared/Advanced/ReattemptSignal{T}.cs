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

        _source = source;
        _retryCount = retryCount;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new ReattemptCoordinator<T>(_source, _retryCount, observer).Run();
    }
}
