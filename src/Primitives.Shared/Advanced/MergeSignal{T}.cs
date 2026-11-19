// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Merges concurrent sources into one sequence, optionally bounding the active subscription count.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("MergeSignal: Sources = {Sources}, MaxConcurrent = {MaxConcurrent}")]
public sealed class MergeSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="MergeSignal{T}"/> class.</summary>
    /// <param name="sources">The enumerable sources to merge.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public MergeSignal(IEnumerable<IObservable<T>> sources) =>
        Sources = sources ?? throw new ArgumentNullException(nameof(sources));

    /// <summary>Initializes a new instance of the <see cref="MergeSignal{T}"/> class.</summary>
    /// <param name="sources">The enumerable sources to merge.</param>
    /// <param name="maxConcurrent">The maximum number of active subscriptions.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public MergeSignal(IEnumerable<IObservable<T>> sources, int maxConcurrent)
    {
        Sources = sources ?? throw new ArgumentNullException(nameof(sources));
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maxConcurrent);
        MaxConcurrent = maxConcurrent;
    }

    /// <summary>Initializes a new instance of the <see cref="MergeSignal{T}"/> class.</summary>
    /// <param name="first">The first source.</param>
    /// <param name="second">The second source.</param>
    /// <exception cref="ArgumentNullException"><paramref name="first"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
    public MergeSignal(IObservable<T> first, IObservable<T> second)
    {
        First = first ?? throw new ArgumentNullException(nameof(first));
        Second = second ?? throw new ArgumentNullException(nameof(second));
    }

    /// <summary>Gets the enumerable sources.</summary>
    private IEnumerable<IObservable<T>>? Sources { get; }

    /// <summary>Gets the first pair source.</summary>
    private IObservable<T>? First { get; }

    /// <summary>Gets the second pair source.</summary>
    private IObservable<T>? Second { get; }

    /// <summary>Gets the maximum number of active subscriptions, when bounded.</summary>
    private int? MaxConcurrent { get; }

    /// <summary>Merges two sequences with empty identity reductions.</summary>
    /// <param name="first">The first sequence.</param>
    /// <param name="second">The second sequence.</param>
    /// <returns>The merged observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="first"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> first, IObservable<T> second)
    {
        ArgumentExceptionHelper.ThrowIfNull(first);
        ArgumentExceptionHelper.ThrowIfNull(second);

        if (first is ImmutableEmptySignal<T>)
        {
            return second;
        }

        return second is ImmutableEmptySignal<T>
            ? first
            : new MergeSignal<T>(first, second);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (Sources is { } sources)
        {
            return MaxConcurrent is { } maxConcurrent && maxConcurrent != int.MaxValue
                ? new MaxConcurrentBlendCoordinator<T>(observer).Run(sources, maxConcurrent)
                : new MergeCoordinator<T>(observer).Run(sources);
        }

        return new MergeCoordinator<T>(observer).Run(First!, Second!);
    }
}
