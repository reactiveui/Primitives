// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Forwards only the non-null values of the source sequence, passing errors and completion through unchanged.</summary>
/// <typeparam name="T">The element type of the source observable.</typeparam>
/// <param name="source">The source observable whose null values are filtered out.</param>
/// <remarks>The element type is unchanged, so a nullable source stays nullable downstream.</remarks>
public sealed class WhereIsNotNullObservable<T>(IObservable<T> source) : IObservable<T>
{
    /// <summary>Creates a non-null filtering sequence over <paramref name="source"/>, eliding redundant wrappers and empty or never singletons.</summary>
    /// <param name="source">The source observable whose null values are filtered out.</param>
    /// <returns>A sequence carrying only non-null values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Create(IObservable<T> source) =>
        source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T> or WhereIsNotNullObservable<T>
            ? source
            : new WhereIsNotNullObservable<T>(source);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (source is WhereIsNotNullObservable<T> inner && inner.GetSource() is { } innerSource)
        {
            return innerSource.Subscribe(new WhereIsNotNullWitness(observer));
        }

        if (source is FromEnumerableSignal<T> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            for (var i = 0; i < values.Count; i++)
            {
                var item = values[i];
                if (item is not null)
                {
                    observer.OnNext(item);
                }
            }

            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        return source.Subscribe(new WhereIsNotNullWitness(observer));
    }

    /// <summary>Returns the underlying source sequence.</summary>
    /// <returns>The underlying source sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal IObservable<T> GetSource() => source;

    /// <summary>Forwarding observer that suppresses null <see cref="OnNext"/> values and passes <see cref="OnError"/> and <see cref="OnCompleted"/> through unchanged.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class WhereIsNotNullWitness(IObserver<T> downstream) : IObserver<T>
    {
        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (value is null)
            {
                return;
            }

            downstream.OnNext(value);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
