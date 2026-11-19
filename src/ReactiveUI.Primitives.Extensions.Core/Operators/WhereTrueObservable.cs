// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Forwards only the <c>true</c> values of a boolean source, passing errors and completion through unchanged.</summary>
/// <param name="source">The boolean source observable.</param>
public sealed class WhereTrueObservable(IObservable<bool> source) : IObservable<bool>
{
    /// <summary>Creates a <see langword="true"/>-only filter sequence over <paramref name="source"/>, eliding redundant wrappers and empty or never singletons.</summary>
    /// <param name="source">The boolean source observable.</param>
    /// <returns>A sequence carrying only <see langword="true"/> values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<bool> Create(IObservable<bool> source) =>
        source is ImmutableEmptySignal<bool> or ImmutableNeverSignal<bool> or WhereTrueObservable
            ? source
            : new WhereTrueObservable(source);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (source is WhereTrueObservable inner && inner.GetSource() is { } innerSource)
        {
            return innerSource.Subscribe(new WhereTrueWitness(observer));
        }

        if (source is FromEnumerableSignal<bool> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            for (var i = 0; i < values.Count; i++)
            {
                if (values[i])
                {
                    observer.OnNext(true);
                }
            }

            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        return source.Subscribe(new WhereTrueWitness(observer));
    }

    /// <summary>Returns the underlying boolean source sequence.</summary>
    /// <returns>The underlying boolean source sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal IObservable<bool> GetSource() => source;

    /// <summary>Forwarding observer that drops <c>false</c> values and passes <c>true</c> values, terminal error and completion through unchanged.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class WhereTrueWitness(IObserver<bool> downstream) : IObserver<bool>
    {
        /// <inheritdoc/>
        public void OnNext(bool value)
        {
            if (!value)
            {
                return;
            }

            downstream.OnNext(true);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
