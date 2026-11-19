// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Emits each source boolean negated, passing errors and completion through unchanged.</summary>
/// <param name="source">The boolean source observable.</param>
public sealed class NotObservable(IObservable<bool> source) : IObservable<bool>
{
    /// <summary>Creates a boolean negation sequence over <paramref name="source"/>, short-circuiting empty and never singletons.</summary>
    /// <param name="source">The boolean source observable.</param>
    /// <returns>A negated boolean sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<bool> Create(IObservable<bool> source) =>
        source is ImmutableEmptySignal<bool> or ImmutableNeverSignal<bool>
            ? source
            : new NotObservable(source);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (source is NotObservable inner && inner.GetSource() is { } innerSource)
        {
            return innerSource.Subscribe(observer);
        }

        if (source is FromEnumerableSignal<bool> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            for (var i = 0; i < values.Count; i++)
            {
                observer.OnNext(!values[i]);
            }

            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        return source.Subscribe(new NotWitness(observer));
    }

    /// <summary>Returns the underlying boolean source sequence.</summary>
    /// <returns>The underlying boolean source sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal IObservable<bool> GetSource() => source;

    /// <summary>Forwarding observer that negates every boolean <see cref="OnNext"/>.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class NotWitness(IObserver<bool> downstream) : IObserver<bool>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(bool value) => downstream.OnNext(!value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
