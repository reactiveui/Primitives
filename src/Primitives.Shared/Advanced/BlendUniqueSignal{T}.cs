// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>A fused merge + distinct-until-changed observable over a fixed set of sources.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("BlendUniqueSignal: Sources = {_sources}")]
public sealed class BlendUniqueSignal<T> : IObservable<T>
{
    /// <summary>The sources to merge.</summary>
    private readonly IObservable<T>[] _sources;

    /// <summary>The equality comparer used to suppress duplicates.</summary>
    private readonly IEqualityComparer<T> _comparer;

    /// <summary>Initializes a new instance of the <see cref="BlendUniqueSignal{T}"/> class.</summary>
    /// <param name="sources">The sources to merge.</param>
    /// <param name="comparer">The equality comparer used to suppress duplicates.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/>, one of its elements, or <paramref name="comparer"/> is <see langword="null"/>.</exception>
    public BlendUniqueSignal(IObservable<T>[] sources, IEqualityComparer<T> comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);
        ArgumentExceptionHelper.ThrowIfNull(comparer);

        for (var i = 0; i < sources.Length; i++)
        {
            ArgumentExceptionHelper.ThrowIfNull(sources[i]);
        }

        _sources = sources;
        _comparer = comparer;
    }

    /// <summary>Concurrently merges <paramref name="sources"/> and suppresses adjacent duplicate values using the default equality comparer.</summary>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable of the distinct merged values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Create(params IObservable<T>[] sources) => Create(sources, null);

    /// <summary>Concurrently merges <paramref name="sources"/> and suppresses adjacent duplicate values using <paramref name="comparer"/>.</summary>
    /// <param name="sources">The sources to merge.</param>
    /// <param name="comparer">The equality comparer used to suppress duplicates, or <see langword="null"/> for the default.</param>
    /// <returns>An observable of the distinct merged values.</returns>
    public static IObservable<T> Create(IObservable<T>[] sources, IEqualityComparer<T>? comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        for (var i = 0; i < sources.Length; i++)
        {
            ArgumentExceptionHelper.ThrowIfNull(sources[i]);
        }

        return sources.Length == 0
            ? ImmutableEmptySignal<T>.Instance
            : new BlendUniqueSignal<T>(sources, comparer ?? EqualityComparer<T>.Default);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        BlendUniqueSink<T> sink = new(observer, _comparer);
        sink.Run(_sources);
        return sink;
    }
}
