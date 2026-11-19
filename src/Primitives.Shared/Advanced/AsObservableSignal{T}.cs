// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that forwards to its source while hiding the source's own type from the caller.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>A caller handed one of these cannot cast it back to a subject and push values into it.</remarks>
[System.Diagnostics.DebuggerDisplay("AsObservableSignal: Source = {_source}")]
public sealed class AsObservableSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Initializes a new instance of the <see cref="AsObservableSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public AsObservableSignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
    }

    /// <summary>Validates <paramref name="source"/> and returns it unchanged.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>The supplied <paramref name="source"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Identity(IObservable<T> source) =>
        source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>Wraps <paramref name="source"/> in an <see cref="AsObservableSignal{T}"/> unless it already is one.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>An <see cref="AsObservableSignal{T}"/> hiding the concrete type of <paramref name="source"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static AsObservableSignal<T> Wrap(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source as AsObservableSignal<T> ?? new AsObservableSignal<T>(source);
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _source.Subscribe(observer);
    }
}
