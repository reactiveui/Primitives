// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Repeats a source observable by resubscribing after each successful completion.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Source = {_source}, RepeatCount = {_repeatCount}")]
public sealed class RepeatSourceSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The number of repetitions, or <see langword="null"/> for indefinite repetition.</summary>
    private readonly int? _repeatCount;

    /// <summary>Initializes a new instance of the <see cref="RepeatSourceSignal{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="repeatCount">The number of repetitions, or <see langword="null"/> for indefinite repetition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="repeatCount"/> is less than zero.</exception>
    public RepeatSourceSignal(IObservable<T> source, int? repeatCount)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (repeatCount.HasValue)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(repeatCount.GetValueOrDefault());
        }

        _source = source;
        _repeatCount = repeatCount;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return SignalSubscription.Subscribe(observer, true, SubscribeCore);
    }

    /// <summary>Starts the repeat coordinator once the subscription lifetime has been created.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The cancellation handle owned by the subscription helper.</param>
    /// <returns>The repeat coordinator.</returns>
    private RepeatSourceCoordinator<T> SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        RepeatSourceCoordinator<T> coordinator = new(_source, _repeatCount, new GuardedWitness<T>(observer, cancel));
        return coordinator.Run();
    }
}
