// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits only the latest value after a quiet period.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("EmitIfQuietSignal: DueTime = {DueTime}, Source = {Source}")]
public sealed class EmitIfQuietSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="EmitIfQuietSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The quiet period before emitting the latest value.</param>
    /// <param name="sequencer">The sequencer that schedules delayed emissions.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    public EmitIfQuietSignal(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer)
    {
        Source = source ?? throw new ArgumentNullException(nameof(source));
        DueTime = dueTime;
        Sequencer = sequencer ?? throw new ArgumentNullException(nameof(sequencer));
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <summary>Gets the quiet period before emitting the latest value.</summary>
    private TimeSpan DueTime { get; }

    /// <summary>Gets the sequencer that schedules delayed emissions.</summary>
    private ISequencer Sequencer { get; }

    /// <summary>Creates a throttled signal using the default sequencer, returning <paramref name="source"/> unchanged when <paramref name="dueTime"/> is non-positive.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The quiet period before emitting the latest value.</param>
    /// <returns>A throttled signal.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, TimeSpan dueTime)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return dueTime <= TimeSpan.Zero
            ? source
            : new EmitIfQuietSignal<T>(source, dueTime, Concurrency.Sequencer.Default);
    }

    /// <summary>Creates a throttled signal on <paramref name="sequencer"/>, returning <paramref name="source"/> unchanged when <paramref name="dueTime"/> is non-positive.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The quiet period before emitting the latest value.</param>
    /// <param name="sequencer">The sequencer that schedules delayed emissions.</param>
    /// <returns>A throttled signal.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(sequencer);

        return dueTime <= TimeSpan.Zero
            ? source
            : new EmitIfQuietSignal<T>(source, dueTime, sequencer);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (DueTime <= TimeSpan.Zero)
        {
            return Source.Subscribe(observer);
        }

        EmitIfQuietWitness<T> emitObserver = new(observer, DueTime, Sequencer);
        emitObserver.SetSubscription(Source.Subscribe(emitObserver));
        return emitObserver;
    }
}
