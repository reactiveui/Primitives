// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Concrete signal for time-windowed buffer operators.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("BufferSignal: Window = {Window}, Source = {Source}")]
public sealed class BufferSignal<T> : IObservable<IList<T>>
{
    /// <summary>Initializes a new instance of the <see cref="BufferSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="timeSpan">The buffer window duration.</param>
    /// <param name="sequencer">The sequencer used to schedule buffer flushes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    public BufferSignal(IObservable<T> source, TimeSpan timeSpan, ISequencer sequencer)
    {
        Source = source ?? throw new ArgumentNullException(nameof(source));
        Window = timeSpan;
        Scheduler = sequencer ?? throw new ArgumentNullException(nameof(sequencer));
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <summary>Gets the buffer window duration.</summary>
    private TimeSpan Window { get; }

    /// <summary>Gets the sequencer used to schedule buffer flushes.</summary>
    private ISequencer Scheduler { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IList<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (Window <= TimeSpan.Zero)
        {
            BufferEachWitness<T> bufferObserver = new(observer);
            bufferObserver.SetSubscription(Source.Subscribe(bufferObserver));
            return bufferObserver;
        }

        return new Signal.CollectCoordinator<T>(observer, Window, Scheduler).Subscribe(Source);
    }
}
