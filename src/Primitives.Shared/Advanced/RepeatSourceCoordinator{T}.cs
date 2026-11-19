// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates sequential subscriptions for <see cref="RepeatSourceSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("RepeatSourceCoordinator: Remaining = {_remaining}, Disposed = {_disposed}")]
public sealed class RepeatSourceCoordinator<T> : IDisposable
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The configured repeat count.</summary>
    private readonly int? _repeatCount;

    /// <summary>The active source subscription or queued resubscription.</summary>
    private readonly SingleReplaceableDisposable _active = new();

    /// <summary>
    /// The cancel handle of the downstream subscription, which can be disposed before this coordinator is: a bounding operator
    /// such as <c>Take</c> disposes it from inside a value, while the loop that subscribes again is still running.
    /// </summary>
    private readonly IsDisposed? _downstream;

    /// <summary>The remaining number of finite subscriptions. Only the thread that is subscribing reads or writes it.</summary>
    private int _remaining;

    /// <summary>
    /// The subscriptions asked for and not yet made. The thread that raises it from zero makes them all, so a source that
    /// completes on another thread while this one is still subscribing to it is followed by the next subscription and never
    /// by a second thread subscribing at the same time.
    /// </summary>
    private int _pending;

    /// <summary>Tracks the current subscription generation.</summary>
    private int _generation;

    /// <summary>The generation currently allowed to forward notifications.</summary>
    private int _activeGeneration;

    /// <summary>Tracks disposal and terminal notification state.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="RepeatSourceCoordinator{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="repeatCount">The number of repetitions, or <see langword="null"/> for indefinite repetition.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="repeatCount"/> is less than zero.</exception>
    public RepeatSourceCoordinator(IObservable<T> source, int? repeatCount, IObserver<T> observer)
        : this(source, repeatCount, observer, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="RepeatSourceCoordinator{T}"/> class that stops when the downstream subscription is disposed.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="repeatCount">The number of repetitions, or <see langword="null"/> for indefinite repetition.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="downstream">The cancel handle of the downstream subscription, or <see langword="null"/> when there is none to watch.</param>
    internal RepeatSourceCoordinator(IObservable<T> source, int? repeatCount, IObserver<T> observer, IsDisposed? downstream)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (repeatCount.HasValue)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(repeatCount.GetValueOrDefault());
        }

        ArgumentExceptionHelper.ThrowIfNull(observer);

        _source = source;
        _observer = observer;
        _repeatCount = repeatCount;
        _remaining = repeatCount.GetValueOrDefault();
        _downstream = downstream;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _active.Dispose();
    }

    /// <summary>Starts the repeated subscription loop.</summary>
    /// <returns>The coordinator that owns the subscription cleanup.</returns>
    public RepeatSourceCoordinator<T> Run()
    {
        ScheduleNext();
        return this;
    }

    /// <summary>Handles completion for the active source subscription.</summary>
    /// <param name="generation">The source subscription generation.</param>
    internal void OnCompleted(int generation)
    {
        if (!IsCurrentGeneration(generation))
        {
            return;
        }

        ScheduleNext();
    }

    /// <summary>Handles failure for the active source subscription.</summary>
    /// <param name="generation">The source subscription generation.</param>
    /// <param name="error">The source error.</param>
    internal void OnError(int generation, Exception error)
    {
        if (!IsCurrentGeneration(generation) || Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            _observer.OnError(error);
        }
        finally
        {
            _active.Dispose();
        }
    }

    /// <summary>Forwards a value from the active source subscription.</summary>
    /// <param name="generation">The source subscription generation.</param>
    /// <param name="value">The source value.</param>
    internal void OnNext(int generation, T value)
    {
        if (IsDisposed() || !IsCurrentGeneration(generation))
        {
            return;
        }

        _observer.OnNext(value);
    }

    /// <summary>Completes the downstream observer once.</summary>
    internal void Complete()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            _observer.OnCompleted();
        }
        finally
        {
            _active.Dispose();
        }
    }

    /// <summary>
    /// Asks for the next source subscription. The first caller makes every subscription asked for while it works: a source
    /// that completes inside its own <c>Subscribe</c>, or on another thread before <c>Subscribe</c> returns, only raises the
    /// count, and the subscribing thread goes round again. No stack grows and no two threads subscribe at once.
    /// </summary>
    internal void ScheduleNext()
    {
        if (IsDisposed() || Interlocked.Increment(ref _pending) != 1)
        {
            return;
        }

        // The loop goes on the current-thread trampoline, which runs it once the subscription handle exists, so a bounding
        // operator such as Take can dispose the subscription from inside a value and stop the loop.
        var scheduled = Sequencer.CurrentThread.Schedule(SubscribeWhilePending);
        if (ReferenceEquals(scheduled, EmptyDisposable.Instance) || IsDisposed())
        {
            return;
        }

        _active.Create(scheduled);
    }

    /// <summary>Subscribes for each subscription asked for, including those asked for while this one is subscribing.</summary>
    internal void SubscribeWhilePending()
    {
        do
        {
            SubscribeNext();
        }
        while (!IsDisposed() && Interlocked.Decrement(ref _pending) != 0);
    }

    /// <summary>Subscribes to the source for the next repetition.</summary>
    internal void SubscribeNext()
    {
        if (IsDisposed())
        {
            return;
        }

        if (_repeatCount is not null && _remaining == 0)
        {
            Complete();
            return;
        }

        if (_repeatCount is not null)
        {
            _remaining--;
        }

        var generation = Interlocked.Increment(ref _generation);
        Volatile.Write(ref _activeGeneration, generation);
        RepeatSourceWitness<T> observer = new(this, generation);
        IDisposable subscription;
        try
        {
            subscription = _source.Subscribe(observer);
        }
        catch (Exception error)
        {
            observer.OnError(error);
            return;
        }

        // A run that has already completed asked for the next one, which this thread makes next, so its subscription is
        // done with. Keeping it would only hold it until the next run replaces it.
        if (IsDisposed() || Volatile.Read(ref _pending) > 1)
        {
            subscription.Dispose();
            return;
        }

        _active.Create(subscription);
    }

    /// <summary>Gets a value indicating whether the coordinator is disposed.</summary>
    /// <returns><see langword="true"/> when the coordinator is disposed; otherwise, <see langword="false"/>.</returns>
    private bool IsDisposed() => Volatile.Read(ref _disposed) != 0 || _downstream is { IsDisposed: true };

    /// <summary>Gets a value indicating whether a notification belongs to the active generation.</summary>
    /// <param name="generation">The source subscription generation.</param>
    /// <returns><see langword="true"/> when the notification belongs to the active generation; otherwise, <see langword="false"/>.</returns>
    private bool IsCurrentGeneration(int generation) =>
        Volatile.Read(ref _activeGeneration) == generation;
}
