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

    /// <summary>Guards synchronous completion while a subscription is still being assigned.</summary>
    private readonly Lock _gate = new();

    /// <summary>The remaining number of finite subscriptions.</summary>
    private int _remaining;

    /// <summary>Tracks whether a source subscription is currently being created.</summary>
    private bool _subscribing;

    /// <summary>Tracks synchronous completion before the subscription disposable is returned.</summary>
    private bool _completedWhileSubscribing;

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
        if (!IsCurrentGeneration(generation))
        {
            return;
        }

        _observer.OnNext(value);
    }

    /// <summary>Completes the downstream observer once.</summary>
    private void Complete()
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

    /// <summary>Schedules the next source subscription on the current-thread trampoline.</summary>
    private void ScheduleNext()
    {
        if (IsDisposed())
        {
            return;
        }

        lock (_gate)
        {
            _completedWhileSubscribing |= _subscribing;
        }

        var scheduled = Sequencer.CurrentThread.Schedule(SubscribeNext);
        if (ReferenceEquals(scheduled, EmptyDisposable.Instance) || IsDisposed())
        {
            return;
        }

        _active.Create(scheduled);
    }

    /// <summary>Subscribes to the source for the next repetition.</summary>
    private void SubscribeNext()
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
        IDisposable? subscription = null;
        var completedWhileSubscribing = false;
        lock (_gate)
        {
            _subscribing = true;
            _completedWhileSubscribing = false;
        }

        try
        {
            subscription = _source.Subscribe(observer);
        }
        catch (Exception error)
        {
            observer.OnError(error);
        }
        finally
        {
            lock (_gate)
            {
                completedWhileSubscribing = _completedWhileSubscribing;
                _completedWhileSubscribing = false;
                _subscribing = false;
            }
        }

        if (subscription is null)
        {
            return;
        }

        if (completedWhileSubscribing || IsDisposed())
        {
            subscription.Dispose();
            return;
        }

        _active.Create(subscription);
    }

    /// <summary>Gets a value indicating whether the coordinator is disposed.</summary>
    /// <returns><see langword="true"/> when the coordinator is disposed; otherwise, <see langword="false"/>.</returns>
    private bool IsDisposed() => Volatile.Read(ref _disposed) != 0;

    /// <summary>Gets a value indicating whether a notification belongs to the active generation.</summary>
    /// <param name="generation">The source subscription generation.</param>
    /// <returns><see langword="true"/> when the generation can still forward; otherwise, <see langword="false"/>.</returns>
    private bool IsCurrentGeneration(int generation) =>
        !IsDisposed() && Volatile.Read(ref _activeGeneration) == generation;
}
