// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that hands out a window signal and starts the next window each time a boundary signal emits.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TBoundary">The value type of the boundary signal.</typeparam>
/// <remarks>
/// The window ends and the outer sequence completes when the boundary signal completes, and both end when the source ends.
/// Notifications are delivered outside the sink's lock.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SliceBoundaryWitness: Done = {_done}")]
public sealed class SliceBoundaryWitness<T, TBoundary> : IObserver<T>, IDisposable
{
    /// <summary>Serializes access to the current window and the terminal latch.</summary>
    private readonly Lock _gate = new();

    /// <summary>Delivers windows and their values in the order they were posted.</summary>
    private readonly SliceRouter<IObservable<T>, T> _router;

    /// <summary>The source subscription and the boundary subscription.</summary>
    private readonly MultipleDisposable _upstream = [];

    /// <summary>The window currently receiving values.</summary>
    private SliceWindow<T> _current;

    /// <summary>The observer of the boundary signal, created on first use.</summary>
    private IObserver<TBoundary>? _boundaries;

    /// <summary>The terminal latch; non-zero once the sink has terminated and must ignore further notifications.</summary>
    private int _done;

    /// <summary>Initializes a new instance of the <see cref="SliceBoundaryWitness{T, TBoundary}"/> class.</summary>
    /// <param name="observer">The downstream observer of the windows.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public SliceBoundaryWitness(IObserver<IObservable<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        Subscription = new();
        Subscription.Attach(_upstream);
        _router = new(observer, Subscription);
        _current = new(Subscription);
    }

    /// <summary>Gets the subscription handed to the outer subscriber; the source stays subscribed while any window is.</summary>
    public SharedSubscription Subscription { get; }

    /// <summary>Gets the observer to subscribe to the boundary signal.</summary>
    public IObserver<TBoundary> Boundaries => _boundaries ??= new BoundaryWitness(this);

    /// <summary>Opens the first window before the source is subscribed.</summary>
    public void Start()
    {
        lock (_gate)
        {
            _router.Open(_current);
        }

        Deliver();
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            _router.Publish(_current, value);
        }

        Deliver();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => Terminate(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => Terminate(null);

    /// <summary>Assigns an upstream subscription, disposing the incoming one when this sink has been disposed.</summary>
    /// <param name="subscription">The source or boundary subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => _upstream.Add(subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _done, 1);
        Subscription.Release();
        _upstream.Dispose();
    }

    /// <summary>Ends the current window and opens the next.</summary>
    private void Advance()
    {
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            _router.Complete(_current);
            _current = new(Subscription);
            _router.Open(_current);
        }

        Deliver();
    }

    /// <summary>Delivers the posted notifications, tearing the sink down when a downstream observer throws.</summary>
    private void Deliver()
    {
        try
        {
            _router.Flush();
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>Ends the current window and the outer sequence, then tears the sink down.</summary>
    /// <param name="error">The error, or <see langword="null"/> to complete.</param>
    private void Terminate(Exception? error)
    {
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            Volatile.Write(ref _done, 1);
            _router.Finish([_current], error);
        }

        try
        {
            _router.Flush();
        }
        finally
        {
            Dispose();
        }
    }

    /// <summary>Observes the boundary signal.</summary>
    /// <param name="parent">The owning sink.</param>
    private sealed class BoundaryWitness(SliceBoundaryWitness<T, TBoundary> parent) : IObserver<TBoundary>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(TBoundary value) => parent.Advance();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.Terminate(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => parent.Terminate(null);
    }
}
