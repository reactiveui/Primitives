// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that hands out a window signal that ends when its own closing signal emits or completes.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TClosing">The value type of the closing signals.</typeparam>
/// <remarks>
/// A closing signal is requested for each window as it opens. A failure in the source, the selector or a closing signal
/// faults the current window and the outer sequence. Notifications are delivered outside the sink's lock.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SliceClosingWitness: Done = {_done}")]
public sealed class SliceClosingWitness<T, TClosing> : IObserver<T>, IDisposable
{
    /// <summary>Serializes access to the current window and the terminal latch.</summary>
    private readonly Lock _gate = new();

    /// <summary>The selector that supplies the signal that ends a window.</summary>
    private readonly Func<IObservable<TClosing>> _closingSelector;

    /// <summary>Delivers windows and their values in the order they were posted.</summary>
    private readonly SliceRouter<IObservable<T>, T> _router;

    /// <summary>The source subscription and the live closing subscription.</summary>
    private readonly MultipleDisposable _upstream = [];

    /// <summary>The window currently receiving values.</summary>
    private SliceWindow<T> _current;

    /// <summary>The terminal latch; non-zero once the sink has terminated and must ignore further notifications.</summary>
    private int _done;

    /// <summary>Initializes a new instance of the <see cref="SliceClosingWitness{T, TClosing}"/> class.</summary>
    /// <param name="observer">The downstream observer of the windows.</param>
    /// <param name="closingSelector">The selector that supplies the signal that ends a window.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="closingSelector"/> is <see langword="null"/>.</exception>
    public SliceClosingWitness(IObserver<IObservable<T>> observer, Func<IObservable<TClosing>> closingSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        _closingSelector = closingSelector ?? throw new ArgumentNullException(nameof(closingSelector));
        Subscription = new();
        Subscription.Attach(_upstream);
        _router = new(observer, Subscription);
        _current = new(Subscription);
    }

    /// <summary>Gets the subscription handed to the outer subscriber; the source stays subscribed while any window is.</summary>
    public SharedSubscription Subscription { get; }

    /// <summary>Opens the first window and starts watching its closing signal before the source is subscribed.</summary>
    public void Start()
    {
        lock (_gate)
        {
            _router.Open(_current);
        }

        Deliver();
        WatchClosing();
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

    /// <summary>Assigns the source subscription, disposing the incoming one when this sink has been disposed.</summary>
    /// <param name="subscription">The source subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => _upstream.Add(subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _done, 1);
        Subscription.Release();
        _upstream.Dispose();
    }

    /// <summary>Asks for the closing signal of the current window and subscribes to it.</summary>
    private void WatchClosing()
    {
        IObservable<TClosing> closing;
        try
        {
            closing = _closingSelector();
        }
        catch (Exception error)
        {
            Terminate(error);
            return;
        }

        ClosingWitness witness = new(this);
        _upstream.Add(witness);
        witness.SetSubscription(closing.Subscribe(witness));
    }

    /// <summary>Ends the current window, opens the next and watches its closing signal.</summary>
    /// <param name="witness">The closing subscription that fired.</param>
    private void Advance(ClosingWitness witness)
    {
        _ = _upstream.Remove(witness);
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
        WatchClosing();
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

    /// <summary>Observes the closing signal of one window.</summary>
    /// <param name="parent">The owning sink.</param>
    private sealed class ClosingWitness(SliceClosingWitness<T, TClosing> parent) : IObserver<TClosing>, IDisposable
    {
        /// <summary>Non-zero once the closing signal has ended the window.</summary>
        private int _fired;

        /// <summary>The closing subscription.</summary>
        private IDisposable? _subscription;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(TClosing value) => Fire();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.Terminate(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => Fire();

        /// <summary>Assigns the closing subscription, disposing the incoming one when this witness has been disposed.</summary>
        /// <param name="subscription">The closing subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => SinkSubscription.Dispose(ref _subscription);

        /// <summary>Ends the window the first time the closing signal emits or completes.</summary>
        private void Fire()
        {
            if (Interlocked.Exchange(ref _fired, 1) != 0)
            {
                return;
            }

            parent.Advance(this);
        }
    }
}
