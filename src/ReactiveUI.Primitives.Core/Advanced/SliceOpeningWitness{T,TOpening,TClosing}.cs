// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Sink that hands out a window signal each time an opening signal emits, and ends each window when its closing signal fires.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TOpening">The value type of the opening signal.</typeparam>
/// <typeparam name="TClosing">The value type of the closing signals.</typeparam>
/// <remarks>
/// Windows can overlap, and each receives every source value while it is open. The outer sequence completes when the
/// opening signal completes; windows that are open then keep receiving values until their closing signal fires or the
/// source ends. A failure anywhere faults every open window and the outer sequence. Notifications are delivered outside
/// the sink's lock.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SliceOpeningWitness: Open = {_open.Count}, Done = {_done}")]
public sealed class SliceOpeningWitness<T, TOpening, TClosing> : IObserver<T>, IDisposable
{
    /// <summary>Serializes access to the open windows and the terminal latch.</summary>
    private readonly Lock _gate = new();

    /// <summary>The selector that supplies the signal that ends the window an opening started.</summary>
    private readonly Func<TOpening, IObservable<TClosing>> _closingSelector;

    /// <summary>The windows that have not ended, oldest first.</summary>
    private readonly List<SliceWindow<T>> _open = [];

    /// <summary>Delivers windows and their values in the order they were posted.</summary>
    private readonly SliceRouter<IObservable<T>, T> _router;

    /// <summary>The source subscription, the opening subscription and the live closing subscriptions.</summary>
    private readonly MultipleDisposable _upstream = [];

    /// <summary>The observer of the opening signal, created on first use.</summary>
    private IObserver<TOpening>? _openings;

    /// <summary>Whether the outer sequence has terminated.</summary>
    private bool _outerDone;

    /// <summary>The terminal latch; non-zero once the sink has terminated and must ignore further notifications.</summary>
    private int _done;

    /// <summary>Initializes a new instance of the <see cref="SliceOpeningWitness{T, TOpening, TClosing}"/> class.</summary>
    /// <param name="observer">The downstream observer of the windows.</param>
    /// <param name="closingSelector">The selector that supplies the signal that ends the window an opening started.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="closingSelector"/> is <see langword="null"/>.</exception>
    public SliceOpeningWitness(IObserver<IObservable<T>> observer, Func<TOpening, IObservable<TClosing>> closingSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        _closingSelector = closingSelector ?? throw new ArgumentNullException(nameof(closingSelector));
        Subscription = new();
        Subscription.Attach(_upstream);
        _router = new(observer, Subscription);
    }

    /// <summary>Gets the subscription handed to the outer subscriber; the source stays subscribed while any window is.</summary>
    public SharedSubscription Subscription { get; }

    /// <summary>Gets the observer to subscribe to the opening signal.</summary>
    public IObserver<TOpening> Openings => _openings ??= new OpeningWitness(this);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            for (var i = 0; i < _open.Count; i++)
            {
                _router.Publish(_open[i], value);
            }
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
    /// <param name="subscription">The source or opening subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => _upstream.Add(subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _done, 1);
        Subscription.Release();
        _upstream.Dispose();
    }

    /// <summary>Starts a window for an opening and watches its closing signal.</summary>
    /// <param name="opening">The opening value.</param>
    private void Open(TOpening opening)
    {
        IObservable<TClosing> closing;
        try
        {
            closing = _closingSelector(opening);
        }
        catch (Exception error)
        {
            Terminate(error);
            return;
        }

        SliceWindow<T> window = new(Subscription);
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            _open.Add(window);
            _router.Open(window);
        }

        Deliver();

        ClosingWitness witness = new(this, window);
        _upstream.Add(witness);
        witness.SetSubscription(closing.Subscribe(witness));
    }

    /// <summary>Ends a window when its closing signal fires.</summary>
    /// <param name="window">The window to end.</param>
    /// <param name="witness">The closing subscription to release.</param>
    private void Close(SliceWindow<T> window, ClosingWitness witness)
    {
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            if (_open.Remove(window))
            {
                _router.Complete(window);
            }
        }

        Deliver();
        _ = _upstream.Remove(witness);
    }

    /// <summary>Completes the outer sequence when the opening signal completes; open windows stay open.</summary>
    private void CompleteOuter()
    {
        lock (_gate)
        {
            if (_done != 0 || _outerDone)
            {
                return;
            }

            _outerDone = true;
            _router.CompleteOuter();
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

    /// <summary>Ends every open window and the outer sequence, then tears the sink down.</summary>
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
            if (_outerDone)
            {
                _router.FinishWindows(_open, error);
            }
            else
            {
                _router.Finish(_open, error);
            }
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

    /// <summary>Observes the opening signal.</summary>
    /// <param name="parent">The owning sink.</param>
    private sealed class OpeningWitness(SliceOpeningWitness<T, TOpening, TClosing> parent) : IObserver<TOpening>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(TOpening value) => parent.Open(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.Terminate(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => parent.CompleteOuter();
    }

    /// <summary>Observes the closing signal of one window.</summary>
    /// <param name="parent">The owning sink.</param>
    /// <param name="window">The window the signal ends.</param>
    private sealed class ClosingWitness(SliceOpeningWitness<T, TOpening, TClosing> parent, SliceWindow<T> window) : IObserver<TClosing>, IDisposable
    {
        /// <summary>The closing subscription.</summary>
        private IDisposable? _subscription;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(TClosing value) => parent.Close(window, this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => parent.Terminate(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => parent.Close(window, this);

        /// <summary>Assigns the closing subscription, disposing the incoming one when this witness has been disposed.</summary>
        /// <param name="subscription">The closing subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => SinkSubscription.Dispose(ref _subscription);
    }
}
