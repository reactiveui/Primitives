// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that hands out a window signal holding a fixed number of values, opening a new window every skip values.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>Windows end when the source ends. Source notifications must be serialized, as the observer contract requires.</remarks>
[System.Diagnostics.DebuggerDisplay("SliceCountWitness: Count = {_count}, Skip = {_skip}, Open = {_windows.Count}, Done = {_done}")]
public sealed class SliceCountWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The number of values in each window.</summary>
    private readonly int _count;

    /// <summary>The number of values between the starts of consecutive windows.</summary>
    private readonly int _skip;

    /// <summary>The windows still filling, oldest first.</summary>
    private readonly List<SliceWindow<T>> _windows = [];

    /// <summary>Delivers windows and their values in the order they were posted.</summary>
    private readonly SliceRouter<IObservable<T>, T> _router;

    /// <summary>The number of values seen.</summary>
    private int _index;

    /// <summary>The terminal latch; non-zero once the sink has terminated and must ignore further notifications.</summary>
    private int _done;

    /// <summary>Initializes a new instance of the <see cref="SliceCountWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer of the windows.</param>
    /// <param name="count">The number of values in each window.</param>
    /// <param name="skip">The number of values between the starts of consecutive windows.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> or <paramref name="skip"/> is zero or negative.</exception>
    public SliceCountWitness(IObserver<IObservable<T>> observer, int count, int skip)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(count);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(skip);
        _count = count;
        _skip = skip;
        Subscription = new();
        _router = new(observer, Subscription);
    }

    /// <summary>Gets the subscription handed to the outer subscriber; the source stays subscribed while any window is.</summary>
    public SharedSubscription Subscription { get; }

    /// <summary>Opens the first window before the source is subscribed.</summary>
    public void Start()
    {
        OpenWindow();
        Deliver();
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Volatile.Read(ref _done) != 0)
        {
            return;
        }

        for (var i = 0; i < _windows.Count; i++)
        {
            _router.Publish(_windows[i], value);
        }

        var filled = _index - _count + 1;
        if (filled >= 0 && filled % _skip == 0)
        {
            var oldest = _windows[0];
            _windows.RemoveAt(0);
            _router.Complete(oldest);
        }

        _index++;
        if (_index % _skip == 0)
        {
            OpenWindow();
        }

        Deliver();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => Terminate(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => Terminate(null);

    /// <summary>Assigns the upstream subscription, disposing the incoming one when this sink holds a subscription or has been disposed.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => Subscription.Attach(subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _done, 1);
        Subscription.Release();
    }

    /// <summary>Opens a window and posts it to the outer observer.</summary>
    private void OpenWindow()
    {
        SliceWindow<T> window = new(Subscription);
        _windows.Add(window);
        _router.Open(window);
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
        if (Interlocked.Exchange(ref _done, 1) != 0)
        {
            return;
        }

        try
        {
            _router.Finish(_windows, error);
            _router.Flush();
        }
        finally
        {
            Dispose();
        }
    }
}
