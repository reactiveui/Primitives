// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Sink that annotates each value with the elapsed interval since the previous value.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TimeIntervalWitness: First = {_first}, Last = {_last}")]
public sealed class TimeIntervalWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TimeInterval<T>> _observer;

    /// <summary>The sequencer that supplies timestamps.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>The timestamp of the previous value.</summary>
    private DateTimeOffset _last;

    /// <summary>A value indicating whether the next value is the first.</summary>
    private bool _first = true;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="TimeIntervalWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="scheduler">The sequencer that supplies timestamps.</param>
    public TimeIntervalWitness(IObserver<TimeInterval<T>> observer, ISequencer scheduler)
    {
        _observer = observer;
        _scheduler = scheduler;
        _last = scheduler.Now;
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        var now = _scheduler.Now;
        var interval = _first ? TimeSpan.Zero : now - _last;
        _first = false;
        _last = now;
        _observer.OnNext(new(value, interval));
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => SinkTerminal.Complete(_observer, this);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
