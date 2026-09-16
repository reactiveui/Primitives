// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for absolute <c>Expire</c>/<c>Timeout</c> overloads.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AbsoluteExpireSignal: DueTime = {_dueTime}, Source = {_source}")]
public sealed class AbsoluteExpireSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The absolute timeout time.</summary>
    private readonly DateTimeOffset _dueTime;

    /// <summary>The sequencer used to schedule the timeout.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="AbsoluteExpireSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The absolute timeout time.</param>
    /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public AbsoluteExpireSignal(IObservable<T> source, DateTimeOffset dueTime, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        _source = source;
        _dueTime = dueTime;
        _scheduler = scheduler;
    }

    /// <summary>Gets the sequencer used to schedule the timeout.</summary>
    internal ISequencer Scheduler => _scheduler;

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _scheduler == Sequencer.CurrentThread
        || (_source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread());

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var dueTime = Sequencer.Normalize(_dueTime - _scheduler.Now);
        return new ExpireSignal<T>(_source, dueTime, _scheduler).Subscribe(observer);
    }
}
