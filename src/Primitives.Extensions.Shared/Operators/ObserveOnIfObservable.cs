// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Schedules values per the latest <paramref name="condition"/> value; terminal notifications bypass scheduling.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="condition">The reactive condition observable.</param>
/// <param name="trueScheduler">The scheduler to use when condition is true.</param>
/// <param name="falseScheduler">The scheduler to use when condition is false.</param>
[System.Diagnostics.DebuggerDisplay("ObserveOnIfObservable: Source = {_source}")]
public sealed class ObserveOnIfObservable<T>(
    IObservable<T> source,
    IObservable<bool> condition,
    ISequencer trueScheduler,
    ISequencer falseScheduler) : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = InvalidOperationExceptionHelper.Check(source);

    /// <summary>The reactive condition observable.</summary>
    private readonly IObservable<bool> _condition = InvalidOperationExceptionHelper.Check(condition);

    /// <summary>The scheduler to use when condition is true.</summary>
    private readonly ISequencer _trueScheduler = InvalidOperationExceptionHelper.Check(trueScheduler);

    /// <summary>The scheduler to use when condition is false.</summary>
    private readonly ISequencer _falseScheduler = InvalidOperationExceptionHelper.Check(falseScheduler);

    /// <summary>Initializes a new instance of the <see cref="ObserveOnIfObservable{T}"/> class that runs inline when the condition is <see langword="false"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="condition">The reactive condition observable.</param>
    /// <param name="scheduler">The scheduler to use when condition is true.</param>
    public ObserveOnIfObservable(
        IObservable<T> source,
        IObservable<bool> condition,
        ISequencer scheduler)
        : this(source, condition, scheduler, Sequencer.Immediate)
    {
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ObserveOnIfSink sink = new(observer, _trueScheduler, _falseScheduler);
        sink.SetConditionSubscription(_condition.Subscribe(sink.ConditionObserver));
        sink.SetSourceSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Sink that forwards each value on whichever scheduler the latest condition value selects.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="trueScheduler">The scheduler to use when condition is true.</param>
    /// <param name="falseScheduler">The scheduler to use when condition is false.</param>
    private sealed class ObserveOnIfSink(
        IObserver<T> downstream,
        ISequencer trueScheduler,
        ISequencer falseScheduler) : IObserver<T>, IDisposable
    {
        /// <summary>The gate for synchronization.</summary>
        private readonly Lock _gate = new();

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _downstream = InvalidOperationExceptionHelper.Check(downstream);

        /// <summary>The scheduler to use when condition is true.</summary>
        private readonly ISequencer _trueScheduler = InvalidOperationExceptionHelper.Check(trueScheduler);

        /// <summary>The scheduler to use when condition is false.</summary>
        private readonly ISequencer _falseScheduler = InvalidOperationExceptionHelper.Check(falseScheduler);

        /// <summary>The condition stream subscription.</summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Usage",
            "CA2213:Disposable fields should be disposed",
            Justification = "Disposed atomically via SinkSubscription.Dispose.")]
        private IDisposable? _conditionSub;

        /// <summary>The source stream subscription.</summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Usage",
            "CA2213:Disposable fields should be disposed",
            Justification = "Disposed atomically via SinkSubscription.Dispose.")]
        private IDisposable? _sourceSub;

        /// <summary>The scheduler chosen by the latest condition value.</summary>
        private ISequencer _currentScheduler = falseScheduler;

        /// <summary>The last condition value.</summary>
        private bool _lastCondition;

        /// <summary>Whether the condition has been received.</summary>
        private bool _hasCondition;

        /// <summary>Whether the sequence is done.</summary>
        private bool _done;

        /// <summary>Gets a new observer that feeds condition values into this sink.</summary>
        public IObserver<bool> ConditionObserver => new ConditionObserverImpl(this);

        /// <summary>Assigns the condition subscription.</summary>
        /// <param name="subscription">The condition subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetConditionSubscription(IDisposable subscription) =>
            SinkSubscription.Set(ref _conditionSub, subscription);

        /// <summary>Assigns the source subscription.</summary>
        /// <param name="subscription">The source subscription.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetSourceSubscription(IDisposable subscription) =>
            SinkSubscription.Set(ref _sourceSub, subscription);

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            ISequencer scheduler;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                scheduler = _currentScheduler;
            }

            _ = scheduler.Schedule((Sink: this, Value: value), static state => state.Sink.ForwardNext(state.Value));
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
            }

            try
            {
                _downstream.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
            }

            try
            {
                _downstream.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
            }

            SinkSubscription.Dispose(ref _sourceSub);
            SinkSubscription.Dispose(ref _conditionSub);
        }

        /// <summary>Updates the scheduler selected by the condition stream.</summary>
        /// <param name="conditionValue">The latest condition value.</param>
        private void UpdateCondition(bool conditionValue)
        {
            lock (_gate)
            {
                if (_hasCondition && _lastCondition == conditionValue)
                {
                    return;
                }

                _currentScheduler = conditionValue ? _trueScheduler : _falseScheduler;
                _lastCondition = conditionValue;
                _hasCondition = true;
            }
        }

        /// <summary>Forwards a scheduled value downstream unless the sink has terminated.</summary>
        /// <param name="value">The value to forward.</param>
        private void ForwardNext(T value)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }
            }

            _downstream.OnNext(value);
        }

        /// <summary>Observer that pushes condition values into the sink and ignores the condition's terminal notifications.</summary>
        /// <param name="sink">The owning sink.</param>
        private sealed class ConditionObserverImpl(ObserveOnIfSink sink) : IObserver<bool>
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnNext(bool value) => sink.UpdateCondition(value);

            /// <inheritdoc/>
            public void OnError(Exception error)
            {
            }

            /// <inheritdoc/>
            public void OnCompleted()
            {
            }
        }
    }
}
