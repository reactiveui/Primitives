// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Loops the supplied <see cref="Action"/> on the supplied
/// <see cref="ISequencer"/> (or inline when no scheduler is provided), emitting
/// <see cref="RxVoid.Default"/> after each iteration, for as long as
/// <paramref name="condition"/> returns <c>true</c>. Replaces the legacy
/// <c>Observable.While(condition, Observable.Start(action, scheduler))</c>
/// pattern.
/// </summary>
/// <param name="condition">The loop predicate. Evaluated before each iteration.</param>
/// <param name="action">The action to invoke per iteration.</param>
/// <param name="scheduler">An optional scheduler; <c>null</c> runs every iteration inline.</param>
internal sealed class WhileObservable(
    Func<bool> condition,
    Action action,
    ISequencer? scheduler) : IObservable<RxVoid>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(condition);
        InvalidOperationExceptionHelper.ThrowIfNull(action);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        WhileSink sink = new(observer, condition, action, scheduler);
        sink.Run();
        return sink;
    }

    /// <summary>
    /// Sink that orchestrates the iteration loop, scheduling the next iteration
    /// after each emission and terminating when the predicate becomes false.
    /// </summary>
    private sealed class WhileSink : IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<RxVoid> _downstream;

        /// <summary>The loop predicate.</summary>
        private readonly Func<bool> _condition;

        /// <summary>The action invoked per iteration.</summary>
        private readonly Action _action;

        /// <summary>An optional scheduler used per iteration.</summary>
        private readonly ISequencer? _scheduler;

        /// <summary>The disposable tracking the currently-scheduled iteration.</summary>
        private readonly MutableDisposable _current = new();

        /// <summary>Whether the sink has been disposed.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="WhileSink"/> class.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="condition">The loop predicate.</param>
        /// <param name="action">The action invoked per iteration.</param>
        /// <param name="scheduler">An optional scheduler used per iteration.</param>
        public WhileSink(IObserver<RxVoid> downstream, Func<bool> condition, Action action, ISequencer? scheduler)
        {
            _downstream = downstream;
            _condition = condition;
            _action = action;
            _scheduler = scheduler;
        }

        /// <summary>Starts the loop.</summary>
        public void Run() => Iterate();

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
            {
                return;
            }

            _current.Dispose();
        }

        /// <summary>Performs a single iteration: checks the predicate, runs the action, emits <see cref="RxVoid.Default"/>, and schedules the next iteration.</summary>
        private void Iterate()
        {
            if (Volatile.Read(ref _disposed) == 1)
            {
                return;
            }

            try
            {
                if (!_condition())
                {
                    _downstream.OnCompleted();
                    return;
                }
            }
            catch (Exception error)
            {
                _downstream.OnError(error);
                return;
            }

            if (_scheduler is null)
            {
                RunActionAndContinue();
                return;
            }

            _current.Disposable = _scheduler.Schedule(this, static (_, self) =>
            {
                self.RunActionAndContinue();
                return EmptyDisposable.Instance;
            });
        }

        /// <summary>Runs the per-iteration action, emits <see cref="RxVoid.Default"/>, and re-enters <see cref="Iterate"/> to evaluate the predicate for the next iteration.</summary>
        private void RunActionAndContinue()
        {
            if (Volatile.Read(ref _disposed) == 1)
            {
                return;
            }

            try
            {
                _action();
            }
            catch (Exception error)
            {
                _downstream.OnError(error);
                return;
            }

            _downstream.OnNext(RxVoid.Default);
            Iterate();
        }
    }
}
