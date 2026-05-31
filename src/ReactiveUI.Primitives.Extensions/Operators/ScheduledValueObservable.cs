// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Single-value scheduled observable. On subscription, schedules a callback on
/// the supplied <see cref="ISequencer"/> that applies an optional
/// <see cref="Action{T}"/> side-effect and/or an optional <see cref="Func{T,T}"/>
/// transform to the captured value, calls <see cref="IObserver{T}.OnNext"/>
/// once, then <see cref="IObserver{T}.OnCompleted"/>. Replaces the
/// <c>Observable.Create&lt;T&gt;(o =&gt; scheduler.Schedule[Safe](due, () =&gt; o.OnNext(...)))</c>
/// family of single-value <c>Schedule</c> overloads with one dedicated type
/// that captures only the fields each overload actually uses.
/// </summary>
/// <typeparam name="T">The value type emitted to the downstream observer.</typeparam>
internal sealed class ScheduledValueObservable<T> : IObservable<T>
{
    /// <summary>The value to emit.</summary>
    private readonly T _value;

    /// <summary>The scheduler used to dispatch the emission.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>The relative delay if <see cref="_useAbsolute"/> is <c>false</c>.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The absolute due time if <see cref="_useAbsolute"/> is <c>true</c>.</summary>
    private readonly DateTimeOffset _absoluteDueTime;

    /// <summary>Optional transform applied to the value before emission.</summary>
    private readonly Func<T, T>? _transform;

    /// <summary>Optional side-effect invoked with the value before emission.</summary>
    private readonly Action<T>? _action;

    /// <summary><c>true</c> when <see cref="_absoluteDueTime"/> is used; otherwise <see cref="_dueTime"/> is used.</summary>
    private readonly bool _useAbsolute;

    /// <summary><c>true</c> when a delay is configured (relative or absolute).</summary>
    private readonly bool _hasDelay;

    /// <summary>
    /// Initializes a new instance of the <see cref="ScheduledValueObservable{T}"/> class.
    /// </summary>
    /// <param name="value">The value to emit.</param>
    /// <param name="scheduler">The scheduler used to dispatch the emission.</param>
    /// <param name="dueTime">Optional relative delay before emission.</param>
    /// <param name="absoluteDueTime">Optional absolute time at which to emit.</param>
    /// <param name="transform">Optional transform applied to the value before emission.</param>
    /// <param name="action">Optional side-effect invoked with the value before emission.</param>
    public ScheduledValueObservable(
        T value,
        ISequencer scheduler,
        TimeSpan? dueTime,
        DateTimeOffset? absoluteDueTime,
        Func<T, T>? transform,
        Action<T>? action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);
        _value = value;
        _scheduler = scheduler;
        _transform = transform;
        _action = action;

        if (absoluteDueTime.HasValue)
        {
            _absoluteDueTime = absoluteDueTime.Value;
            _useAbsolute = true;
            _hasDelay = true;
        }
        else if (dueTime.HasValue)
        {
            _dueTime = dueTime.Value;
            _hasDelay = true;
        }
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var state = new EmitState(observer, _value, _transform, _action);

        if (!_hasDelay)
        {
            return _scheduler.Schedule(state, static (_, s) =>
            {
                s.Emit();
                return EmptyDisposable.Instance;
            });
        }

        if (_useAbsolute)
        {
            return _scheduler.Schedule(state, _absoluteDueTime, static (_, s) =>
            {
                s.Emit();
                return EmptyDisposable.Instance;
            });
        }

        return _scheduler.Schedule(state, _dueTime, static (_, s) =>
        {
            s.Emit();
            return EmptyDisposable.Instance;
        });
    }

    /// <summary>
    /// Carries the per-subscription state into the scheduled callback so the
    /// scheduler lambda does not capture any fields.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The value to emit.</param>
    /// <param name="transform">The optional transform.</param>
    /// <param name="action">The optional side-effect.</param>
    private sealed class EmitState(
        IObserver<T> observer,
        T value,
        Func<T, T>? transform,
        Action<T>? action)
    {
        /// <summary>
        /// Applies the optional side-effect and transform, then emits the value
        /// followed by completion to the captured observer.
        /// </summary>
        public void Emit()
        {
            // Preserves the original Observable.Create-based semantics: the
            // scheduled callback only emits OnNext. The sequence completes
            // when downstream subscribers dispose; we do not auto-call
            // OnCompleted here.
            try
            {
                action?.Invoke(value);
                var emitted = transform is null ? value : transform(value);
                observer.OnNext(emitted);
            }
            catch (Exception error)
            {
                observer.OnError(error);
            }
        }
    }
}
