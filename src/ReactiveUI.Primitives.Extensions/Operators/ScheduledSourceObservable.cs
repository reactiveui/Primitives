// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Source-driven scheduled observable. Subscribes to an upstream
/// <see cref="IObservable{T}"/> and, for every emitted value, schedules a
/// callback on the supplied <see cref="ISequencer"/> that applies an optional
/// <see cref="Action{T}"/> side-effect and/or <see cref="Func{T,T}"/> transform
/// before forwarding the value to the downstream observer. Replaces the
/// <c>Observable.Create&lt;T&gt;(o =&gt; source.Subscribe(v =&gt; scheduler.Schedule(...)))</c>
/// family of source-driven <c>Schedule</c> overloads.
/// </summary>
/// <typeparam name="T">The element type of the source observable.</typeparam>
/// <remarks>
/// To match the original <c>source.Subscribe(Action&lt;T&gt;)</c> semantics, this
/// operator only forwards <see cref="IObserver{T}.OnNext"/>. Source errors and
/// completion are intentionally not propagated to the downstream observer; that
/// preserves the historical behaviour of <c>Observable.Create</c> + a
/// next-only subscription.
/// </remarks>
internal sealed class ScheduledSourceObservable<T> : IObservable<T>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The bundled scheduling configuration shared across every subscription.</summary>
    private readonly ScheduleConfig<T> _config;

    /// <summary>
    /// Initializes a new instance of the <see cref="ScheduledSourceObservable{T}"/> class.
    /// </summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="config">Bundled scheduling configuration (scheduler, optional delay, optional transform/action).</param>
    public ScheduledSourceObservable(IObservable<T> source, ScheduleConfig<T> config)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(config.Scheduler);
        _source = source;
        _config = config;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new ScheduledSourceObserver(observer, _config);
        return _source.Subscribe(sink);
    }

    /// <summary>
    /// Carries the per-emission state into the scheduled callback so the
    /// scheduler lambda does not capture any fields. A <see langword="readonly"/>
    /// <see langword="record"/> <see langword="struct"/> so it rides inside the
    /// scheduler's work item by value rather than as a separate per-emission heap
    /// allocation.
    /// </summary>
    /// <param name="Observer">The downstream observer.</param>
    /// <param name="Value">The value to emit.</param>
    /// <param name="Transform">The optional transform.</param>
    /// <param name="Action">The optional side-effect.</param>
    private readonly record struct EmitState(
        IObserver<T> Observer,
        T Value,
        Func<T, T>? Transform,
        Action<T>? Action)
    {
        /// <summary>
        /// Applies the optional side-effect and transform, then emits the value
        /// to the captured observer.
        /// </summary>
        public void Emit()
        {
            try
            {
                Action?.Invoke(Value);
                var emitted = Transform is null ? Value : Transform(Value);
                Observer.OnNext(emitted);
            }
            catch (Exception error)
            {
                Observer.OnError(error);
            }
        }
    }

    /// <summary>
    /// Per-value sink that captures the configured scheduling parameters once
    /// and schedules each <see cref="OnNext"/> through the configured
    /// <see cref="ISequencer"/>.
    /// </summary>
    private sealed class ScheduledSourceObserver(IObserver<T> downstream, ScheduleConfig<T> config) : IObserver<T>
    {
        /// <inheritdoc/>
        public void OnNext(T value)
        {
            var state = new EmitState(downstream, value, config.Transform, config.Action);
            var scheduler = config.Scheduler;

            if (!config.HasDelay)
            {
                scheduler.Schedule(state, static (_, s) =>
                {
                    s.Emit();
                    return EmptyDisposable.Instance;
                });
                return;
            }

            if (config.UseAbsolute)
            {
                scheduler.Schedule(state, config.AbsoluteDueTime, static (_, s) =>
                {
                    s.Emit();
                    return EmptyDisposable.Instance;
                });
                return;
            }

            scheduler.Schedule(state, config.DueTime, static (_, s) =>
            {
                s.Emit();
                return EmptyDisposable.Instance;
            });
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            // Intentionally not forwarded: original Observable.Create + Subscribe(Action<T>)
            // pattern silently dropped source errors. Preserving that behaviour.
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            // Intentionally not forwarded: original Observable.Create + Subscribe(Action<T>)
            // pattern silently dropped completion. Preserving that behaviour.
        }
    }
}
