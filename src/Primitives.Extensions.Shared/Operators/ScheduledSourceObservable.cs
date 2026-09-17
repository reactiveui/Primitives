// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Schedules source values without forwarding errors or completion.</summary>
/// <typeparam name="T">The element type of the source observable.</typeparam>
internal sealed class ScheduledSourceObservable<T> : IObservable<T>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The bundled scheduling configuration shared across every subscription.</summary>
    private readonly ScheduleConfig<T> _config;

    /// <summary>Initializes a new instance of the <see cref="ScheduledSourceObservable{T}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="config">Bundled scheduling configuration (scheduler, optional delay, optional transform/action).</param>
    public ScheduledSourceObservable(IObservable<T> source, in ScheduleConfig<T> config)
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

        ScheduledSourceWitness sink = new(observer, _config);
        return _source.Subscribe(sink);
    }

    /// <summary>Carries the per-emission state by value into the scheduled callback so the scheduler lambda captures nothing.</summary>
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
        /// <summary>Applies the optional side-effect and transform, then emits the value to the captured observer.</summary>
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

    /// <summary>Per-value sink that schedules each <see cref="OnNext"/> through the configured sequencer.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="config">The captured scheduling configuration.</param>
    private sealed class ScheduledSourceWitness(IObserver<T> downstream, ScheduleConfig<T> config) : IObserver<T>
    {
        /// <inheritdoc/>
        public void OnNext(T value)
        {
            EmitState state = new(downstream, value, config.Transform, config.Action);
            var scheduler = config.Scheduler;

            if (!config.HasDelay)
            {
                _ = scheduler.Schedule(state, static (_, s) =>
                {
                    s.Emit();
                    return EmptyDisposable.Instance;
                });
                return;
            }

            if (config.UseAbsolute)
            {
                _ = scheduler.Schedule(state, config.AbsoluteDueTime, static (_, s) =>
                {
                    s.Emit();
                    return EmptyDisposable.Instance;
                });
                return;
            }

            _ = scheduler.Schedule(state, config.DueTime, static (_, s) =>
            {
                s.Emit();
                return EmptyDisposable.Instance;
            });
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            // Terminal notifications are not forwarded.
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            // Terminal notifications are not forwarded.
        }
    }
}
