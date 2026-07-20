// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>
/// Bundled scheduling configuration shared by <see cref="ScheduledValueObservable{T}"/> and
/// <see cref="ScheduledSourceObservable{T}"/>. Carrying these parameters as a single readonly record
/// struct keeps observable/observer constructor parameter counts low, avoids SA1117-style parameter
/// soup, and lets the compiler copy the configuration into capture-free scheduler lambdas.
/// </summary>
/// <typeparam name="T">The element type emitted by the configured observable.</typeparam>
/// <param name="Scheduler">The scheduler on which each emission is dispatched.</param>
/// <param name="HasDelay"><c>true</c> when a delay (relative or absolute) is configured.</param>
/// <param name="UseAbsolute"><c>true</c> when <paramref name="AbsoluteDueTime"/> is used; <c>false</c> when <paramref name="DueTime"/> applies.</param>
/// <param name="DueTime">The relative delay before each emission.</param>
/// <param name="AbsoluteDueTime">The absolute time at which each emission fires.</param>
/// <param name="Transform">Optional transform applied to the value before emission.</param>
/// <param name="Action">Optional side-effect invoked with the value before emission.</param>
internal readonly record struct ScheduleConfig<T>(
    ISequencer Scheduler,
    bool HasDelay,
    bool UseAbsolute,
    TimeSpan DueTime,
    DateTimeOffset AbsoluteDueTime,
    Func<T, T>? Transform,
    Action<T>? Action)
{
    /// <summary>Creates a config with no delay, no transform, no action.</summary>
    /// <param name="scheduler">The scheduler used to dispatch emissions.</param>
    /// <returns>A new configuration.</returns>
    internal static ScheduleConfig<T> Immediate(ISequencer scheduler) =>
        new(scheduler, false, false, TimeSpan.Zero, default, null, null);

    /// <summary>Creates a config with a relative delay.</summary>
    /// <param name="scheduler">The scheduler used to dispatch emissions.</param>
    /// <param name="dueTime">The relative delay before each emission.</param>
    /// <returns>A new configuration.</returns>
    internal static ScheduleConfig<T> Delayed(ISequencer scheduler, TimeSpan dueTime) =>
        new(scheduler, true, false, dueTime, default, null, null);

    /// <summary>Creates a config with an absolute due time.</summary>
    /// <param name="scheduler">The scheduler used to dispatch emissions.</param>
    /// <param name="absoluteDueTime">The absolute time at which each emission fires.</param>
    /// <returns>A new configuration.</returns>
    internal static ScheduleConfig<T> Absolute(ISequencer scheduler, DateTimeOffset absoluteDueTime) =>
        new(scheduler, true, true, TimeSpan.Zero, absoluteDueTime, null, null);

    /// <summary>Returns a new config with the supplied transform applied to each value before emission.</summary>
    /// <param name="transform">The transform.</param>
    /// <returns>A new configuration.</returns>
    internal ScheduleConfig<T> WithTransform(Func<T, T> transform) => this with { Transform = transform };

    /// <summary>Returns a new config with the supplied side-effect invoked with each value before emission.</summary>
    /// <param name="action">The action.</param>
    /// <returns>A new configuration.</returns>
    internal ScheduleConfig<T> WithAction(Action<T> action) => this with { Action = action };
}
