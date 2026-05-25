// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// ImmediateSequencer.
/// </summary>
/// <seealso cref="ReactiveUI.Primitives.Concurrency.ISequencer" />
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public sealed class ImmediateSequencer : ISequencer
{
    /// <summary>
    /// Singleton holder for the immediate sequencer.
    /// </summary>
    private static readonly Lazy<ImmediateSequencer> StaticInstance = new(static () => new ImmediateSequencer());

    /// <summary>
    /// Initializes a new instance of the <see cref="ImmediateSequencer"/> class.
    /// </summary>
    private ImmediateSequencer()
    {
    }

    /// <summary>
    /// Gets the singleton instance of the immediate scheduler.
    /// </summary>
    public static ImmediateSequencer Instance => StaticInstance.Value;

    /// <summary>
    /// Gets the scheduler's notion of current time.
    /// </summary>
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>
    /// Schedules the specified state.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="state">The state.</param>
    /// <param name="action">The action.</param>
    /// <returns>An IDisposable.</returns>
    public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return action(this, state);
    }

    /// <summary>
    /// Schedules the specified state.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="state">The state.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="action">The action.</param>
    /// <returns>An IDisposable.</returns>
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (action is null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var dt = Sequencer.Normalize(dueTime);
        if (dt.Ticks > 0)
        {
            Thread.Sleep(dt);
        }

        return action(this, state);
    }

    /// <summary>
    /// Schedules the specified state.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="state">The state.</param>
    /// <param name="dueTime">The due time.</param>
    /// <param name="action">The action.</param>
    /// <returns>An IDisposable.</returns>
    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        var due = Sequencer.Normalize(dueTime - Now);
        return Schedule(state, due, action);
    }
}
