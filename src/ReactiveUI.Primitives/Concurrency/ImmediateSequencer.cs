// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// ImmediateSequencer.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed partial class ImmediateSequencer : ISequencer
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
    /// Gets the scheduler's monotonic timestamp.
    /// </summary>
    public long Timestamp => Sequencer.Timestamp;

    /// <summary>
    /// Schedules an action to run immediately.
    /// </summary>
    /// <param name="action">Action to execute.</param>
    /// <returns>An empty disposable because the action has already run.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    #pragma warning disable CA1822 // Mark members as static
    public IDisposable Schedule(Action action)
    #pragma warning restore CA1822 // Mark members as static
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        action();
        return Disposable.Empty;
    }

    /// <summary>
    /// Schedules the specified work item.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item)
    {
        if (item is null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        if (Sequencer.IsCancelled(item))
        {
            return;
        }

        item.Execute();
    }

    /// <summary>
    /// Schedules the specified work item.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        if (item is null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        var dueTime = Sequencer.TimeUntil(dueTimestamp);
        if (dueTime.Ticks > 0)
        {
            Thread.Sleep(dueTime);
        }

        if (Sequencer.IsCancelled(item))
        {
            return;
        }

        item.Execute();
    }
}
