// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>A sequencer that executes work immediately on the calling thread.</summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ImmediateSequencer : ISequencer
{
    /// <summary>Singleton holder for the immediate sequencer.</summary>
    private static readonly Lazy<ImmediateSequencer> StaticInstance = new(static () => new());

    /// <summary>Initializes a new instance of the <see cref="ImmediateSequencer"/> class.</summary>
    private ImmediateSequencer()
    {
    }

    /// <summary>Gets the singleton instance of the immediate scheduler.</summary>
    public static ImmediateSequencer Instance => StaticInstance.Value;

    /// <summary>Gets the scheduler's notion of current time.</summary>
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>Gets the scheduler's monotonic timestamp.</summary>
    public long Timestamp => Sequencer.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Schedules an action to run immediately.</summary>
    /// <param name="action">Action to execute.</param>
    /// <returns>An empty disposable because the action has already run.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="action"/> is <see langword="null"/>.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Performance",
        "CA1822:Mark members as static",
        Justification =
            "Must remain an instance method so overload resolution selects it over the allocating ISequencer.Schedule(Action) extension, giving an allocation-free immediate path.")]
    public IDisposable Schedule(Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        action();
        return EmptyDisposable.Instance;
    }

    /// <summary>Schedules the specified work item.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        if (Sequencer.IsCancelled(item))
        {
            return;
        }

        item.Execute();
    }

    /// <summary>Schedules the specified work item.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

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
