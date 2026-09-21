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

    /// <summary>The clock supplying time and blocking waits.</summary>
    private readonly SequencerClock _clock;

    /// <summary>Blocks the scheduling thread for a delay measured by the clock.</summary>
    private readonly Action<TimeSpan> _wait;

    /// <summary>Initializes a new instance of the <see cref="ImmediateSequencer"/> class that reads time and waits through a <see cref="TimeProvider"/>.</summary>
    /// <param name="timeProvider">The provider supplying the current time, timestamps and timers.</param>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is <see langword="null"/>.</exception>
    public ImmediateSequencer(TimeProvider timeProvider)
        : this(new SequencerClock(timeProvider))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ImmediateSequencer"/> class; callers use <see cref="Instance"/>.</summary>
    private ImmediateSequencer()
        : this(SequencerClock.Default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ImmediateSequencer"/> class.</summary>
    /// <param name="clock">The clock supplying time and blocking waits.</param>
    private ImmediateSequencer(SequencerClock clock)
    {
        _clock = clock;
        _wait = clock.Wait;
    }

    /// <summary>Gets the singleton instance of the immediate scheduler.</summary>
    public static ImmediateSequencer Instance => StaticInstance.Value;

    /// <summary>Gets the scheduler's notion of current time.</summary>
    public DateTimeOffset Now => _clock.GetUtcNow();

    /// <summary>Gets the scheduler's monotonic timestamp.</summary>
    public long Timestamp => _clock.GetTimestamp();

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Schedules an action to run immediately.</summary>
    /// <param name="action">Action to execute.</param>
    /// <returns>An empty disposable; the action runs before this method returns.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="action"/> is <see langword="null"/>.</exception>
    public static IDisposable Schedule(Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        action();
        return EmptyDisposable.Instance;
    }

    /// <summary>Executes the work item on the calling thread unless it has been cancelled.</summary>
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

    /// <summary>Blocks the calling thread until the due timestamp, then executes the work item unless it has been cancelled.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        RunScheduled(item, _clock.TimeUntil(dueTimestamp), _wait);
    }

    /// <summary>Waits for a relative delay and executes work that remains active.</summary>
    /// <param name="item">The scheduled work.</param>
    /// <param name="dueTime">The remaining delay.</param>
    /// <param name="wait">The blocking wait operation.</param>
    internal static void RunScheduled(IWorkItem item, TimeSpan dueTime, Action<TimeSpan> wait)
    {
        if (dueTime.Ticks > 0)
        {
            wait(dueTime);
        }

        if (Sequencer.IsCancelled(item))
        {
            return;
        }

        item.Execute();
    }
}
