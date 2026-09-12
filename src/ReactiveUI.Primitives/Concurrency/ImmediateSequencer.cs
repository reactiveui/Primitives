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
    /// <returns>An empty disposable; the action runs before this method returns.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="action"/> is <see langword="null"/>.</exception>
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

        RunScheduled(item, Sequencer.TimeUntil(dueTimestamp), Wait);
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

    /// <summary>Blocks the scheduling thread for the remaining delay.</summary>
    /// <param name="dueTime">The remaining delay.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static void Wait(TimeSpan dueTime) => Thread.Sleep(dueTime);
}
