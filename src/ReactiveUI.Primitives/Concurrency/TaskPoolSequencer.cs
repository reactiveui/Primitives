// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// TaskPoolSequencer.
/// </summary>
/// <seealso cref="ReactiveUI.Primitives.Concurrency.ISequencer" />
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public sealed class TaskPoolSequencer : ISequencer
{
    private readonly TaskFactory _taskFactory;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskPoolSequencer"/> class.
    /// </summary>
    /// <param name="taskFactory">The task factory.</param>
    public TaskPoolSequencer(TaskFactory taskFactory) => _taskFactory = taskFactory;

    /// <summary>
    /// Gets the instance.
    /// </summary>
    /// <value>
    /// The instance.
    /// </value>
    public static TaskPoolSequencer Instance { get; } = new(Task.Factory);

    /// <summary>
    /// Gets the default task-pool scheduler.
    /// </summary>
    public static TaskPoolSequencer Default => Instance;

    /// <summary>
    /// Gets the scheduler's notion of current time.
    /// </summary>
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>
    /// Schedules an action to be executed.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var cancellationDisposable = new CancellationDisposable();
#pragma warning disable CA2008 // Do not create tasks without passing a TaskScheduler
        _taskFactory.StartNew(
             (_) =>
         {
             try
             {
                 return action(this, state);
             }
             catch (Exception ex)
             {
                 var thread = new Thread(() => ex.Rethrow());
                 thread.Start();
                 thread.Join();
                 return Disposable.Empty;
             }
         },
             cancellationDisposable.Token);
#pragma warning restore CA2008 // Do not create tasks without passing a TaskScheduler

        return cancellationDisposable;
    }

    /// <summary>
    /// Schedules an action to be executed after dueTime.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var g = new MultipleDisposable(new IDisposable[0]);
        g.Add(ThreadPoolSequencer.Instance.Schedule(state, Sequencer.Normalize(dueTime), action));
        return g;
    }

    /// <summary>
    /// Schedules an action to be executed at dueTime.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action) =>
        Schedule(state, Sequencer.Normalize(dueTime - Now), action);
}
