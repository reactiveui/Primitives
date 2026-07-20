// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Concurrency;
#else
namespace ReactiveUI.Primitives.Concurrency;
#endif

/// <summary>Disposable scheduled work item used by UI-backed sequencers.</summary>
/// <typeparam name="TSequencer">The sequencer type passed to the scheduled action.</typeparam>
/// <typeparam name="TState">The scheduled state type.</typeparam>
/// <param name="sequencer">Sequencer passed to the action.</param>
/// <param name="state">State passed to the action.</param>
/// <param name="action">Action to invoke.</param>
internal sealed class SequencerWorkItem<TSequencer, TState>(TSequencer sequencer, TState state, Func<ISequencer, TState, IDisposable> action) : IDisposable
    where TSequencer : ISequencer
{
    /// <summary>Sequencer passed to the scheduled action.</summary>
    private readonly TSequencer _sequencer = sequencer;

    /// <summary>State passed to the scheduled action.</summary>
    private readonly TState _state = state;

    /// <summary>Action invoked when the scheduled item runs.</summary>
    private readonly Func<ISequencer, TState, IDisposable> _action = action;

    /// <summary>Disposable returned by the scheduled action after it starts.</summary>
    private IDisposable? _disposable;

    /// <summary>Tracks cancellation.</summary>
    private int _isDisposed;

    /// <summary>Cancels the work item.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
        {
            return;
        }

        Interlocked.Exchange(ref _disposable, EmptyDisposable.Instance)?.Dispose();
    }

    /// <summary>Invokes the scheduled action if it has not been cancelled.</summary>
    internal void Invoke()
    {
        if (Volatile.Read(ref _isDisposed) != 0)
        {
            return;
        }

        var disposable = _action(_sequencer, _state) ?? EmptyDisposable.Instance;
        SequencerWorkItemDisposal.Publish(ref _disposable, disposable);
    }
}
