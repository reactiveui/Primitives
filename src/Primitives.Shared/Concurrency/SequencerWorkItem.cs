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
internal sealed class SequencerWorkItem<TSequencer, TState> : IDisposable
    where TSequencer : ISequencer
{
    /// <summary>Sequencer passed to the scheduled action.</summary>
    private readonly TSequencer _sequencer;

    /// <summary>State passed to the scheduled action.</summary>
    private readonly TState _state;

    /// <summary>Action invoked when the scheduled item runs.</summary>
    private readonly Func<ISequencer, TState, IDisposable> _action;

    /// <summary>Disposable returned by the scheduled action after it starts.</summary>
    private IDisposable? _disposable;

    /// <summary>Tracks cancellation.</summary>
    private int _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="SequencerWorkItem{TSequencer, TState}"/> class.</summary>
    /// <param name="sequencer">Sequencer passed to the action.</param>
    /// <param name="state">State passed to the action.</param>
    /// <param name="action">Action to invoke.</param>
    public SequencerWorkItem(TSequencer sequencer, TState state, Func<ISequencer, TState, IDisposable> action)
    {
        _sequencer = sequencer;
        _state = state;
        _action = action;
    }

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
    public void Invoke()
    {
        if (Volatile.Read(ref _isDisposed) != 0)
        {
            return;
        }

        var disposable = _action(_sequencer, _state) ?? EmptyDisposable.Instance;
        var previous = Interlocked.CompareExchange(ref _disposable, disposable, null);
        if (previous is not null)
        {
            disposable.Dispose();
            return;
        }

        if (Volatile.Read(ref _isDisposed) == 0)
        {
            return;
        }

        if (!ReferenceEquals(Interlocked.CompareExchange(ref _disposable, EmptyDisposable.Instance, disposable), disposable))
        {
            return;
        }

        disposable.Dispose();
    }
}
