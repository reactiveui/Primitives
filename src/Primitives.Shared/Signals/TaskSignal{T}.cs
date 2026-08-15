// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>A task-backed signal of values.</summary>
/// <typeparam name="T">The object that provides notification information.</typeparam>
internal sealed class TaskSignal<T> : ITaskSignal<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>Executes the new operation.</summary>
    /// <returns>The result.</returns>
    private readonly MultipleDisposable _cleanUp = [];

    /// <summary>Initializes a new instance of the <see cref="TaskSignal{T}" /> class.</summary>
    /// <param name="sequencer">The sequencer.</param>
    /// <param name="cancellationTokenSource">The cancellation token source.</param>
    /// <remarks>
    /// Private, so the factory can only ever see a signal that is already built. See <see cref="Create"/>.
    /// </remarks>
    private TaskSignal(ISequencer? sequencer, CancellationTokenSource? cancellationTokenSource)
    {
        CancellationTokenSource = cancellationTokenSource ?? new();
        _sequencer = sequencer ?? CurrentThreadSequencer.Instance;
    }

    /// <summary>Gets or sets the source.</summary>
    /// <value>
    /// The source.
    /// </value>
    public IObservable<T>? Source { get; set; }

    /// <summary>Gets the cancellation token source.</summary>
    /// <value>
    /// The cancellation token source.
    /// </value>
    public CancellationTokenSource? CancellationTokenSource { get; }

    /// <summary>Gets a value indicating whether this instance is cancellation requested.</summary>
    /// <value>
    ///   <c>true</c> if this instance is cancellation requested; otherwise, <c>false</c>.
    /// </value>
    public bool IsCancellationRequested => CancellationTokenSource?.IsCancellationRequested == true;

    /// <summary>Gets a value indicating whether gets a value that indicates whether the object is disposed.</summary>
    public bool IsDisposed => _cleanUp.IsDisposed;

    /// <summary>Gets the operation canceled.</summary>
    /// <param name="observer">The observer.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetOperationCanceled(IObserver<Exception> observer) =>
        CancellationTokenSource?.Token
            .UnsafeRegister(static o => ((IObserver<Exception>)o!).OnNext(new OperationCanceledException()), observer)
            .DisposeWith(_cleanUp);

    /// <summary>Subscribes the specified observer.</summary>
    /// <param name="observer">The observer.</param>
    /// <returns>A Disposable.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        var subscription = ReferenceEquals(_sequencer, Sequencer.Immediate)
            ? Source!.Subscribe(observer)
            : Source!.WitnessOn(_sequencer).Subscribe(observer);

        return subscription.DisposeWith(_cleanUp);
    }

    /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Dispose(true);

    /// <summary>Creates a task-backed signal whose source the supplied factory builds.</summary>
    /// <param name="observableFactory">The observable factory.</param>
    /// <param name="sequencer">The sequencer.</param>
    /// <param name="cancellationTokenSource">The cancellation token source.</param>
    /// <returns>The created signal.</returns>
    /// <remarks>
    /// The factory is handed the signal, so the signal has to be whole before it runs. A factory is
    /// caller-supplied code that may subscribe to, dispose, or stash the signal the moment it receives
    /// it; from a constructor it would be doing that to an object the runtime had not finished building.
    /// </remarks>
    internal static TaskSignal<T> Create(
        Func<ITaskSignal<T>, IObservable<T>> observableFactory,
        ISequencer? sequencer = null,
        CancellationTokenSource? cancellationTokenSource = null)
    {
        ArgumentExceptionHelper.ThrowIfNull(observableFactory);

        TaskSignal<T> signal = new(sequencer, cancellationTokenSource);
        signal.Source = observableFactory(signal);
        return signal;
    }

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    private void Dispose(bool disposing)
    {
        if (_cleanUp.IsDisposed || !disposing)
        {
            return;
        }

        try
        {
            CancellationTokenSource?.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The token source can be disposed by the task completion path.
        }

        _cleanUp.Dispose();
        CancellationTokenSource?.Dispose();
    }
}
