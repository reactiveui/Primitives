// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;

namespace ReactiveUI.Primitives.Reactive.Signals;
#else
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>A task-backed signal of values.</summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class TaskSignal<T> : ITaskSignal<T>
{
    /// <summary>The sequencer subscriptions are observed on.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>The subscriptions and cancellation registrations released on disposal.</summary>
    private readonly MultipleDisposable _cleanUp = [];

    /// <summary>Initializes a new instance of the <see cref="TaskSignal{T}" /> class.</summary>
    /// <param name="sequencer">The sequencer.</param>
    /// <param name="cancellationTokenSource">The cancellation token source, or <see langword="null"/> to own a new one.</param>
    private TaskSignal(ISequencer? sequencer, CancellationTokenSource? cancellationTokenSource)
    {
        CancellationTokenSource = cancellationTokenSource ?? new();
        _sequencer = sequencer ?? CurrentThreadSequencer.Instance;
    }

    /// <summary>Gets or sets the sequence subscribers are forwarded to.</summary>
    public IObservable<T>? Source { get; set; }

    /// <summary>Gets the cancellation source that cancels the backing task.</summary>
    public CancellationTokenSource CancellationTokenSource { get; }

    /// <summary>Gets a value indicating whether cancellation has been requested.</summary>
    public bool IsCancellationRequested => CancellationTokenSource.IsCancellationRequested;

    /// <summary>Gets a value indicating whether the signal has been disposed.</summary>
    public bool IsDisposed => _cleanUp.IsDisposed;

    /// <summary>Pushes an <see cref="OperationCanceledException"/> to the observer when cancellation is requested.</summary>
    /// <param name="observer">The observer notified on cancellation.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetOperationCanceled(IObserver<Exception> observer) =>
        CancellationTokenSource.Token
            .UnsafeRegister(static o => ((IObserver<Exception>)o!).OnNext(new OperationCanceledException()), observer)
            .DisposeWith(_cleanUp);

    /// <summary>Subscribes the observer to the source, observing on the sequencer unless it is the immediate one.</summary>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>A disposable that ends the subscription.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        var subscription = _sequencer.IsImmediate
            ? Source!.Subscribe(observer)
            : new WitnessOnSignal<T>(Source!, _sequencer).Subscribe(observer);

        return subscription.DisposeWith(_cleanUp);
    }

    /// <summary>Cancels the backing task and releases the subscriptions.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Dispose(true);

    /// <summary>Invokes the factory after the signal is fully initialized.</summary>
    /// <param name="observableFactory">Builds the source, receiving the signal it will belong to.</param>
    /// <param name="sequencer">The sequencer subscriptions are observed on.</param>
    /// <param name="cancellationTokenSource">The cancellation source to observe.</param>
    /// <returns>The created signal.</returns>
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

    /// <summary>Cancels the token source and releases the subscriptions when disposing.</summary>
    /// <param name="disposing"><c>true</c> to release managed resources; otherwise, <c>false</c>.</param>
    private void Dispose(bool disposing)
    {
        if (_cleanUp.IsDisposed || !disposing)
        {
            return;
        }

        try
        {
            CancellationTokenSource.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Cancellation remains harmless after task completion.
        }

        _cleanUp.Dispose();
        CancellationTokenSource.Dispose();
    }
}
