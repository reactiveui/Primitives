// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>A signal backed by a task.</summary>
/// <typeparam name="T">The value type.</typeparam>
public interface ITaskSignal<out T> : IObservable<T>, IsDisposed
{
    /// <summary>Gets the source that cancels the backing task, or <see langword="null"/> when the signal is not cancellable.</summary>
    CancellationTokenSource? CancellationTokenSource { get; }

    /// <summary>Gets a value indicating whether cancellation of the backing task has been requested.</summary>
    bool IsCancellationRequested { get; }

    /// <summary>Gets the sequence the backing task produces, or <see langword="null"/> before the task starts.</summary>
    IObservable<T>? Source { get; }

    /// <summary>Notifies an observer with the exception produced by cancelling the backing task.</summary>
    /// <param name="observer">The observer that receives the cancellation exception.</param>
    void GetOperationCanceled(IObserver<Exception> observer);
}
