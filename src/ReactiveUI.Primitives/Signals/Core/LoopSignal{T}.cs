// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>Represents an infinite repetition signal.</summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class LoopSignal<T> : SignalsBase<T>
{
    /// <summary>Initializes a new instance of the <see cref="LoopSignal{T}"/> class.</summary>
    /// <param name="value">The value repeated by the signal.</param>
    public LoopSignal(T value)
        : base(true) =>
        Value = value;

    /// <summary>Gets the repeated value.</summary>
    public T Value { get; }

    /// <inheritdoc/>
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        Sequencer.CurrentThread.Schedule(self =>
        {
            if (IsDisposed(cancel))
            {
                return;
            }

            observer.OnNext(Value);
            if (IsDisposed(cancel))
            {
                return;
            }

            self();
        });

    /// <summary>Checks whether a cancellation handle has been disposed.</summary>
    /// <param name="disposable">The cancellation handle.</param>
    /// <returns><see langword="true"/> when the handle is disposed.</returns>
    private static bool IsDisposed(IDisposable disposable) =>
        disposable is IsDisposed state && state.IsDisposed;
}
