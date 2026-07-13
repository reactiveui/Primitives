// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Represents an infinite repetition signal.</summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class LoopSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>Whether subscription must run on the current thread.</summary>
    private readonly bool _currentThreadRequired;

    /// <summary>Initializes a new instance of the <see cref="LoopSignal{T}"/> class.</summary>
    /// <param name="value">The value repeated by the signal.</param>
    public LoopSignal(T value)
    {
        _currentThreadRequired = true;
        Value = value;
    }

    /// <summary>Gets the repeated value.</summary>
    public T Value { get; }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => _currentThreadRequired;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, _currentThreadRequired, SubscribeCore);

    /// <summary>Checks whether a cancellation handle has been disposed.</summary>
    /// <param name="disposable">The cancellation handle.</param>
    /// <returns><see langword="true"/> when the handle is disposed.</returns>
    private static bool IsDisposed(IDisposable disposable) =>
        disposable is IsDisposed state && state.IsDisposed;

    /// <summary>Executes the SubscribeCore operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Allocations",
        "PSH1011",
        Justification =
            "No state-taking recursive Schedule overload exists; needs new scheduling API or a rule exclusion.")]
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
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
}
