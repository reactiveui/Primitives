// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the SignalsBase class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
internal abstract class SignalsBase<T> : IRequireCurrentThread<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SignalsBase{T}"/> class.
    /// </summary>
    /// <param name="isRequiredSubscribeOnCurrentThread">The isRequiredSubscribeOnCurrentThread value.</param>
    private protected SignalsBase(bool isRequiredSubscribeOnCurrentThread) =>
        IsCurrentThreadSubscriptionRequired = isRequiredSubscribeOnCurrentThread;

    /// <summary>
    /// Gets a value indicating whether subscription must be dispatched through the current-thread sequencer.
    /// </summary>
    public bool IsCurrentThreadSubscriptionRequired { get; }

    /// <summary>
    /// Executes the IsRequiredSubscribeOnCurrentThread operation.
    /// </summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => IsCurrentThreadSubscriptionRequired;

    /// <summary>
    /// Executes the Subscribe operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public virtual IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        var subscription = new SingleDisposable();

        if (IsCurrentThreadSubscriptionRequired && Sequencer.CurrentThread.IsScheduleRequired)
        {
            Sequencer.CurrentThread.Schedule(() => subscription.Create(SubscribeCore(observer, subscription)));
        }
        else
        {
            subscription.Create(SubscribeCore(observer, subscription));
        }

        return subscription;
    }

    /// <summary>
    /// Executes the SubscribeCore operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    protected abstract IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel);
}
