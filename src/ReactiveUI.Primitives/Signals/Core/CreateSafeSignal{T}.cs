// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the CreateSafeSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class CreateSafeSignal<T> : SignalsBase<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly Func<IObserver<T>, IDisposable> _subscribe;

    /// <summary>
    /// Initializes a new instance of the <see cref="CreateSafeSignal{T}"/> class.
    /// </summary>
    /// <param name="subscribe">The subscribe value.</param>
    public CreateSafeSignal(Func<IObserver<T>, IDisposable> subscribe)
        : base(true) => _subscribe = subscribe; // fail safe

    /// <summary>
    /// Initializes a new instance of the <see cref="CreateSafeSignal{T}"/> class.
    /// </summary>
    /// <param name="subscribe">The subscribe value.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">The isRequiredSubscribeOnCurrentThread value.</param>
    public CreateSafeSignal(Func<IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
        : base(isRequiredSubscribeOnCurrentThread) => _subscribe = subscribe;

    /// <summary>
    /// Executes the SubscribeCore operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new CreateSafe(observer, cancel);
        return _subscribe(observer) ?? Disposable.Empty;
    }

    /// <summary>
    /// Represents the CreateSafe class.
    /// </summary>
    private sealed class CreateSafe : WitnessBase<T, T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CreateSafe"/> class.
        /// </summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public CreateSafe(IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel)
        {
        }

        /// <summary>
        /// Executes the OnNext operation.
        /// </summary>
        /// <param name="value">The value.</param>
        public override void OnNext(T value)
        {
            try
            {
                Observer.OnNext(value);
            }
            catch
            {
                Dispose(); // safe
                throw;
            }
        }

        /// <summary>
        /// Executes the OnError operation.
        /// </summary>
        /// <param name="error">The error value.</param>
        public override void OnError(Exception error)
        {
            try
            {
                Observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <summary>
        /// Executes the OnCompleted operation.
        /// </summary>
        public override void OnCompleted()
        {
            try
            {
                Observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }
}
