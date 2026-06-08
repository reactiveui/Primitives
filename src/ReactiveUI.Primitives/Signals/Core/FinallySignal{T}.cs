// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>Represents the FinallySignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class FinallySignal<T> : SignalsBase<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Action _finallyAction;

    /// <summary>Initializes a new instance of the <see cref="FinallySignal{T}"/> class.</summary>
    /// <param name="source">The source value.</param>
    /// <param name="finallyAction">The finallyAction value.</param>
    public FinallySignal(IObservable<T> source, Action finallyAction)
        : base(true)
    {
        _source = source;
        _finallyAction = finallyAction;
    }

    /// <summary>Executes the SubscribeCore operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new Finally(this, observer, cancel).Run();

    /// <summary>Represents the Finally class.</summary>
    private sealed class Finally : WitnessBase<T, T>
    {
        /// <summary>Stores state for the signal implementation.</summary>
        private readonly FinallySignal<T> _parent;

        /// <summary>Initializes a new instance of the <see cref="Finally"/> class.</summary>
        /// <param name="parent">The parent value.</param>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Finally(FinallySignal<T> parent, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel) => _parent = parent;

        /// <summary>Executes the Run operation.</summary>
        /// <returns>The result.</returns>
        public MultipleDisposable Run()
        {
            IDisposable subscription;
            try
            {
                subscription = _parent._source.Subscribe(this);
            }
            catch
            {
                _parent._finallyAction();
                throw;
            }

            return new(subscription, new ActionDisposable(() => _parent._finallyAction()));
        }

        /// <summary>Executes the OnNext operation.</summary>
        /// <param name="value">The value.</param>
        public override void OnNext(T value) => Observer.OnNext(value);

        /// <summary>Executes the OnError operation.</summary>
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

        /// <summary>Executes the OnCompleted operation.</summary>
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
