// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Signals.Core
{
    /// <summary>
    /// Represents the WitnessBase class.
    /// </summary>
    /// <typeparam name="TSource">The TSource type.</typeparam>
    /// <typeparam name="TResult">The TResult type.</typeparam>
    internal abstract class WitnessBase<TSource, TResult> : IDisposable, IObserver<TSource>
    {
#pragma warning disable SA1401 // Fields should be private

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        protected internal volatile IObserver<TResult> Observer;
#pragma warning restore SA1401 // Fields should be private

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private IDisposable? _cancel;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="WitnessBase{TSource,TResult}"/> class.
        /// </summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        private protected WitnessBase(IObserver<TResult> observer, IDisposable cancel)
        {
            _cancel = cancel ?? throw new ArgumentNullException(nameof(cancel));
            Observer = observer;
        }

        /// <summary>
        /// Executes the OnNext operation.
        /// </summary>
        /// <param name="value">The value.</param>
        public abstract void OnNext(TSource value);

        /// <summary>
        /// Executes the OnError operation.
        /// </summary>
        /// <param name="error">The error value.</param>
        public abstract void OnError(Exception error);

        /// <summary>
        /// Executes the OnCompleted operation.
        /// </summary>
        public abstract void OnCompleted();

        /// <summary>
        /// Executes the Dispose operation.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Executes the Dispose operation.
        /// </summary>
        /// <param name="disposing">The disposing value.</param>
        protected virtual void Dispose(bool disposing)
        {
            Observer = EmptyWitness<TResult>.Instance;
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                var target = Interlocked.Exchange(ref _cancel, null);
                target?.Dispose();
            }

            _disposed = true;
        }
    }
}
