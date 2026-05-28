// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives;

/// <summary>
/// Private helper types for boolean terminal parity operators.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Predicate all operator implemented without delegate observer wrappers.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AllPredicateSignal<T> : IRequireCurrentThread<bool>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The predicate.
        /// </summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>
        /// Initializes a new instance of the <see cref="AllPredicateSignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="predicate">The predicate.</param>
        internal AllPredicateSignal(IObservable<T> source, Func<T, bool> predicate)
        {
            _source = source;
            _predicate = predicate;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<bool> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            if (_source is RangeSignal range && typeof(T) == typeof(int))
            {
                EmitAllRange(range, _predicate, observer);
                return Disposable.Empty;
            }

            var sink = new AllPredicateObserver<T>(observer, _predicate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <summary>
        /// Evaluates a predicate directly over a range source and emits the all result.
        /// </summary>
        /// <param name="range">The range source.</param>
        /// <param name="predicate">The predicate.</param>
        /// <param name="observer">The downstream observer.</param>
        private static void EmitAllRange(RangeSignal range, Func<T, bool> predicate, IObserver<bool> observer)
        {
            try
            {
                var typedPredicate = (Func<int, bool>)(object)predicate;
                for (var i = 0; i < range.Count; i++)
                {
                    if (typedPredicate(range.Start + i))
                    {
                        continue;
                    }

                    observer.OnNext(false);
                    observer.OnCompleted();
                    return;
                }

                observer.OnNext(true);
                observer.OnCompleted();
            }
            catch (Exception error)
            {
                observer.OnError(error);
            }
        }
    }

    /// <summary>
    /// Contains operator implemented without composing Any and comparer closures.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class ContainsSignal<T> : IRequireCurrentThread<bool>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The value to locate.
        /// </summary>
        private readonly T _value;

        /// <summary>
        /// The comparer used for equality checks.
        /// </summary>
        private readonly IEqualityComparer<T> _comparer;

        /// <summary>
        /// Initializes a new instance of the <see cref="ContainsSignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="value">The value to locate.</param>
        /// <param name="comparer">The comparer used for equality checks.</param>
        internal ContainsSignal(IObservable<T> source, T value, IEqualityComparer<T> comparer)
        {
            _source = source;
            _value = value;
            _comparer = comparer;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<bool> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            if (_source is RangeSignal range && typeof(T) == typeof(int))
            {
                EmitContainsRange(range, _value, _comparer, observer);
                return Disposable.Empty;
            }

            var sink = new ContainsObserver<T>(observer, _value, _comparer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <summary>
        /// Evaluates contains directly over a range source and emits the result.
        /// </summary>
        /// <param name="range">The range source.</param>
        /// <param name="value">The value to locate.</param>
        /// <param name="comparer">The comparer used for equality checks.</param>
        /// <param name="observer">The downstream observer.</param>
        private static void EmitContainsRange(RangeSignal range, T value, IEqualityComparer<T> comparer, IObserver<bool> observer)
        {
            try
            {
                if (ReferenceEquals(comparer, EqualityComparer<T>.Default))
                {
                    var target = (int)(object)value!;
                    var offset = (long)target - range.Start;
                    observer.OnNext(offset >= 0 && offset < range.Count);
                    observer.OnCompleted();
                    return;
                }

                for (var i = 0; i < range.Count; i++)
                {
                    if (!comparer.Equals((T)(object)(range.Start + i), value))
                    {
                        continue;
                    }

                    observer.OnNext(true);
                    observer.OnCompleted();
                    return;
                }

                observer.OnNext(false);
                observer.OnCompleted();
            }
            catch (Exception error)
            {
                observer.OnError(error);
            }
        }
    }

    /// <summary>
    /// Observer for detecting whether all values match a predicate.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AllPredicateObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<bool> _observer;

        /// <summary>
        /// The predicate.
        /// </summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="AllPredicateObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="predicate">The predicate.</param>
        internal AllPredicateObserver(IObserver<bool> observer, Func<T, bool> predicate)
        {
            _observer = observer;
            _predicate = predicate;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done)
            {
                return;
            }

            bool matches;
            try
            {
                matches = _predicate(value);
            }
            catch (Exception error)
            {
                OnError(error);
                return;
            }

            if (matches)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(false);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(true);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Observer for detecting whether a value is contained in a sequence.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class ContainsObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<bool> _observer;

        /// <summary>
        /// The value to locate.
        /// </summary>
        private readonly T _value;

        /// <summary>
        /// The comparer used for equality checks.
        /// </summary>
        private readonly IEqualityComparer<T> _comparer;

        /// <summary>
        /// A value indicating whether the observer has terminated.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="ContainsObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="value">The value to locate.</param>
        /// <param name="comparer">The comparer used for equality checks.</param>
        internal ContainsObserver(IObserver<bool> observer, T value, IEqualityComparer<T> comparer)
        {
            _observer = observer;
            _value = value;
            _comparer = comparer;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            if (_done)
            {
                return;
            }

            bool matches;
            try
            {
                matches = _comparer.Equals(value, _value);
            }
            catch (Exception error)
            {
                OnError(error);
                return;
            }

            if (!matches)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(true);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            if (_done)
            {
                return;
            }

            _done = true;
            try
            {
                _observer.OnNext(false);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }
}
