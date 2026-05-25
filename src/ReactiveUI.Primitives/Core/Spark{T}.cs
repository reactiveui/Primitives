// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Core
{
    /// <summary>
    /// Represents a spark to an observer.
    /// </summary>
    /// <typeparam name="T">The type of the elements received by the observer.</typeparam>
    [Serializable]
#pragma warning disable CS0659 // Type overrides Object.Equals(object o) but does not override Object.GetHashCode()
#pragma warning disable CS0661 // Type defines operator == or operator != but does not override Object.GetHashCode()
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    public abstract class Spark<T> : IEquatable<Spark<T>>
#pragma warning restore CS0661 // Type defines operator == or operator != but does not override Object.GetHashCode()
#pragma warning restore CS0659 // Type overrides Object.Equals(object o) but does not override Object.GetHashCode()
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Spark{T}"/> class.
        /// Default constructor used by derived types.
        /// </summary>
        protected internal Spark()
        {
        }

        /// <summary>
        /// Gets the value of an OnNext spark or throws an exception.
        /// </summary>
        public abstract T Value { get; }

        /// <summary>
        /// Gets a value indicating whether returns a value that indicates whether the spark has a value.
        /// </summary>
        public abstract bool HasValue { get; }

        /// <summary>
        /// Gets the exception of an OnError spark or returns null.
        /// </summary>
        public abstract Exception Exception { get; }

        /// <summary>
        /// Gets the kind of Spark that is represented.
        /// </summary>
        public abstract SparkKind Kind { get; }

        /// <summary>
        /// Determines whether the two specified Spark&lt;T&gt; objects have a different observer message payload.
        /// </summary>
        /// <param name="left">The first Spark&lt;T&gt; to compare, or null.</param>
        /// <param name="right">The second Spark&lt;T&gt; to compare, or null.</param>
        /// <returns>true if the first Spark&lt;T&gt; value has a different observer message payload as the second Spark&lt;T&gt; value; otherwise, false.</returns>
        /// <remarks>
        /// Equality of Spark&lt;T&gt; objects is based on the equality of the observer message payload they represent, including the Spark Kind and the Value or Exception (if any).
        /// This means two Spark&lt;T&gt; objects can be equal even though they don't represent the same observer method call, but have the same Kind and have equal parameters passed to the observer method.
        /// In case one wants to determine whether two Spark&lt;T&gt; objects represent a different observer method call, use Object.ReferenceEquals identity equality instead.
        /// </remarks>
        public static bool operator !=(Spark<T> left, Spark<T> right) => !(left == right);

        /// <summary>
        /// Determines whether the two specified Spark&lt;T&gt; objects have the same observer message payload.
        /// </summary>
        /// <param name="left">The first Spark&lt;T&gt; to compare, or null.</param>
        /// <param name="right">The second Spark&lt;T&gt; to compare, or null.</param>
        /// <returns>true if the first Spark&lt;T&gt; value has the same observer message payload as the second Spark&lt;T&gt; value; otherwise, false.</returns>
        /// <remarks>
        /// Equality of Spark&lt;T&gt; objects is based on the equality of the observer message payload they represent, including the Spark Kind and the Value or Exception (if any).
        /// This means two Spark&lt;T&gt; objects can be equal even though they don't represent the same observer method call, but have the same Kind and have equal parameters passed to the observer method.
        /// In case one wants to determine whether two Spark&lt;T&gt; objects represent a different observer method call, use Object.ReferenceEquals identity equality instead.
        /// </remarks>
        public static bool operator ==(Spark<T>? left, Spark<T>? right) =>
            ReferenceEquals(left, right) || (left is not null && left.Equals(right));

        /// <summary>
        /// Determines whether the current Spark&lt;T&gt; object has the same observer message payload as a specified Spark&lt;T&gt; value.
        /// </summary>
        /// <param name="other">An object to compare to the current Spark&lt;T&gt; object.</param>
        /// <returns>true if both Spark&lt;T&gt; objects have the same observer message payload; otherwise, false.</returns>
        /// <remarks>
        /// Equality of Spark&lt;T&gt; objects is based on the equality of the observer message payload they represent, including the Spark Kind and the Value or Exception (if any).
        /// This means two Spark&lt;T&gt; objects can be equal even though they don't represent the same observer method call, but have the same Kind and have equal parameters passed to the observer method.
        /// In case one wants to determine whether two Spark&lt;T&gt; objects represent the same observer method call, use Object.ReferenceEquals identity equality instead.
        /// </remarks>
        public abstract bool Equals(Spark<T>? other);

        /// <summary>
        /// Determines whether the specified System.Object is equal to the current Spark&lt;T&gt;.
        /// </summary>
        /// <param name="obj">The System.Object to compare with the current Spark&lt;T&gt;.</param>
        /// <returns>true if the specified System.Object is equal to the current Spark&lt;T&gt;; otherwise, false.</returns>
        /// <remarks>
        /// Equality of Spark&lt;T&gt; objects is based on the equality of the observer message payload they represent, including the Spark Kind and the Value or Exception (if any).
        /// This means two Spark&lt;T&gt; objects can be equal even though they don't represent the same observer method call, but have the same Kind and have equal parameters passed to the observer method.
        /// In case one wants to determine whether two Spark&lt;T&gt; objects represent the same observer method call, use Object.ReferenceEquals identity equality instead.
        /// </remarks>
        public override bool Equals(object? obj) => Equals(obj as Spark<T>);

        /// <summary>
        /// Invokes the observer's method corresponding to the Spark.
        /// </summary>
        /// <param name="observer">Observer to invoke the Spark on.</param>
        public abstract void Accept(IObserver<T> observer);

        /// <summary>
        /// Invokes the observer's method corresponding to the Spark and returns the produced result.
        /// </summary>
        /// <typeparam name="TResult">The type of the result returned from the observer's Spark handlers.</typeparam>
        /// <param name="observer">Observer to invoke the Spark on.</param>
        /// <returns>Result produced by the observation.</returns>
        public abstract TResult Accept<TResult>(IObserver<T, TResult> observer);

        /// <summary>
        /// Invokes the delegate corresponding to the Spark.
        /// </summary>
        /// <param name="onNext">Delegate to invoke for an OnNext Spark.</param>
        /// <param name="onError">Delegate to invoke for an OnError Spark.</param>
        /// <param name="onCompleted">Delegate to invoke for an OnCompleted Spark.</param>
        public abstract void Accept(Action<T> onNext, Action<Exception> onError, Action onCompleted);

        /// <summary>
        /// Invokes the delegate corresponding to the Spark and returns the produced result.
        /// </summary>
        /// <typeparam name="TResult">The type of the result returned from the Spark handler delegates.</typeparam>
        /// <param name="onNext">Delegate to invoke for an OnNext Spark.</param>
        /// <param name="onError">Delegate to invoke for an OnError Spark.</param>
        /// <param name="onCompleted">Delegate to invoke for an OnCompleted Spark.</param>
        /// <returns>Result produced by the observation.</returns>
        public abstract TResult Accept<TResult>(Func<T, TResult> onNext, Func<Exception, TResult> onError, Func<TResult> onCompleted);

        /// <summary>
        /// Returns an observable sequence with a single Spark, using the immediate scheduler.
        /// </summary>
        /// <returns>The observable sequence that surfaces the behavior of the Spark upon subscription.</returns>
        public IObservable<T> ToObservable() => ToObservable(Scheduler.Immediate);

        /// <summary>
        /// Returns an observable sequence with a single Spark.
        /// </summary>
        /// <param name="scheduler">Scheduler to send out the Spark calls on.</param>
        /// <returns>The observable sequence that surfaces the behavior of the Spark upon subscription.</returns>
        public IObservable<T> ToObservable(ISequencer scheduler)
        {
            if (scheduler == null)
            {
                throw new ArgumentNullException(nameof(scheduler));
            }

            return Signal.Create<T>(observer => scheduler.Schedule(() =>
            {
                Accept(observer);
                if (Kind == SparkKind.OnNext)
                {
                    observer.OnCompleted();
                }
            }));
        }

        /// <summary>
        /// Represents an OnNext spark to an observer.
        /// </summary>
        [DebuggerDisplay("OnNext({Value})")]
        [Serializable]
        internal sealed class OnNextSpark : Spark<T>
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="OnNextSpark"/> class.
            /// Constructs a Spark of a new value.
            /// </summary>
            public OnNextSpark(T value) => Value = value;

            /// <summary>
            /// Gets the value of an OnNext Spark.
            /// </summary>
            public override T Value { get; }

            /// <summary>
            /// Gets null.
            /// </summary>
            public override Exception Exception => null!;

            /// <summary>
            /// Gets a value indicating whether returns true.
            /// </summary>
            public override bool HasValue => true;

            /// <summary>
            /// Gets SparkKind.OnNext.
            /// </summary>
            public override SparkKind Kind => SparkKind.OnNext;

            /// <summary>
            /// Returns the hash code for this instance.
            /// </summary>
            public override int GetHashCode() => EqualityComparer<T>.Default.GetHashCode(Value!);

            /// <summary>
            /// Indicates whether this instance and a specified object are equal.
            /// </summary>
            /// <param name="other">The other.</param>
            /// <returns>A bool.</returns>
            public override bool Equals(Spark<T>? other)
            {
                if (ReferenceEquals(this, other))
                {
                    return true;
                }

                if (other is null)
                {
                    return false;
                }

                if (other.Kind != SparkKind.OnNext)
                {
                    return false;
                }

                return EqualityComparer<T>.Default.Equals(Value, other.Value);
            }

            /// <summary>
            /// Returns a string representation of this instance.
            /// </summary>
            public override string ToString() => string.Format(CultureInfo.CurrentCulture, "OnNext({0})", Value);

            /// <summary>
            /// Invokes the observer's method corresponding to the Spark.
            /// </summary>
            /// <param name="observer">Observer to invoke the Spark on.</param>
            public override void Accept(IObserver<T> observer)
            {
                if (observer == null)
                {
                    throw new ArgumentNullException(nameof(observer));
                }

                observer.OnNext(Value);
            }

            /// <summary>
            /// Invokes the observer's method corresponding to the Spark and returns the produced result.
            /// </summary>
            /// <param name="observer">Observer to invoke the Spark on.</param>
            /// <returns>Result produced by the observation.</returns>
            public override TResult Accept<TResult>(IObserver<T, TResult> observer)
            {
                if (observer == null)
                {
                    throw new ArgumentNullException(nameof(observer));
                }

                return observer.OnNext(Value);
            }

            /// <summary>
            /// Invokes the delegate corresponding to the Spark.
            /// </summary>
            /// <param name="onNext">Delegate to invoke for an OnNext Spark.</param>
            /// <param name="onError">Delegate to invoke for an OnError Spark.</param>
            /// <param name="onCompleted">Delegate to invoke for an OnCompleted Spark.</param>
            public override void Accept(Action<T> onNext, Action<Exception> onError, Action onCompleted)
            {
                if (onNext == null)
                {
                    throw new ArgumentNullException(nameof(onNext));
                }

                if (onError == null)
                {
                    throw new ArgumentNullException(nameof(onError));
                }

                if (onCompleted == null)
                {
                    throw new ArgumentNullException(nameof(onCompleted));
                }

                onNext(Value);
            }

            /// <summary>
            /// Invokes the delegate corresponding to the Spark and returns the produced result.
            /// </summary>
            /// <param name="onNext">Delegate to invoke for an OnNext Spark.</param>
            /// <param name="onError">Delegate to invoke for an OnError Spark.</param>
            /// <param name="onCompleted">Delegate to invoke for an OnCompleted Spark.</param>
            /// <returns>Result produced by the observation.</returns>
            public override TResult Accept<TResult>(Func<T, TResult> onNext, Func<Exception, TResult> onError, Func<TResult> onCompleted)
            {
                if (onNext == null)
                {
                    throw new ArgumentNullException(nameof(onNext));
                }

                if (onError == null)
                {
                    throw new ArgumentNullException(nameof(onError));
                }

                if (onCompleted == null)
                {
                    throw new ArgumentNullException(nameof(onCompleted));
                }

                return onNext(Value);
            }
        }

        /// <summary>
        /// Represents an OnError Spark to an observer.
        /// </summary>
        [DebuggerDisplay("OnError({Exception})")]
        [Serializable]
        internal sealed class OnErrorSpark : Spark<T>
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="OnErrorSpark"/> class.
            /// Constructs a Spark of an exception.
            /// </summary>
            public OnErrorSpark(Exception exception) => Exception = exception;

            /// <summary>
            /// Gets throws the exception.
            /// </summary>
            public override T Value
            {
                get
                {
                    Exception.Throw();
                    throw Exception;
                }
            }

            /// <summary>
            /// Gets the exception.
            /// </summary>
            public override Exception Exception { get; }

            /// <summary>
            /// Gets a value indicating whether returns false.
            /// </summary>
            public override bool HasValue => false;

            /// <summary>
            /// Gets SparkKind.OnError.
            /// </summary>
            public override SparkKind Kind => SparkKind.OnError;

            /// <summary>
            /// Returns the hash code for this instance.
            /// </summary>
            public override int GetHashCode() => Exception.GetHashCode();

            /// <summary>
            /// Indicates whether this instance and other are equal.
            /// </summary>
            public override bool Equals(Spark<T>? other)
            {
                if (ReferenceEquals(this, other))
                {
                    return true;
                }

                if (other is null)
                {
                    return false;
                }

                if (other.Kind != SparkKind.OnError)
                {
                    return false;
                }

                return Equals(Exception, other.Exception);
            }

            /// <summary>
            /// Returns a string representation of this instance.
            /// </summary>
            public override string ToString() => string.Format(CultureInfo.CurrentCulture, "OnError({0})", Exception.GetType().FullName);

            /// <summary>
            /// Invokes the observer's method corresponding to the Spark.
            /// </summary>
            /// <param name="observer">Observer to invoke the Spark on.</param>
            public override void Accept(IObserver<T> observer)
            {
                if (observer == null)
                {
                    throw new ArgumentNullException(nameof(observer));
                }

                observer.OnError(Exception);
            }

            /// <summary>
            /// Invokes the observer's method corresponding to the Spark and returns the produced result.
            /// </summary>
            /// <param name="observer">Observer to invoke the Spark on.</param>
            /// <returns>Result produced by the observation.</returns>
            public override TResult Accept<TResult>(IObserver<T, TResult> observer)
            {
                if (observer == null)
                {
                    throw new ArgumentNullException(nameof(observer));
                }

                return observer.OnError(Exception);
            }

            /// <summary>
            /// Invokes the delegate corresponding to the Spark.
            /// </summary>
            /// <param name="onNext">Delegate to invoke for an OnNext Spark.</param>
            /// <param name="onError">Delegate to invoke for an OnError Spark.</param>
            /// <param name="onCompleted">Delegate to invoke for an OnCompleted Spark.</param>
            public override void Accept(Action<T> onNext, Action<Exception> onError, Action onCompleted)
            {
                if (onNext == null)
                {
                    throw new ArgumentNullException(nameof(onNext));
                }

                if (onError == null)
                {
                    throw new ArgumentNullException(nameof(onError));
                }

                if (onCompleted == null)
                {
                    throw new ArgumentNullException(nameof(onCompleted));
                }

                onError(Exception);
            }

            /// <summary>
            /// Invokes the delegate corresponding to the Spark and returns the produced result.
            /// </summary>
            /// <param name="onNext">Delegate to invoke for an OnNext Spark.</param>
            /// <param name="onError">Delegate to invoke for an OnError Spark.</param>
            /// <param name="onCompleted">Delegate to invoke for an OnCompleted Spark.</param>
            /// <returns>Result produced by the observation.</returns>
            public override TResult Accept<TResult>(Func<T, TResult> onNext, Func<Exception, TResult> onError, Func<TResult> onCompleted)
            {
                if (onNext == null)
                {
                    throw new ArgumentNullException(nameof(onNext));
                }

                if (onError == null)
                {
                    throw new ArgumentNullException(nameof(onError));
                }

                if (onCompleted == null)
                {
                    throw new ArgumentNullException(nameof(onCompleted));
                }

                return onError(Exception);
            }
        }

        /// <summary>
        /// Represents an OnCompleted spark to an observer.
        /// </summary>
        [DebuggerDisplay("OnCompleted()")]
        [Serializable]
        internal sealed class OnCompletedSpark : Spark<T>
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="OnCompletedSpark"/> class.
            /// Constructs a Spark of the end of a sequence.
            /// </summary>
            public OnCompletedSpark()
            {
            }

            /// <summary>
            /// Gets throws an InvalidOperationException.
            /// </summary>
            public override T Value => throw new InvalidOperationException("No Value");

            /// <summary>
            /// Gets null.
            /// </summary>
            public override Exception Exception => null!;

            /// <summary>
            /// Gets a value indicating whether returns false.
            /// </summary>
            public override bool HasValue => false;

            /// <summary>
            /// Gets SparkKind.OnCompleted.
            /// </summary>
            public override SparkKind Kind => SparkKind.OnCompleted;

            /// <summary>
            /// Returns the hash code for this instance.
            /// </summary>
            public override int GetHashCode() => typeof(T).GetHashCode() ^ 8510;

            /// <summary>
            /// Indicates whether this instance and other are equal.
            /// </summary>
            public override bool Equals(Spark<T>? other)
            {
                if (ReferenceEquals(this, other))
                {
                    return true;
                }

                if (other is null)
                {
                    return false;
                }

                return other.Kind == SparkKind.OnCompleted;
            }

            /// <summary>
            /// Returns a string representation of this instance.
            /// </summary>
            public override string ToString() => "OnCompleted()";

            /// <summary>
            /// Invokes the observer's method corresponding to the Spark.
            /// </summary>
            /// <param name="observer">Observer to invoke the Spark on.</param>
            public override void Accept(IObserver<T> observer)
            {
                if (observer == null)
                {
                    throw new ArgumentNullException(nameof(observer));
                }

                observer.OnCompleted();
            }

            /// <summary>
            /// Invokes the observer's method corresponding to the Spark and returns the produced result.
            /// </summary>
            /// <param name="observer">Observer to invoke the Spark on.</param>
            /// <returns>Result produced by the observation.</returns>
            public override TResult Accept<TResult>(IObserver<T, TResult> observer)
            {
                if (observer == null)
                {
                    throw new ArgumentNullException(nameof(observer));
                }

                return observer.OnCompleted();
            }

            /// <summary>
            /// Invokes the delegate corresponding to the Spark.
            /// </summary>
            /// <param name="onNext">Delegate to invoke for an OnNext Spark.</param>
            /// <param name="onError">Delegate to invoke for an OnError Spark.</param>
            /// <param name="onCompleted">Delegate to invoke for an OnCompleted Spark.</param>
            public override void Accept(Action<T> onNext, Action<Exception> onError, Action onCompleted)
            {
                if (onNext == null)
                {
                    throw new ArgumentNullException(nameof(onNext));
                }

                if (onError == null)
                {
                    throw new ArgumentNullException(nameof(onError));
                }

                if (onCompleted == null)
                {
                    throw new ArgumentNullException(nameof(onCompleted));
                }

                onCompleted();
            }

            /// <summary>
            /// Invokes the delegate corresponding to the Spark and returns the produced result.
            /// </summary>
            /// <param name="onNext">Delegate to invoke for an OnNext Spark.</param>
            /// <param name="onError">Delegate to invoke for an OnError Spark.</param>
            /// <param name="onCompleted">Delegate to invoke for an OnCompleted Spark.</param>
            /// <returns>Result produced by the observation.</returns>
            public override TResult Accept<TResult>(Func<T, TResult> onNext, Func<Exception, TResult> onError, Func<TResult> onCompleted)
            {
                if (onNext == null)
                {
                    throw new ArgumentNullException(nameof(onNext));
                }

                if (onError == null)
                {
                    throw new ArgumentNullException(nameof(onError));
                }

                if (onCompleted == null)
                {
                    throw new ArgumentNullException(nameof(onCompleted));
                }

                return onCompleted();
            }
        }
    }
}
