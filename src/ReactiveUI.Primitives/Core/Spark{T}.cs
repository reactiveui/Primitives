// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Core;

/// <summary>
/// Represents a spark to an observer. This is a by-value type: materializing a sequence allocates
/// no per-notification heap object, mirroring the value-type notification used by other modern
/// reactive libraries.
/// </summary>
/// <typeparam name="T">The type of the elements received by the observer.</typeparam>
[Serializable]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public readonly struct Spark<T> : IEquatable<Spark<T>>
{
    /// <summary>
    /// The carried exception for an OnError spark; otherwise <see langword="null"/>.
    /// </summary>
    private readonly Exception? _exception;

    /// <summary>
    /// Initializes a new instance of the <see cref="Spark{T}"/> struct.
    /// </summary>
    /// <param name="kind">The kind of spark.</param>
    /// <param name="value">The carried value, when the kind is OnNext.</param>
    /// <param name="exception">The carried exception, when the kind is OnError.</param>
    private Spark(SparkKind kind, T value, Exception? exception)
    {
        Kind = kind;
        Value = value;
        _exception = exception;
    }

    /// <summary>
    /// Gets the value carried by an OnNext spark, or the default value for OnError and OnCompleted
    /// sparks. Check <see cref="HasValue"/> (or <see cref="Kind"/>) to determine whether the value is
    /// meaningful, and read <see cref="Exception"/> for the error carried by an OnError spark.
    /// </summary>
    public T Value { get; }

    /// <summary>
    /// Gets a value indicating whether the spark carries a value.
    /// </summary>
    public bool HasValue => Kind == SparkKind.OnNext;

    /// <summary>
    /// Gets the exception of an OnError spark or returns null.
    /// </summary>
    public Exception Exception => _exception!;

    /// <summary>
    /// Gets the kind of Spark that is represented.
    /// </summary>
    public SparkKind Kind { get; }

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>
    /// Determines whether the two specified Spark&lt;T&gt; objects have a different observer message payload.
    /// </summary>
    /// <param name="left">The first Spark&lt;T&gt; to compare.</param>
    /// <param name="right">The second Spark&lt;T&gt; to compare.</param>
    /// <returns>
    /// <see langword="true"/> if the first Spark&lt;T&gt; value has a different observer message payload as the second Spark&lt;T&gt; value;
    /// otherwise, <see langword="false"/>.
    /// </returns>
    public static bool operator !=(Spark<T> left, Spark<T> right) => !left.Equals(right);

    /// <summary>
    /// Determines whether the two specified Spark&lt;T&gt; objects have the same observer message payload.
    /// </summary>
    /// <param name="left">The first Spark&lt;T&gt; to compare.</param>
    /// <param name="right">The second Spark&lt;T&gt; to compare.</param>
    /// <returns>
    /// <see langword="true"/> if the first Spark&lt;T&gt; value has the same observer message payload as the second Spark&lt;T&gt; value;
    /// otherwise, <see langword="false"/>.
    /// </returns>
    public static bool operator ==(Spark<T> left, Spark<T> right) => left.Equals(right);

    /// <summary>
    /// Determines whether the current Spark&lt;T&gt; object has the same observer message payload as a specified Spark&lt;T&gt; value.
    /// </summary>
    /// <param name="other">An object to compare to the current Spark&lt;T&gt; object.</param>
    /// <returns>true if both Spark&lt;T&gt; objects have the same observer message payload; otherwise, false.</returns>
    /// <remarks>
    /// Equality of Spark&lt;T&gt; objects is based on the equality of the observer message payload they represent,
    /// including the Spark Kind and the Value or Exception (if any).
    /// </remarks>
    public bool Equals(Spark<T> other)
    {
        if (Kind != other.Kind)
        {
            return false;
        }

        return Kind switch
        {
            SparkKind.OnNext => EqualityComparer<T>.Default.Equals(Value, other.Value),
            SparkKind.OnError => Equals(_exception, other._exception),
            _ => true,
        };
    }

    /// <summary>
    /// Determines whether the specified System.Object is equal to the current Spark&lt;T&gt;.
    /// </summary>
    /// <param name="obj">The System.Object to compare with the current Spark&lt;T&gt;.</param>
    /// <returns>true if the specified System.Object is equal to the current Spark&lt;T&gt;; otherwise, false.</returns>
    public override bool Equals(object? obj) => obj is Spark<T> other && Equals(other);

    /// <summary>
    /// Returns the hash code for this spark.
    /// </summary>
    /// <returns>A hash code for this spark.</returns>
    public override int GetHashCode() => Kind switch
    {
        SparkKind.OnNext => EqualityComparer<T>.Default.GetHashCode(Value!),
        SparkKind.OnError => _exception!.GetHashCode(),
        _ => typeof(T).GetHashCode() ^ 8510,
    };

    /// <summary>
    /// Returns a string representation of this spark.
    /// </summary>
    /// <returns>A string representation of this spark.</returns>
    public override string ToString() => Kind switch
    {
        SparkKind.OnNext => string.Format(CultureInfo.CurrentCulture, "OnNext({0})", Value),
        SparkKind.OnError => string.Format(CultureInfo.CurrentCulture, "OnError({0})", _exception!.GetType().FullName),
        _ => "OnCompleted()",
    };

    /// <summary>
    /// Invokes the observer's method corresponding to the Spark.
    /// </summary>
    /// <param name="observer">Observer to invoke the Spark on.</param>
    public void Accept(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        if (Kind == SparkKind.OnNext)
        {
            observer.OnNext(Value);
        }
        else if (Kind == SparkKind.OnError)
        {
            observer.OnError(_exception!);
        }
        else
        {
            observer.OnCompleted();
        }
    }

    /// <summary>
    /// Invokes the observer's method corresponding to the Spark and returns the produced result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned from the observer's Spark handlers.</typeparam>
    /// <param name="observer">Observer to invoke the Spark on.</param>
    /// <returns>Result produced by the observation.</returns>
    public TResult Accept<TResult>(IObserver<T, TResult> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        return Kind switch
        {
            SparkKind.OnNext => observer.OnNext(Value),
            SparkKind.OnError => observer.OnError(_exception!),
            _ => observer.OnCompleted(),
        };
    }

    /// <summary>
    /// Invokes the delegate corresponding to the Spark.
    /// </summary>
    /// <param name="onNext">Delegate to invoke for an OnNext Spark.</param>
    /// <param name="onError">Delegate to invoke for an OnError Spark.</param>
    /// <param name="onCompleted">Delegate to invoke for an OnCompleted Spark.</param>
    public void Accept(Action<T> onNext, Action<Exception> onError, Action onCompleted)
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

        if (Kind == SparkKind.OnNext)
        {
            onNext(Value);
        }
        else if (Kind == SparkKind.OnError)
        {
            onError(_exception!);
        }
        else
        {
            onCompleted();
        }
    }

    /// <summary>
    /// Invokes the delegate corresponding to the Spark and returns the produced result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned from the Spark handler delegates.</typeparam>
    /// <param name="onNext">Delegate to invoke for an OnNext Spark.</param>
    /// <param name="onError">Delegate to invoke for an OnError Spark.</param>
    /// <param name="onCompleted">Delegate to invoke for an OnCompleted Spark.</param>
    /// <returns>Result produced by the observation.</returns>
    public TResult Accept<TResult>(Func<T, TResult> onNext, Func<Exception, TResult> onError, Func<TResult> onCompleted)
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

        return Kind switch
        {
            SparkKind.OnNext => onNext(Value),
            SparkKind.OnError => onError(_exception!),
            _ => onCompleted(),
        };
    }

    /// <summary>
    /// Returns an observable sequence with a single Spark, using the immediate scheduler.
    /// </summary>
    /// <returns>The observable sequence that surfaces the behavior of the Spark upon subscription.</returns>
    public IObservable<T> ToObservable() => ToObservable(Sequencer.Immediate);

    /// <summary>
    /// Returns an observable sequence with a single Spark.
    /// </summary>
    /// <param name="scheduler">Sequencer to send out the Spark calls on.</param>
    /// <returns>The observable sequence that surfaces the behavior of the Spark upon subscription.</returns>
    public IObservable<T> ToObservable(ISequencer scheduler)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        var self = this;
        return Signal.Create<T>(observer => scheduler.Schedule(() =>
        {
            self.Accept(observer);
            if (self.Kind != SparkKind.OnNext)
            {
                return;
            }

            observer.OnCompleted();
        }));
    }

    /// <summary>
    /// Creates an OnNext spark carrying the supplied value.
    /// </summary>
    /// <param name="value">The value carried by the spark.</param>
    /// <returns>The OnNext spark.</returns>
    internal static Spark<T> OnNext(T value) => new(SparkKind.OnNext, value, null);

    /// <summary>
    /// Creates an OnError spark carrying the supplied exception.
    /// </summary>
    /// <param name="exception">The exception carried by the spark.</param>
    /// <returns>The OnError spark.</returns>
    internal static Spark<T> OnError(Exception exception) => new(SparkKind.OnError, default!, exception);

    /// <summary>
    /// Creates an OnCompleted spark.
    /// </summary>
    /// <returns>The OnCompleted spark.</returns>
    internal static Spark<T> OnCompleted() => new(SparkKind.OnCompleted, default!, null);
}