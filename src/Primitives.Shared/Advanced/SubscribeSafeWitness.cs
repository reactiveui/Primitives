// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Static factory methods for <see cref="SubscribeSafeWitness{T}"/> across nullable and value-type call shapes.</summary>
public static class SubscribeSafeWitness
{
    /// <summary>Subscribes an observer with downstream exception protection.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(1)]
    public static SubscribeSafeWitness<T> Subscribe<T>(
        IObservable<T> source,
        IObserver<T> observer,
        params bool[] allowNullable)
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T>.Subscribe(source, observer);
    }

    /// <summary>Subscribes a nullable value observer with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(2)]
    public static SubscribeSafeWitness<T?> Subscribe<T>(
        IObservable<T?> source,
        IObserver<T?> observer,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T?>.Subscribe(source, observer);
    }

    /// <summary>Subscribes callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(1)]
    public static SubscribeSafeWitness<T> Subscribe<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        params bool[] allowNullable)
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T>.Subscribe(source, onNext, onError);
    }

    /// <summary>Subscribes nullable value callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(2)]
    public static SubscribeSafeWitness<T?> Subscribe<T>(
        IObservable<T?> source,
        Action<T?> onNext,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T?>.Subscribe(source, Witness.Create(onNext, onError));
    }

    /// <summary>Subscribes callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(1)]
    public static SubscribeSafeWitness<T> Subscribe<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T>.Subscribe(source, onNext, onError, onCompleted);
    }

    /// <summary>Subscribes nullable value callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(2)]
    public static SubscribeSafeWitness<T?> Subscribe<T>(
        IObservable<T?> source,
        Action<T?> onNext,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T?>.Subscribe(source, Witness.Create(onNext, onError, onCompleted));
    }

    /// <summary>Subscribes a value callback with downstream exception protection.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(1)]
    public static SubscribeSafeWitness<T> Subscribe<T>(
        IObservable<T> source,
        Action<T> onNext,
        params bool[] allowNullable)
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T>.Subscribe(source, onNext);
    }

    /// <summary>Subscribes a nullable value callback with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(2)]
    public static SubscribeSafeWitness<T?> Subscribe<T>(
        IObservable<T?> source,
        Action<T?> onNext,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T?>.Subscribe(source, Witness.Create(onNext));
    }

    /// <summary>Subscribes a non-nullable value callback with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(2)]
    public static SubscribeSafeWitness<T> Subscribe<T>(
        IObservable<T> source,
        Action<T> onNext,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeWitness<T>.Subscribe(source, Witness.Create(onNext));
    }

    /// <summary>Subscribes a non-nullable value observer with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(2)]
    public static SubscribeSafeWitness<T> Subscribe<T>(
        IObservable<T> source,
        IObserver<T> observer,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeWitness<T>.Subscribe(source, observer);
    }

    /// <summary>Subscribes non-nullable value callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(2)]
    public static SubscribeSafeWitness<T> Subscribe<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeWitness<T>.Subscribe(source, Witness.Create(onNext, onError));
    }

    /// <summary>Subscribes non-nullable value callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(2)]
    public static SubscribeSafeWitness<T> Subscribe<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeWitness<T>.Subscribe(source, Witness.Create(onNext, onError, onCompleted));
    }

    /// <summary>Subscribes nullable reference error callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(1)]
    public static SubscribeSafeWitness<T?> SubscribeErrors<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : class
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T?>.SubscribeErrors(source, onError);
    }

    /// <summary>Subscribes nullable value error callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(1)]
    [SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification = "The class-constrained and struct-constrained overloads cannot forward to one another.")]
    public static SubscribeSafeWitness<T?> SubscribeErrors<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T?>.SubscribeErrors(source, onError);
    }

    /// <summary>Subscribes nullable reference error and completion callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(1)]
    public static SubscribeSafeWitness<T?> SubscribeErrors<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : class
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T?>.SubscribeErrors(source, onError, onCompleted);
    }

    /// <summary>Subscribes nullable value error and completion callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(1)]
    [SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification = "The class-constrained and struct-constrained overloads cannot forward to one another.")]
    public static SubscribeSafeWitness<T?> SubscribeErrors<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeWitness<T?>.SubscribeErrors(source, onError, onCompleted);
    }

    /// <summary>Subscribes non-nullable value error callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(1)]
    public static SubscribeSafeWitness<T> SubscribeErrors<T>(
        IObservable<T> source,
        Action<Exception> onError,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeWitness<T>.SubscribeErrors(source, onError);
    }

    /// <summary>Subscribes non-nullable value error and completion callbacks with downstream exception protection.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>The witness subscription.</returns>
    [OverloadResolutionPriority(1)]
    public static SubscribeSafeWitness<T> SubscribeErrors<T>(
        IObservable<T> source,
        Action<Exception> onError,
        Action onCompleted,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeWitness<T>.SubscribeErrors(source, onError, onCompleted);
    }
}
