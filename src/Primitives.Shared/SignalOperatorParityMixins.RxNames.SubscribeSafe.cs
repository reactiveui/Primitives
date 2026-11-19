// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>The System.Reactive-named SubscribeSafe overloads, which isolate observer faults from the source.</summary>
public static partial class LinqExtensions
{
    /// <summary>Subscribes a nullable reference observer with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        IObserver<T> observer,
        params bool[] allowNullable)
        where T : class =>
        SubscribeSafeWitness.Subscribe(source, observer, allowNullable);

    /// <summary>Subscribes a nullable value observer with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        IObserver<T?> observer,
        params bool[] allowNullable)
        where T : struct =>
        SubscribeSafeWitness.Subscribe(source, observer, allowNullable);

    /// <summary>Subscribes nullable reference callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<T> onNext,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : class =>
        SubscribeSafeWitness.Subscribe(source, onNext, onError, allowNullable);

    /// <summary>Subscribes nullable value callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<T?> onNext,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : struct =>
        SubscribeSafeWitness.Subscribe(source, onNext, onError, allowNullable);

    /// <summary>Subscribes nullable reference callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : class =>
        SubscribeSafeWitness.Subscribe(source, onNext, onError, onCompleted, allowNullable);

    /// <summary>Subscribes nullable value callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<T?> onNext,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : struct =>
        SubscribeSafeWitness.Subscribe(source, onNext, onError, onCompleted, allowNullable);

    /// <summary>Subscribes a nullable reference value callback with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    /// <remarks>With no error handler, a source error is rethrown to the caller of the source's <c>OnError</c>.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<T> onNext,
        params bool[] allowNullable)
        where T : class =>
        SubscribeSafeWitness.Subscribe(source, onNext, allowNullable);

    /// <summary>Subscribes a nullable value callback with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    /// <remarks>With no error handler, a source error is rethrown to the caller of the source's <c>OnError</c>.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<T?> onNext,
        params bool[] allowNullable)
        where T : struct =>
        SubscribeSafeWitness.Subscribe(source, onNext, allowNullable);

    /// <summary>Subscribes non-nullable value callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    /// <remarks>With no error handler, a source error is rethrown to the caller of the source's <c>OnError</c>.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T> source,
        Action<T> onNext,
        params byte[] allowValueType)
        where T : struct =>
        SubscribeSafeWitness.Subscribe(source, onNext, allowValueType);

    /// <summary>Subscribes a non-nullable value observer with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T> source,
        IObserver<T> observer,
        params byte[] allowValueType)
        where T : struct =>
        SubscribeSafeWitness.Subscribe(source, observer, allowValueType);

    /// <summary>Subscribes non-nullable value callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        params byte[] allowValueType)
        where T : struct =>
        SubscribeSafeWitness.Subscribe(source, onNext, onError, allowValueType);

    /// <summary>Subscribes non-nullable value callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted,
        params byte[] allowValueType)
        where T : struct =>
        SubscribeSafeWitness.Subscribe(source, onNext, onError, onCompleted, allowValueType);

    /// <summary>Subscribes nullable reference error callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafeErrors<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : class =>
        SubscribeSafeWitness.SubscribeErrors(source, onError, allowNullable);

    /// <summary>Subscribes nullable value error callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    [SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification = "The class-constrained and struct-constrained overloads cannot forward to one another.")]
    public static IDisposable SubscribeSafeErrors<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : struct =>
        SubscribeSafeWitness.SubscribeErrors(source, onError, allowNullable);

    /// <summary>Subscribes nullable reference error and completion callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafeErrors<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : class =>
        SubscribeSafeWitness.SubscribeErrors(source, onError, onCompleted, allowNullable);

    /// <summary>Subscribes nullable value error and completion callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    [SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification = "The class-constrained and struct-constrained overloads cannot forward to one another.")]
    public static IDisposable SubscribeSafeErrors<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : struct =>
        SubscribeSafeWitness.SubscribeErrors(source, onError, onCompleted, allowNullable);

    /// <summary>Subscribes non-nullable value error callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafeErrors<T>(
        IObservable<T> source,
        Action<Exception> onError,
        params byte[] allowValueType)
        where T : struct =>
        SubscribeSafeWitness.SubscribeErrors(source, onError, allowValueType);

    /// <summary>Subscribes non-nullable value error and completion callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafeErrors<T>(
        IObservable<T> source,
        Action<Exception> onError,
        Action onCompleted,
        params byte[] allowValueType)
        where T : struct =>
        SubscribeSafeWitness.SubscribeErrors(source, onError, onCompleted, allowValueType);
}
