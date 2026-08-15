// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        IObserver<T> observer,
        params bool[] allowNullable)
        where T : class
    {
        _ = allowNullable;
        return SubscribeSafeCore(source, (IObserver<T?>)observer);
    }

    /// <summary>Subscribes a nullable value observer with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        IObserver<T?> observer,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeCore(source, observer);
    }

    /// <summary>Subscribes nullable reference callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<T> onNext,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : class
    {
        _ = allowNullable;
        return SubscribeSafeCore(source, Witness.Create<T?>(value => onNext(value!), onError));
    }

    /// <summary>Subscribes nullable value callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<T?> onNext,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeCore(source, Witness.Create(onNext, onError));
    }

    /// <summary>Subscribes nullable reference callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : class
    {
        _ = allowNullable;
        return SubscribeSafeCore(source, Witness.Create<T?>(value => onNext(value!), onError, onCompleted));
    }

    /// <summary>Subscribes nullable value callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<T?> onNext,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeCore(source, Witness.Create(onNext, onError, onCompleted));
    }

    /// <summary>Subscribes nullable reference terminal callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "PublicApi",
        "PAS0003:Public API differs from the baseline",
        Justification =
            "This overload and its 'where T : struct' twin have identical signatures apart from the generic "
            + "constraint. The baseline records both, but an entry is resolved by the constraint-erased signature "
            + "and compared against the first match, so only one of the pair can ever be matched.")]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : class
    {
        _ = allowNullable;
        return SubscribeSafeCore(source, Witness.Create<T?>(static _ => { }, onError));
    }

    /// <summary>Subscribes nullable value terminal callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "Reference-type and value-type constraint overloads (where T : class / where T : struct) of the same "
            + "operator. The bodies are necessarily identical, and the two cannot forward to one another because their "
            + "generic constraints differ; both must exist so nullable T? resolves for either kind of T.")]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeCore(source, Witness.Create<T?>(static _ => { }, onError));
    }

    /// <summary>Subscribes nullable reference terminal callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable reference value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : class
    {
        _ = allowNullable;
        return SubscribeSafeCore(source, Witness.Create<T?>(static _ => { }, onError, onCompleted));
    }

    /// <summary>Subscribes nullable value terminal callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowNullable">Reserved for nullable overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "Reference-type and value-type constraint overloads (where T : class / where T : struct) of the same "
            + "operator. The bodies are necessarily identical, and the two cannot forward to one another because their "
            + "generic constraints differ; both must exist so nullable T? resolves for either kind of T.")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "PublicApi",
        "PAS0003:Public API differs from the baseline",
        Justification =
            "This overload and its 'where T : class' twin have identical signatures apart from the generic "
            + "constraint. The baseline records both, but an entry is resolved by the constraint-erased signature "
            + "and compared against the first match, so only one of the pair can ever be matched.")]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T?> source,
        Action<Exception> onError,
        Action onCompleted,
        params bool[] allowNullable)
        where T : struct
    {
        _ = allowNullable;
        return SubscribeSafeCore(source, Witness.Create<T?>(static _ => { }, onError, onCompleted));
    }

    /// <summary>Subscribes a non-nullable value observer with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T> source,
        IObserver<T> observer,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeCore(source, observer);
    }

    /// <summary>Subscribes non-nullable value callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeCore(source, Witness.Create(onNext, onError));
    }

    /// <summary>Subscribes non-nullable value callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeCore(source, Witness.Create(onNext, onError, onCompleted));
    }

    /// <summary>Subscribes non-nullable value terminal callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T> source,
        Action<Exception> onError,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeCore(source, Witness.Create<T>(static _ => { }, onError));
    }

    /// <summary>Subscribes non-nullable value terminal callbacks with downstream exception protection from static-call syntax.</summary>
    /// <typeparam name="T">The non-nullable value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The action to invoke for an error.</param>
    /// <param name="onCompleted">The action to invoke when the sequence completes.</param>
    /// <param name="allowValueType">Reserved for value-type overload resolution.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    [System.Runtime.CompilerServices.OverloadResolutionPriority(1)]
    public static IDisposable SubscribeSafe<T>(
        IObservable<T> source,
        Action<Exception> onError,
        Action onCompleted,
        params byte[] allowValueType)
        where T : struct
    {
        _ = allowValueType;
        return SubscribeSafeCore(source, Witness.Create<T>(static _ => { }, onError, onCompleted));
    }

    /// <summary>Subscribes an observer with downstream exception protection.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>A disposable that cancels the subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
    private static SubscribeSafeWitness<T> SubscribeSafeCore<T>(IObservable<T> source, IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(observer);

        SubscribeSafeWitness<T> safe = new(observer);
        safe.SetSubscription(source.Subscribe(safe));
        return safe;
    }
}
