// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Provides static factory and operator methods for signals.</summary>
public static partial class Signal
{
    /// <summary>Subscribes to the source and returns an awaiter for its final value.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="source">The source sequence, subscribed immediately.</param>
    /// <returns>An awaiter that completes with the final source value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IAwaitSignal<TSource> RunAsync<TSource>(IObservable<TSource> source) =>
        RunAsync(source, CancellationToken.None);

    /// <summary>Subscribes to the source and returns an awaiter for its final value, cancelled through the token.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="source">The source sequence, subscribed immediately.</param>
    /// <param name="cancellationToken">The token that disposes the subscription and faults the awaiter.</param>
    /// <returns>An awaiter that completes with the final source value, or faults with <see cref="OperationCanceledException"/>.</returns>
    public static IAwaitSignal<TSource> RunAsync<TSource>(
        IObservable<TSource> source,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        AsyncSignal<TSource> s = new();

        if (cancellationToken.IsCancellationRequested)
        {
            return Cancel(s, cancellationToken);
        }

        var d = source.Subscribe(s);

        if (cancellationToken.CanBeCanceled)
        {
            RegisterCancelation(s, d, cancellationToken);
        }

        return s;
    }

    /// <summary>Awaits source completion and returns the last value produced by the source.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A task that completes with the final source value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<TSource> ToTask<TSource>(IObservable<TSource> source) =>
        ToTask(source, CancellationToken.None);

    /// <summary>Awaits source completion and returns the last value produced by the source.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="cancellationToken">The token used to cancel the task and dispose the subscription.</param>
    /// <returns>A task that completes with the final source value.</returns>
    public static Task<TSource> ToTask<TSource>(IObservable<TSource> source, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<TSource>(cancellationToken);
        }

        if (TryCompleteTaskFromRange(source, out var rangeTask))
        {
            return rangeTask;
        }

        TaskTerminalCompletion<TSource> completion = new();
        var seen = false;
        var last = default(TSource);
        var subscription = source.Subscribe(
            value =>
            {
                seen = true;
                last = value;
            },
            completion.Fail,
            () =>
            {
                if (seen)
                {
                    completion.Resolve(last!);
                }
                else
                {
                    completion.FailEmpty();
                }
            });

        return completion.Attach(subscription, cancellationToken);
    }

    /// <summary>Faults the awaiter with an <see cref="OperationCanceledException"/> carrying the token.</summary>
    /// <typeparam name="T">The awaited value type.</typeparam>
    /// <param name="subject">The awaiter to fault.</param>
    /// <param name="cancellationToken">The token reported by the exception.</param>
    /// <returns>The same awaiter.</returns>
    internal static IAwaitSignal<T> Cancel<T>(IAwaitSignal<T> subject, CancellationToken cancellationToken)
    {
        subject.OnError(new OperationCanceledException(cancellationToken));
        return subject;
    }

    /// <summary>Cancels the awaiter and disposes the subscription when the token fires, releasing the registration once the awaiter terminates.</summary>
    /// <typeparam name="T">The awaited value type.</typeparam>
    /// <param name="subject">The awaiter to cancel.</param>
    /// <param name="subscription">The subscription disposed on cancellation.</param>
    /// <param name="token">The token watched for cancellation.</param>
    internal static void RegisterCancelation<T>(
        IAwaitSignal<T> subject,
        IDisposable subscription,
        CancellationToken token)
    {
        var ctr = token.UnsafeRegister(
            static state =>
            {
                var (subscription, subject, token) = ((IDisposable, IAwaitSignal<T>, CancellationToken))state!;
                subscription.Dispose();
                _ = Cancel(subject, token);
            },
            (subscription, subject, token));

        _ = subject.Subscribe(Handle<T>.Ignore, _ => ctr.Dispose(), ctr.Dispose);
    }

    /// <summary>Completes a task directly from a range signal when the source type can represent integers.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="task">The completed task when the fast path applies.</param>
    /// <returns><see langword="true"/> when the range fast path applies.</returns>
    private static bool TryCompleteTaskFromRange<T>(
        IObservable<T> source,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)]
        out Task<T>? task)
    {
        if (source is RangeSignal range && typeof(T).IsAssignableFrom(typeof(int)))
        {
            task = Task.FromResult((T)(object)(range.Start + range.Count - 1));
            return true;
        }

        task = null;
        return false;
    }
}
