// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Provides static factory and operator methods for signals.</summary>
public static partial class Signal
{
    /// <summary>Executes the RunAsync operation.</summary>
    /// <typeparam name="TSource">The TSource type.</typeparam>
    /// <param name="source">The source value.</param>
    /// <returns>The result.</returns>
    public static IAwaitSignal<TSource> RunAsync<TSource>(IObservable<TSource> source) =>
        RunAsync(source, CancellationToken.None);

    /// <summary>Executes the RunAsync operation.</summary>
    /// <typeparam name="TSource">The TSource type.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="cancellationToken">The cancellationToken value.</param>
    /// <returns>The result.</returns>
    public static IAwaitSignal<TSource> RunAsync<TSource>(IObservable<TSource> source, CancellationToken cancellationToken)
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

    /// <summary>Executes the Cancel operation.</summary>
    /// <typeparam name="T">The T type.</typeparam>
    /// <param name="subject">The subject value.</param>
    /// <param name="cancellationToken">The cancellationToken value.</param>
    /// <returns>The result.</returns>
    internal static IAwaitSignal<T> Cancel<T>(IAwaitSignal<T> subject, CancellationToken cancellationToken)
    {
        subject.OnError(new OperationCanceledException(cancellationToken));
        return subject;
    }

    /// <summary>Executes the RegisterCancelation operation.</summary>
    /// <typeparam name="T">The T type.</typeparam>
    /// <param name="subject">The subject value.</param>
    /// <param name="subscription">The subscription value.</param>
    /// <param name="token">The token value.</param>
    internal static void RegisterCancelation<T>(IAwaitSignal<T> subject, IDisposable subscription, CancellationToken token)
    {
        var ctr = token.Register(() =>
        {
            subscription.Dispose();
            _ = Cancel(subject, token);
        });

        _ = subject.Subscribe(Handle<T>.Ignore, _ => ctr.Dispose(), ctr.Dispose);
    }

    /// <summary>Completes a task directly from a range signal when the source type can represent integers.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="task">The completed task when the fast path applies.</param>
    /// <returns><see langword="true"/> when the range fast path applies.</returns>
    private static bool TryCompleteTaskFromRange<T>(IObservable<T> source, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Task<T>? task)
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
