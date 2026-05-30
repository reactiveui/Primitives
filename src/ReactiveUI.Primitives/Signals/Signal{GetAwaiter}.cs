// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Signal.
/// </summary>
public static partial class Signal
{
    /// <summary>
    /// Gets an awaiter that returns the last value of the observable sequence or throws an exception if the sequence is empty.
    /// This operation subscribes to the observable sequence, making it hot.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <param name="source">Source sequence to await.</param>
    /// <returns>A final signal awaiter.</returns>
    /// <exception cref="ArgumentNullException">source.</exception>
    public static IAwaitSignal<TSource> GetAwaiter<TSource>(this IObservable<TSource> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return RunAsync(source, CancellationToken.None);
    }

    /// <summary>
    /// Gets an awaiter that returns the last value of the observable sequence or throws an exception if the sequence is empty.
    /// This operation subscribes to the observable sequence, making it hot.
    /// </summary>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    /// <param name="source">Source sequence to await.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A final signal awaiter.
    /// </returns>
    /// <exception cref="ArgumentNullException">source.</exception>
    public static IAwaitSignal<TSource> GetAwaiter<TSource>(this IObservable<TSource> source, CancellationToken cancellationToken)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return RunAsync(source, cancellationToken);
    }

    /// <summary>
    /// Executes the RunAsync operation.
    /// </summary>
    /// <typeparam name="TSource">The TSource type.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="cancellationToken">The cancellationToken value.</param>
    /// <returns>The result.</returns>
    private static IAwaitSignal<TSource> RunAsync<TSource>(IObservable<TSource> source, CancellationToken cancellationToken)
    {
        var s = new FinalSignal<TSource>();

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

    /// <summary>
    /// Executes the Cancel operation.
    /// </summary>
    /// <typeparam name="T">The T type.</typeparam>
    /// <param name="subject">The subject value.</param>
    /// <param name="cancellationToken">The cancellationToken value.</param>
    /// <returns>The result.</returns>
    private static IAwaitSignal<T> Cancel<T>(IAwaitSignal<T> subject, CancellationToken cancellationToken)
    {
        subject.OnError(new OperationCanceledException(cancellationToken));
        return subject;
    }

    /// <summary>
    /// Executes the RegisterCancelation operation.
    /// </summary>
    /// <typeparam name="T">The T type.</typeparam>
    /// <param name="subject">The subject value.</param>
    /// <param name="subscription">The subscription value.</param>
    /// <param name="token">The token value.</param>
    private static void RegisterCancelation<T>(IAwaitSignal<T> subject, IDisposable subscription, CancellationToken token)
    {
        var ctr = token.Register(() =>
        {
            subscription.Dispose();
            Cancel(subject, token);
        });

        subject.Subscribe(Handle<T>.Ignore, _ => ctr.Dispose(), ctr.Dispose);
    }
}
