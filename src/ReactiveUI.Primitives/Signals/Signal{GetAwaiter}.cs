// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
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
    /// <returns>An AsyncSignal.</returns>
    /// <exception cref="System.ArgumentNullException">source.</exception>
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
    /// An AsyncSignal.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">source.</exception>
    public static IAwaitSignal<TSource> GetAwaiter<TSource>(this IObservable<TSource> source, CancellationToken cancellationToken)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return RunAsync(source, cancellationToken);
    }

#pragma warning disable RCS1047 // Non-asynchronous method name should not end with 'Async'.
    private static IAwaitSignal<TSource> RunAsync<TSource>(IObservable<TSource> source, CancellationToken cancellationToken)
#pragma warning restore RCS1047 // Non-asynchronous method name should not end with 'Async'.
    {
        var s = new AsyncSignal<TSource>();

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

    private static IAwaitSignal<T> Cancel<T>(IAwaitSignal<T> subject, CancellationToken cancellationToken)
    {
        subject.OnError(new OperationCanceledException(cancellationToken));
        return subject;
    }

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
