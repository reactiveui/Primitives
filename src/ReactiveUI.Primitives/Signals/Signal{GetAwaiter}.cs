// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>Provides static factory and operator methods for signals.</summary>
public static partial class Signal
{
    /// <summary>Executes the RunAsync operation.</summary>
    /// <typeparam name="TSource">The TSource type.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="cancellationToken">The cancellationToken value.</param>
    /// <returns>The result.</returns>
    internal static IAwaitSignal<TSource> RunAsync<TSource>(IObservable<TSource> source, CancellationToken cancellationToken)
    {
        FinalSignal<TSource> s = new();

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
            Cancel(subject, token);
        });

        subject.Subscribe(Handle<T>.Ignore, _ => ctr.Dispose(), ctr.Dispose);
    }
}
