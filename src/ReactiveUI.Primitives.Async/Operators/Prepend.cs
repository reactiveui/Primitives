// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The SignalAsync class offers utility methods that enable manipulation and composition of
/// asynchronous observables, such as prepending values to a sequence. These methods facilitate common operations when
/// building reactive, asynchronous workflows.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Returns a new observable sequence that begins with the specified value, followed by the elements of the
    /// current sequence.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="value">The value to prepend to the beginning of the sequence.</param>
    /// <returns>An observable sequence with the specified value prepended to the original sequence.</returns>
    public static IObservableAsync<T> Prepend<T>(this IObservableAsync<T> @this, T value) => @this.Prepend([value]);

    /// <summary>
    /// Returns a new observable sequence that emits the specified values before the emissions from the current
    /// sequence.
    /// </summary>
    /// <remarks>The values in the provided collection are emitted in order before any items from the
    /// original sequence. If the sequence is unsubscribed before completion, remaining values may not be
    /// emitted.</remarks>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="values">The collection of values to emit before the original sequence. Cannot be null.</param>
    /// <returns>An observable sequence that emits the specified values first, followed by the items from the current
    /// sequence.</returns>
    public static IObservableAsync<T> Prepend<T>(this IObservableAsync<T> @this, IEnumerable<T> values) => Create<T>((observer, subscriptionToken) =>
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(subscriptionToken);
        SingleAssignmentDisposableAsync subscriptionDisposable = new();
        AsyncLocal<bool> reentrant = new();
        var task = RunPrependAsync(@this, values, observer, subscriptionDisposable, reentrant, cts.Token);
        var subscription = DisposableAsync.Create(() => DisposePrependAsync(subscriptionDisposable, cts, reentrant, task));
        return new(subscription);
    });

    /// <summary>
    /// Drives the prepend pipeline: emits the prefix values, subscribes to the source, and reports any failure
    /// to the downstream observer.
    /// </summary>
    /// <typeparam name="T">Source element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="values">Values to emit before subscribing to <paramref name="source"/>.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="subscriptionDisposable">Holds the upstream subscription so the disposer can release it.</param>
    /// <param name="reentrant">Flag set while emission is in-flight on the calling async-flow.</param>
    /// <param name="cancellationToken">Cancellation token linked to the subscription token.</param>
    /// <returns>A <see cref="Task"/> tracking the prepend pipeline.</returns>
    private static async Task RunPrependAsync<T>(
        IObservableAsync<T> source,
        IEnumerable<T> values,
        IObserverAsync<T> observer,
        SingleAssignmentDisposableAsync subscriptionDisposable,
        AsyncLocal<bool> reentrant,
        CancellationToken cancellationToken)
    {
        try
        {
            reentrant.Value = true;
            if (!await TryEmitPrefixAsync(values, observer, cancellationToken).ConfigureAwait(false))
            {
                return;
            }

            var subscription = await source.SubscribeAsync(observer.Wrap(), cancellationToken).ConfigureAwait(false);
            await subscriptionDisposable.SetDisposableAsync(subscription).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Subscription was cancelled cooperatively; nothing to forward.
        }
        catch (Exception e)
        {
            await ReportFailureAsync(observer, e).ConfigureAwait(false);
        }
    }

    /// <summary>Emits every prefix value, returning <see langword="false"/> if cancellation is observed mid-flight.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="values">The prefix sequence.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><see langword="true"/> when the prefix completed; <see langword="false"/> on cancellation.</returns>
    private static async ValueTask<bool> TryEmitPrefixAsync<T>(
        IEnumerable<T> values,
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        foreach (var value in values)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return false;
            }

            await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>Forwards a pipeline failure to the downstream observer, escalating to the unhandled handler on re-entry.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="error">The failure that ended the pipeline.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous failure forward.</returns>
    private static async ValueTask ReportFailureAsync<T>(IObserverAsync<T> observer, Exception error)
    {
        try
        {
            await observer.OnCompletedAsync(Result.Failure(error)).ConfigureAwait(false);
        }
        catch (Exception escalated)
        {
            UnhandledExceptionHandler.OnUnhandledException(escalated);
        }
    }

    /// <summary>
    /// Tears the prepend pipeline down: cancels the link, releases the upstream subscription, and awaits the
    /// driver task only when the caller is not itself inside the pipeline.
    /// </summary>
    /// <param name="subscriptionDisposable">The upstream-subscription holder.</param>
    /// <param name="cts">The pipeline cancellation source.</param>
    /// <param name="reentrant">The re-entry flag set by <see cref="RunPrependAsync{T}"/>.</param>
    /// <param name="task">The driver task.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous teardown.</returns>
    private static async ValueTask DisposePrependAsync(
        SingleAssignmentDisposableAsync subscriptionDisposable,
        CancellationTokenSource cts,
        AsyncLocal<bool> reentrant,
        Task task)
    {
        await subscriptionDisposable.DisposeAsync().ConfigureAwait(false);
        await cts.CancelAsync().ConfigureAwait(false);
        if (!reentrant.Value)
        {
            await task.ConfigureAwait(false);
        }

        cts.Dispose();
    }
}
