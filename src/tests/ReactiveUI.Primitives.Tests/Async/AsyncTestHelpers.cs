// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using ReactiveUI.Primitives.Async;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Shared test helpers for async observable tests.
/// </summary>
internal static class AsyncTestHelpers
{
    /// <summary>
    /// Collects all items and the completion result from an async observable.
    /// </summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The async observable to collect from.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>A tuple containing the collected items and the completion result.</returns>
    internal static Task<(List<T> Items, Result? Completion)> CollectAsync<T>(
        ObservableAsync<T> source,
        CancellationToken cancellationToken = default) =>
        CollectAsync(source, TimeProvider.System, cancellationToken);

    /// <summary>
    /// Collects all items and the completion result from an async observable using the supplied
    /// <see cref="TimeProvider"/> for the completion-propagation deadline.
    /// </summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The async observable to collect from.</param>
    /// <param name="timeProvider">Time provider used to compute the propagation deadline.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>A tuple containing the collected items and the completion result.</returns>
    internal static async Task<(List<T> Items, Result? Completion)> CollectAsync<T>(
        ObservableAsync<T> source,
        TimeProvider timeProvider,
        CancellationToken cancellationToken = default)
    {
        const int CompletionPollIntervalMs = 10;
        ArgumentNullException.ThrowIfNull(timeProvider);

        var items = new List<T>();
        Result? completion = null;

        await using var subscription = await source.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            result =>
            {
                completion = result;
                return default;
            },
            cancellationToken);

        // Wait briefly for completion to propagate
        var deadline = timeProvider.GetUtcNow().AddSeconds(5);
        while (completion is null && timeProvider.GetUtcNow() < deadline)
        {
            await Task.Delay(CompletionPollIntervalMs, CancellationToken.None);
        }

        return (items, completion);
    }

    /// <summary>
    /// Collects all items from an async observable using ToListAsync.
    /// </summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The async observable to collect from.</param>
    /// <param name="timeoutMs">The timeout in milliseconds before the operation is cancelled.</param>
    /// <returns>A list of all collected items.</returns>
    internal static async Task<List<T>> ToListWithTimeoutAsync<T>(
        ObservableAsync<T> source,
        int timeoutMs = 5_000)
    {
        using var cts = new CancellationTokenSource(timeoutMs);
        return await source.ToListAsync(cts.Token);
    }

    /// <summary>
    /// Waits until the provided condition is met or the timeout expires.
    /// </summary>
    /// <param name="condition">Condition to evaluate.</param>
    /// <param name="timeout">Maximum time to wait.</param>
    /// <param name="pollInterval">Optional polling interval.</param>
    /// <returns>True if the condition was met before timing out.</returns>
    internal static Task<bool> WaitForConditionAsync(
        Func<bool> condition,
        TimeSpan timeout,
        TimeSpan? pollInterval = null) =>
        WaitForConditionAsync(condition, timeout, TimeProvider.System, pollInterval);

    /// <summary>
    /// Waits until the provided condition is met or the timeout expires, using the supplied
    /// <see cref="TimeProvider"/> for deadline calculation.
    /// </summary>
    /// <param name="condition">Condition to evaluate.</param>
    /// <param name="timeout">Maximum time to wait.</param>
    /// <param name="timeProvider">Time provider used to compute the wait deadline.</param>
    /// <param name="pollInterval">Optional polling interval.</param>
    /// <returns>True if the condition was met before timing out.</returns>
    internal static async Task<bool> WaitForConditionAsync(
        Func<bool> condition,
        TimeSpan timeout,
        TimeProvider timeProvider,
        TimeSpan? pollInterval = null)
    {
        ArgumentNullException.ThrowIfNull(condition);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);

        var interval = pollInterval ?? TimeSpan.FromMilliseconds(10);
        var deadline = timeProvider.GetUtcNow().Add(timeout);

        while (timeProvider.GetUtcNow() < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(interval, CancellationToken.None);
        }

        return condition();
    }
}
