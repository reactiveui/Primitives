// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>
/// Bundled retry configuration for <see cref="RetryWithBackoffObservable{T}"/>: retry count,
/// delay schedule, scheduler, and an optional error sink. A <see langword="readonly record struct"/>
/// so it stays allocation-free and keeps consuming constructors below Sonar's S107 parameter limit.
/// </summary>
/// <param name="MaxRetries">Maximum number of retries.</param>
/// <param name="InitialDelay">Delay before the first retry.</param>
/// <param name="BackoffFactor">Multiplier applied to the delay per retry attempt.</param>
/// <param name="MaxDelay">Cap on the computed delay, or <see langword="null"/> for no cap.</param>
/// <param name="Scheduler">Scheduler used to schedule each delay.</param>
/// <param name="OnError">Optional callback invoked on every upstream error.</param>
internal readonly record struct RetryBackoffPolicy(
    int MaxRetries,
    TimeSpan InitialDelay,
    double BackoffFactor,
    TimeSpan? MaxDelay,
    ISequencer Scheduler,
    Action<Exception>? OnError);
