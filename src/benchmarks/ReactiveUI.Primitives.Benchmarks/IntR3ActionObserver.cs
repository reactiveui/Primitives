// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using R3;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Minimal R3 observer for benchmarks that only need subscription lifecycle handling.
/// </summary>
internal sealed class IntR3ActionObserver : Observer<int>
{
    /// <summary>
    /// The optional per-value callback, or <see langword="null"/> for a no-op observer.
    /// </summary>
    private readonly Action<int>? _onNext;

    /// <summary>
    /// Initializes a new instance of the <see cref="IntR3ActionObserver"/> class.
    /// </summary>
    /// <param name="onNext">An optional callback invoked for each value.</param>
    public IntR3ActionObserver(Action<int>? onNext = null) => _onNext = onNext;

    /// <summary>
    /// Receives the next value.
    /// </summary>
    /// <param name="value">The value.</param>
    protected override void OnNextCore(int value) => _onNext?.Invoke(value);

    /// <summary>
    /// Receives an error.
    /// </summary>
    /// <param name="error">The observed exception.</param>
    protected override void OnErrorResumeCore(Exception error)
    {
    }

    /// <summary>
    /// Receives the completion result.
    /// </summary>
    /// <param name="result">Completion metadata.</param>
    protected override void OnCompletedCore(Result result)
    {
    }
}
