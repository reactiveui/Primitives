// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using R3;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Observer used by Signal and System.Reactive benchmark cases.
/// </summary>
internal sealed class IntSignalObserver : IObserver<int>
{
    /// <summary>
    /// Gets the total of received values.
    /// </summary>
    public int Total { get; private set; }

    /// <summary>
    /// Gets the number of onNext calls.
    /// </summary>
    public int NextCount { get; private set; }

    /// <summary>
    /// Gets the last value observed.
    /// </summary>
    public int LastValue { get; private set; }

    /// <summary>
    /// Gets the number of terminal completions observed.
    /// </summary>
    public int CompletionCount { get; private set; }

    /// <summary>
    /// Gets the number of errors observed.
    /// </summary>
    public int ErrorCount { get; private set; }

    /// <inheritdoc/>
    public void OnNext(int value)
    {
        NextCount++;
        Total += value;
        LastValue = value;
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        ErrorCount++;
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        CompletionCount++;
    }
}

/// <summary>
/// Long observer used by Signal and System.Reactive benchmark cases.
/// </summary>
internal sealed class LongSignalObserver : IObserver<long>
{
    /// <summary>
    /// Gets the total of received values.
    /// </summary>
    public long Total { get; private set; }

    /// <summary>
    /// Gets the number of onNext calls.
    /// </summary>
    public int NextCount { get; private set; }

    /// <summary>
    /// Gets the last value observed.
    /// </summary>
    public long LastValue { get; private set; }

    /// <summary>
    /// Gets the number of terminal completions observed.
    /// </summary>
    public int CompletionCount { get; private set; }

    /// <summary>
    /// Gets the number of errors observed.
    /// </summary>
    public int ErrorCount { get; private set; }

    /// <inheritdoc/>
    public void OnNext(long value)
    {
        NextCount++;
        Total += value;
        LastValue = value;
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        ErrorCount++;
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        CompletionCount++;
    }
}

/// <summary>
/// Boolean observer used by Signal and System.Reactive benchmark cases.
/// </summary>
internal sealed class BoolSignalObserver : IObserver<bool>
{
    /// <summary>
    /// Gets the total of true values observed.
    /// </summary>
    public int Total { get; private set; }

    /// <summary>
    /// Gets the number of onNext calls.
    /// </summary>
    public int NextCount { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the last observed value was true.
    /// </summary>
    public bool LastValue { get; private set; }

    /// <summary>
    /// Gets the number of terminal completions observed.
    /// </summary>
    public int CompletionCount { get; private set; }

    /// <summary>
    /// Gets the number of errors observed.
    /// </summary>
    public int ErrorCount { get; private set; }

    /// <inheritdoc/>
    public void OnNext(bool value)
    {
        NextCount++;
        if (value)
        {
            Total++;
        }

        LastValue = value;
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        ErrorCount++;
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        CompletionCount++;
    }
}

/// <summary>
/// Observer used by R3 benchmark cases.
/// </summary>
internal sealed class IntR3Observer : Observer<int>
{
    /// <summary>
    /// Gets the total of received values.
    /// </summary>
    public int Total { get; private set; }

    /// <summary>
    /// Gets the number of onNext calls.
    /// </summary>
    public int NextCount { get; private set; }

    /// <summary>
    /// Gets the last value observed.
    /// </summary>
    public int LastValue { get; private set; }

    /// <summary>
    /// Gets the number of terminal completions observed.
    /// </summary>
    public int CompletionCount { get; private set; }

    /// <summary>
    /// Gets the number of errors observed.
    /// </summary>
    public int ErrorCount { get; private set; }

    /// <summary>
    /// Called for each emitted value.
    /// </summary>
    /// <param name="value">The emitted value.</param>
    protected override void OnNextCore(int value)
    {
        NextCount++;
        Total += value;
        LastValue = value;
    }

    /// <summary>
    /// Called when an error is observed.
    /// </summary>
    /// <param name="error">The observed exception.</param>
    protected override void OnErrorResumeCore(Exception error)
    {
        ErrorCount++;
    }

    /// <summary>
    /// Called when sequence completed.
    /// </summary>
    /// <param name="result">The completion result.</param>
    protected override void OnCompletedCore(Result result)
    {
        if (result.IsFailure)
        {
            ErrorCount++;
            return;
        }

        CompletionCount++;
    }
}

/// <summary>
/// Observer used by R3 benchmark cases that only need an item count.
/// </summary>
/// <typeparam name="T">The observed value type.</typeparam>
internal sealed class CountingR3Observer<T> : Observer<T>
{
    /// <summary>
    /// Gets the number of onNext calls.
    /// </summary>
    public int Count { get; private set; }

    /// <summary>
    /// Gets the number of terminal completions observed.
    /// </summary>
    public int CompletionCount { get; private set; }

    /// <summary>
    /// Gets the number of errors observed.
    /// </summary>
    public int ErrorCount { get; private set; }

    /// <summary>
    /// Called for each emitted value.
    /// </summary>
    /// <param name="value">The emitted value.</param>
    protected override void OnNextCore(T value)
    {
        Count++;
    }

    /// <summary>
    /// Called when an error is observed.
    /// </summary>
    /// <param name="error">The observed exception.</param>
    protected override void OnErrorResumeCore(Exception error)
    {
        ErrorCount++;
    }

    /// <summary>
    /// Called when sequence completed.
    /// </summary>
    /// <param name="result">The completion result.</param>
    protected override void OnCompletedCore(Result result)
    {
        if (result.IsFailure)
        {
            ErrorCount++;
            return;
        }

        CompletionCount++;
    }
}

/// <summary>
/// Minimal R3 observer for benchmarks that only need subscription lifecycle handling.
/// </summary>
internal sealed class IntR3ActionObserver : Observer<int>
{
    /// <summary>
    /// Receives the next value.
    /// </summary>
    /// <param name="value">The value.</param>
    protected override void OnNextCore(int value)
    {
    }

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
