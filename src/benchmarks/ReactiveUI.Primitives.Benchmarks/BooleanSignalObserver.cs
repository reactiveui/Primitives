// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Observer used to capture a boolean result in benchmark cases.
/// </summary>
internal sealed class BooleanSignalObserver : IObserver<bool>
{
    /// <summary>
    /// Gets a value indicating whether the latest sequence value was <see langword="true" />.
    /// </summary>
    public bool Value { get; private set; }

    /// <summary>
    /// Gets the number of terminal completions observed.
    /// </summary>
    public int CompletionCount { get; private set; }

    /// <summary>
    /// Gets the number of errors observed.
    /// </summary>
    public int ErrorCount { get; private set; }

    /// <summary>
    /// Called when a value is received.
    /// </summary>
    /// <param name="value">The value.</param>
    public void OnNext(bool value)
    {
        Value = value;
    }

    /// <summary>
    /// Called when an error is observed.
    /// </summary>
    /// <param name="error">The exception.</param>
    public void OnError(Exception error)
    {
        ErrorCount++;
    }

    /// <summary>
    /// Called when sequence completed.
    /// </summary>
    public void OnCompleted()
    {
        CompletionCount++;
    }
}
