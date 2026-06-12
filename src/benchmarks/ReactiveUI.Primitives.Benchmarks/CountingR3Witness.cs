// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using R3;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Observer used by R3 benchmark cases that only need an item count.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
internal sealed class CountingR3Witness<T> : Observer<T>
{
    /// <summary>Gets the number of onNext calls.</summary>
    public int Count { get; private set; }

    /// <summary>Gets the number of terminal completions observed.</summary>
    public int CompletionCount { get; private set; }

    /// <summary>Gets the number of errors observed.</summary>
    public int ErrorCount { get; private set; }

    /// <summary>Called for each emitted value.</summary>
    /// <param name="value">The emitted value.</param>
    protected override void OnNextCore(T value) => Count++;

    /// <summary>Called when an error is observed.</summary>
    /// <param name="error">The observed exception.</param>
    protected override void OnErrorResumeCore(Exception error) => ErrorCount++;

    /// <summary>Called when sequence completed.</summary>
    /// <param name="result">The completion result.</param>
    protected override void OnCompletedCore(R3.Result result)
    {
        if (result.IsFailure)
        {
            ErrorCount++;
            return;
        }

        CompletionCount++;
    }
}
