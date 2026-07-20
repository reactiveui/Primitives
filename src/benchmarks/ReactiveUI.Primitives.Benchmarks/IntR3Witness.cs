// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using R3;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Observer used by R3 benchmark cases.</summary>
internal sealed class IntR3Witness : Observer<int>
{
    /// <summary>Gets the total of received values.</summary>
    internal int Total { get; private set; }

    /// <summary>Gets the number of onNext calls.</summary>
    internal int NextCount { get; private set; }

    /// <summary>Gets the last value observed.</summary>
    internal int LastValue { get; private set; }

    /// <summary>Gets the number of terminal completions observed.</summary>
    internal int CompletionCount { get; private set; }

    /// <summary>Gets the number of errors observed.</summary>
    internal int ErrorCount { get; private set; }

    /// <summary>Called for each emitted value.</summary>
    /// <param name="value">The emitted value.</param>
    protected override void OnNextCore(int value)
    {
        NextCount++;
        Total += value;
        LastValue = value;
    }

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
