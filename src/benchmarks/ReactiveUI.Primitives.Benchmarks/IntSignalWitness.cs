// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Observer used by Signal and System.Reactive benchmark cases.</summary>
internal sealed class IntSignalWitness : IObserver<int>
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

    /// <inheritdoc/>
    public void OnNext(int value)
    {
        NextCount++;
        Total += value;
        LastValue = value;
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => ErrorCount++;

    /// <inheritdoc/>
    public void OnCompleted() => CompletionCount++;
}
