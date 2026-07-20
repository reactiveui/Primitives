// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>An observer that records all values, errors, and completion counts.</summary>
/// <typeparam name="T">The type of the observed values.</typeparam>
internal sealed class RecordingWitness<T> : IObserver<T>
{
    /// <summary>Gets the recorded values.</summary>
    internal List<T> Values { get; } = [];

    /// <summary>Gets the recorded errors.</summary>
    internal List<Exception> Errors { get; } = [];

    /// <summary>Gets the number of completion callbacks observed.</summary>
    internal int Completed { get; private set; }

    /// <summary>Records a completion callback.</summary>
    public void OnCompleted() => Completed++;

    /// <summary>Records an error callback.</summary>
    /// <param name="error">The error to record.</param>
    public void OnError(Exception error) => Errors.Add(error);

    /// <summary>Records a value callback.</summary>
    /// <param name="value">The value to record.</param>
    public void OnNext(T value) => Values.Add(value);
}
