// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Represents an asynchronous Signal that allows observers to receive values, errors, or completion notifications asynchronously.</summary>
/// <typeparam name="T">The type of the values observed and published by the Signal.</typeparam>
public interface ISignalAsync<T> : IObserverAsync<T>, IObservableAsync<T>
{
    /// <summary>Gets an observable sequence that asynchronously provides the current values of the collection.</summary>
    IObservableAsync<T> Values { get; }
}
