// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents an asynchronous Signal that allows observers to receive values, errors, or completion notifications
/// asynchronously.
/// </summary>
/// <remarks>An asynchronous Signal enables push-based notification of values, errors, or completion events to
/// multiple observers. Observers can subscribe to the Signal's values stream and receive notifications as they are
/// published. This interface is typically used in scenarios where asynchronous event propagation and coordination are
/// required, such as reactive programming or event-driven architectures.</remarks>
/// <typeparam name="T">The type of the values observed and published by the Signal.</typeparam>
public interface ISignalAsync<T> : IObserverAsync<T>, IObservableAsync<T>
{
    /// <summary>Gets an observable sequence that asynchronously provides the current values of the collection.</summary>
    /// <remarks>The returned sequence emits updates whenever the underlying collection changes. Subscribers
    /// receive notifications asynchronously as values are added, removed, or updated.</remarks>
    IObservableAsync<T> Values { get; }
}
