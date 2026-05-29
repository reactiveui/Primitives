// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Subjects;

/// <summary>
/// Represents an asynchronous subject that allows observers to receive values, errors, or completion notifications
/// asynchronously.
/// </summary>
/// <remarks>An asynchronous subject enables push-based notification of values, errors, or completion events to
/// multiple observers. Observers can subscribe to the subject's values stream and receive notifications as they are
/// published. This interface is typically used in scenarios where asynchronous event propagation and coordination are
/// required, such as reactive programming or event-driven architectures.</remarks>
/// <typeparam name="T">The type of the values observed and published by the subject.</typeparam>
public interface ISubjectAsync<T> : IObserverAsync<T>, IObservableAsync<T>
{
    /// <summary>
    /// Gets an observable sequence that asynchronously provides the current values of the collection.
    /// </summary>
    /// <remarks>The returned sequence emits updates whenever the underlying collection changes. Subscribers
    /// receive notifications asynchronously as values are added, removed, or updated.</remarks>
    IObservableAsync<T> Values { get; }
}
