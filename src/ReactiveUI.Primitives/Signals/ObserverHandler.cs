// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Represents the ObserverHandler class.
/// </summary>
/// <typeparam name="T">The Type.</typeparam>
public sealed class ObserverHandler<T> : IDisposable
{
    /// <summary>
    /// Executes the new operation.
    /// </summary>
    /// <returns>The result.</returns>
    private readonly Lock _gate = new();

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private AsyncSignal<T>? _subject;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private IObserver<T>? _observer;

    /// <summary>
    /// Initializes a new instance of the <see cref="ObserverHandler{T}"/> class.
    /// </summary>
    /// <param name="subject">The subject value.</param>
    /// <param name="observer">The observer value.</param>
    public ObserverHandler(AsyncSignal<T> subject, IObserver<T> observer)
    {
        _subject = subject;
        _observer = observer;
    }

    /// <summary>
    /// Executes the Dispose operation.
    /// </summary>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_subject == null)
            {
                return;
            }

            _subject.RemoveObserver(_observer!);
            _observer = null;
            _subject = null;
        }
    }
}
