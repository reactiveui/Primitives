// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>Subscription handle that removes its observer from the signal exactly once when disposed.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <param name="subject">The signal the observer is subscribed to.</param>
/// <param name="observer">The subscribed observer.</param>
[System.Diagnostics.DebuggerDisplay("ObserverHandler: Subject = {_subject}, Observer = {_observer}")]
public sealed class ObserverHandler<T>(AsyncSignal<T> subject, IObserver<T> observer) : IDisposable
{
    /// <summary>Serializes detachment from the signal.</summary>
    private readonly Lock _gate = new();

    /// <summary>The signal to unsubscribe from, cleared by the first disposal.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Back-reference to the subject; unsubscribed, not owned.")]
    private AsyncSignal<T>? _subject = subject;

    /// <summary>The observer to remove, cleared by the first disposal.</summary>
    private IObserver<T>? _observer = observer;

    /// <summary>Detaches the observer once and releases references to both endpoints.</summary>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_subject is null)
            {
                return;
            }

            _subject.RemoveObserver(_observer!);
            _observer = null;
            _subject = null;
        }
    }
}
