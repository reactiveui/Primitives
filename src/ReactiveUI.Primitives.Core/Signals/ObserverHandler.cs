// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>Represents the ObserverHandler class.</summary>
/// <typeparam name="T">The Type.</typeparam>
/// <param name="subject">The subject value.</param>
/// <param name="observer">The observer value.</param>
[System.Diagnostics.DebuggerDisplay("Subject = {_subject}, Observer = {_observer}")]
public sealed class ObserverHandler<T>(AsyncSignal<T> subject, IObserver<T> observer) : IDisposable
{
    /// <summary>Executes the new operation.</summary>
    /// <returns>The result.</returns>
    private readonly Lock _gate = new();

    /// <summary>Stores state for the signal implementation.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Back-reference to the subject; unsubscribed, not owned.")]
    private AsyncSignal<T>? _subject = subject;

    /// <summary>Stores state for the signal implementation.</summary>
    private IObserver<T>? _observer = observer;

    /// <summary>Executes the Dispose operation.</summary>
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
