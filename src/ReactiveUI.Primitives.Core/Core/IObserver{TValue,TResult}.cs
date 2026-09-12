// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

/// <summary>Provides a mechanism for receiving push-based notifications and returning a response.</summary>
/// <typeparam name="TValue">The element type received by the observer.</typeparam>
/// <typeparam name="TResult">The result type returned from the observer's notification handlers.</typeparam>
public interface IObserver<in TValue, out TResult>
{
    /// <summary>Notifies the observer of a new element in the sequence.</summary>
    /// <param name="value">The new element in the sequence.</param>
    /// <returns>Result returned upon observation of a new element.</returns>
    TResult OnNext(TValue value);

    /// <summary>Notifies the observer that an exception has occurred.</summary>
    /// <param name="exception">The exception that occurred.</param>
    /// <returns>Result returned upon observation of an error.</returns>
    TResult OnError(Exception exception);

    /// <summary>Notifies the observer of the end of the sequence.</summary>
    /// <returns>Result returned upon observation of the sequence completion.</returns>
    TResult OnCompleted();
}
