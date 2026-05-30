// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// Represents an Action-based disposable.
/// </summary>
public sealed class AnonymousDisposable : IDisposable
{
    /// <summary>
    /// Disposal action, cleared after the first dispose call.
    /// </summary>
    private volatile Action? _dispose;

    /// <summary>
    /// Initializes a new instance of the <see cref="AnonymousDisposable"/> class.
    /// </summary>
    /// <param name="dispose">The dispose.</param>
    public AnonymousDisposable(Action dispose) =>
        _dispose = dispose;

    /// <summary>
    /// Calls the disposal action if and only if the current instance hasn't been disposed yet.
    /// </summary>
    public void Dispose() =>
        Interlocked.Exchange(ref _dispose, null)?.Invoke();
}