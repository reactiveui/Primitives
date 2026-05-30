// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// Disposable.
/// </summary>
public static class Disposable
{
    /// <summary>
    /// Gets the disposable that does nothing when disposed.
    /// </summary>
    public static IDisposable Empty { get; } = new EmptyDisposable();

    /// <summary>
    /// Creates a disposable object that invokes the specified action when disposed.
    /// </summary>
    /// <param name="dispose">Action to run during the first call to <see cref="IDisposable.Dispose"/>. The action is guaranteed to be run at most once.</param>
    /// <returns>The disposable object that runs the given action upon disposal.</returns>
    /// <remarks>A <see langword="null"/> action returns <see cref="Empty"/> for backward compatibility with existing ReactiveUI.Primitives create pipelines.</remarks>
    public static IDisposable Create(Action dispose) =>
        dispose == null ? Empty : new AnonymousDisposable(dispose);
}
