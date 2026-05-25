// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
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

    internal sealed class EmptyDisposable : IDisposable
    {
        public void Dispose()
        {
        }
    }

    /// <summary>
    /// Represents an Action-based disposable.
    /// </summary>
    internal sealed class AnonymousDisposable : IDisposable
    {
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
}
