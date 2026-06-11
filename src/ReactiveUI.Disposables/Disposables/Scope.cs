// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>Scope.</summary>
public static class Scope
{
    /// <summary>Gets the disposable that does nothing when disposed.</summary>
    public static IDisposable Empty { get; } = EmptyDisposable.Instance;

    /// <summary>Creates a disposable object that invokes the specified action when disposed.</summary>
    /// <param name="dispose">Action to run during the first call to <see cref="IDisposable.Dispose"/>. The action is guaranteed to be run at most once.</param>
    /// <returns>The disposable object that runs the given action upon disposal.</returns>
    /// <remarks>A <see langword="null"/> action returns <see cref="Empty"/> for backward compatibility with existing ReactiveUI.Primitives create pipelines.</remarks>
    public static IDisposable Create(Action dispose) =>
        dispose is null ? Empty : new ActionDisposable(dispose);

    /// <summary>Creates a disposable object that invokes the specified stateful action when disposed.</summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <param name="state">State passed to <paramref name="dispose"/>.</param>
    /// <param name="dispose">Action to run during the first call to <see cref="IDisposable.Dispose"/>.</param>
    /// <returns>The disposable object that runs the given action upon disposal.</returns>
    public static IDisposable Create<TState>(TState state, Action<TState> dispose) =>
        dispose is null ? Empty : new StateActionDisposable<TState>(state, dispose);

    /// <summary>Combines two disposable resources into a single disposable.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    /// <returns>A disposable that disposes both supplied resources.</returns>
    public static IDisposable Combine(IDisposable first, IDisposable second) =>
        new MultipleDisposable(first, second);

    /// <summary>Combines disposable resources into a single disposable.</summary>
    /// <param name="disposables">The disposables to combine.</param>
    /// <returns>A disposable that disposes all supplied resources.</returns>
    public static IDisposable Combine(params IDisposable[] disposables) =>
        new MultipleDisposable(disposables);
}
