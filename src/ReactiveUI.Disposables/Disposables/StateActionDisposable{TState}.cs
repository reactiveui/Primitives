// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>Disposable that invokes a stateful action once.</summary>
/// <typeparam name="TState">The state type.</typeparam>
/// <param name="state">State passed to the dispose action.</param>
/// <param name="dispose">The dispose action.</param>
internal sealed class StateActionDisposable<TState>(TState state, Action<TState> dispose) : IDisposable
{
    /// <summary>State supplied to the dispose action.</summary>
    private readonly TState _state = state;

    /// <summary>Dispose action, cleared after the first invocation.</summary>
    private Action<TState>? _dispose = dispose;

    /// <inheritdoc/>
    public void Dispose() =>
        Interlocked.Exchange(ref _dispose, null)?.Invoke(_state);
}
