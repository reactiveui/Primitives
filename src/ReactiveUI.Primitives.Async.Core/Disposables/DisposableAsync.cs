// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Disposables;

/// <summary>Provides factory methods for creating and working with implementations of <see cref="IAsyncDisposable"/>.</summary>
/// <remarks>Every disposable handed out here runs its delegate at most once, however many times — and from however many
/// threads — it is disposed.</remarks>
public static class DisposableAsync
{
    /// <summary>Gets a shared <see cref="IAsyncDisposable"/> that does nothing when disposed.</summary>
    public static IAsyncDisposable Empty { get; } = new NoopAsyncDisposable();

    /// <summary>Creates a new asynchronous disposable object that invokes the specified delegate when disposed asynchronously.</summary>
    /// <param name="disposeAsync">A delegate that is called to perform asynchronous disposal logic when the returned object is disposed. Cannot be
    /// null.</param>
    /// <returns>An <see cref="IAsyncDisposable"/> instance that invokes the specified delegate when disposed asynchronously.</returns>
    public static IAsyncDisposable Create(Func<ValueTask> disposeAsync)
    {
        ArgumentExceptionHelper.ThrowIfNull(disposeAsync);

        return new DelegateAsyncDisposable(disposeAsync);
    }

    /// <summary>Creates a disposable that passes explicit state to its cleanup delegate.</summary>
    /// <typeparam name="TState">The type of the state passed to the dispose delegate.</typeparam>
    /// <param name="state">The state forwarded to <paramref name="disposeAsync"/> at dispose time.</param>
    /// <param name="disposeAsync">The dispose delegate. Must not be null.</param>
    /// <returns>An <see cref="IAsyncDisposable"/> instance that invokes the specified delegate when disposed.</returns>
    public static IAsyncDisposable Create<TState>(TState state, Func<TState, ValueTask> disposeAsync)
    {
        ArgumentExceptionHelper.ThrowIfNull(disposeAsync);

        return new DelegateAsyncDisposable<TState>(state, disposeAsync);
    }

    /// <summary>An asynchronous disposable that invokes a delegate when disposed.</summary>
    /// <param name="disposeAsync">The delegate invoked to perform asynchronous disposal.</param>
    internal sealed class DelegateAsyncDisposable(Func<ValueTask> disposeAsync) : IAsyncDisposable
    {
        /// <summary>Guard that keeps the delegate to a single invocation (0 = open, 1 = disposed).</summary>
        private int _disposed;

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => Interlocked.Exchange(ref _disposed, 1) == 1 ? default : disposeAsync();
    }

    /// <summary>An asynchronous disposable that invokes a delegate with a stored state when disposed, so the caller's data travels in <typeparamref name="TState"/> instead of a closure.</summary>
    /// <typeparam name="TState">The type of the state passed to the dispose delegate.</typeparam>
    /// <param name="state">The state forwarded to the dispose delegate at dispose time.</param>
    /// <param name="disposeAsync">The delegate invoked to perform asynchronous disposal.</param>
    internal sealed class DelegateAsyncDisposable<TState>(TState state, Func<TState, ValueTask> disposeAsync) : IAsyncDisposable
    {
        /// <summary>Guard that keeps the delegate to a single invocation (0 = open, 1 = disposed).</summary>
        private int _disposed;

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => Interlocked.Exchange(ref _disposed, 1) == 1 ? default : disposeAsync(state);
    }

    /// <summary>An asynchronous disposable that performs no action when disposed.</summary>
    internal sealed class NoopAsyncDisposable : IAsyncDisposable
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
