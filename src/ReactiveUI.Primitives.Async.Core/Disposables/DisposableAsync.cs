// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Disposables;

/// <summary>Provides factory methods for creating and working with implementations of <see cref="IAsyncDisposable"/>.</summary>
/// <remarks>This class offers utility members to simplify the creation of asynchronous disposables, such as
/// wrapping a delegate in an <see cref="IAsyncDisposable"/> implementation or providing a no-op disposable instance.
/// All members are thread-safe and can be used to facilitate resource management in asynchronous scenarios.</remarks>
public static class DisposableAsync
{
    /// <summary>Gets an <see cref="IAsyncDisposable"/> instance that performs no action when disposed asynchronously.</summary>
    /// <remarks>Use this property when an <see cref="IAsyncDisposable"/> is required but no disposal logic is
    /// necessary. This can be useful as a default or placeholder implementation.</remarks>
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

    /// <summary>
    /// Creates a new asynchronous disposable that invokes the specified delegate, passing the supplied state, when
    /// disposed asynchronously. Prefer this overload over <see cref="Create(Func{ValueTask})"/> at call sites that
    /// would otherwise capture locals or <c>this</c> in the lambda — the state-carrying overload removes the
    /// closure object and lets the lambda be declared <c>static</c>.
    /// </summary>
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
        /// <summary>A flag indicating whether <see cref="DisposeAsync"/> has already been called (0 = not disposed, 1 = disposed).</summary>
        private int _disposed;

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => Interlocked.Exchange(ref _disposed, 1) == 1 ? default : disposeAsync();
    }

    /// <summary>
    /// An asynchronous disposable that invokes a delegate, passing a stored state, when disposed. The state
    /// indirection avoids a closure allocation when the caller can supply the captured data as <typeparamref
    /// name="TState"/>.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the dispose delegate.</typeparam>
    /// <param name="state">The state forwarded to the dispose delegate at dispose time.</param>
    /// <param name="disposeAsync">The delegate invoked to perform asynchronous disposal.</param>
    internal sealed class DelegateAsyncDisposable<TState>(TState state, Func<TState, ValueTask> disposeAsync) : IAsyncDisposable
    {
        /// <summary>A flag indicating whether <see cref="DisposeAsync"/> has already been called (0 = not disposed, 1 = disposed).</summary>
        private int _disposed;

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => Interlocked.Exchange(ref _disposed, 1) == 1 ? default : disposeAsync(state);
    }

    /// <summary>An asynchronous disposable that performs no action when disposed.</summary>
    internal sealed class NoopAsyncDisposable : IAsyncDisposable
    {
        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;
    }
}
