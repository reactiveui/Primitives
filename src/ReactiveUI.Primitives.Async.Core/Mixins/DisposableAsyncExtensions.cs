// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for converting synchronous disposable objects to asynchronous disposables.</summary>
/// <remarks>These extension methods enable the use of existing IDisposable implementations in asynchronous
/// disposal scenarios by wrapping them as IAsyncDisposable. This is useful when working with APIs that require
/// asynchronous disposal, but only a synchronous Dispose method is available.</remarks>
public static class DisposableAsyncExtensions
{
    /// <summary>Asynchronous-disposal wrapping operators for an <see cref="IDisposable"/> instance.</summary>
    /// <param name="this">The <see cref="IDisposable"/> instance to wrap as an <see cref="IAsyncDisposable"/>.</param>
    extension(IDisposable @this)
    {
        /// <summary>Converts an <see cref="IDisposable"/> instance to an <see cref="IAsyncDisposable"/> wrapper.</summary>
        /// <remarks>The returned <see cref="IAsyncDisposable"/> invokes the synchronous <see
        /// cref="IDisposable.Dispose"/> method when <see cref="IAsyncDisposable.DisposeAsync"/> is called. This is useful
        /// for integrating synchronous disposables into asynchronous disposal patterns.</remarks>
        /// <returns>An <see cref="IAsyncDisposable"/> that disposes the underlying <see cref="IDisposable"/> when disposed
        /// asynchronously.</returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "This is an existing method")]
        public IAsyncDisposable ToDisposableAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);

            return new DisposableToDisposableAsync(@this);
        }
    }

    /// <summary>
    /// Provides an implementation of <see cref="IAsyncDisposable"/> that wraps a synchronous <see cref="IDisposable"/>
    /// instance, enabling it to be used in asynchronous disposal scenarios.
    /// </summary>
    /// <remarks>This class allows objects that implement <see cref="IDisposable"/> but not <see
    /// cref="IAsyncDisposable"/> to be used in contexts that require asynchronous disposal. The asynchronous dispose
    /// operation is performed by invoking the synchronous <see cref="IDisposable.Dispose"/> method; no actual
    /// asynchronous work is performed.</remarks>
    /// <param name="disposable">The <see cref="IDisposable"/> instance to be wrapped for asynchronous disposal. Cannot be null.</param>
    internal sealed class DisposableToDisposableAsync(IDisposable disposable) : IAsyncDisposable
    {
        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            disposable.Dispose();
            return default;
        }
    }
}
