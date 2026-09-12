// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for converting synchronous disposable objects to asynchronous disposables.</summary>
public static class DisposableAsyncExtensions
{
    /// <summary>Asynchronous-disposal wrapping operators for an <see cref="IDisposable"/> instance.</summary>
    /// <param name="disposable">The <see cref="IDisposable"/> instance to wrap as an <see cref="IAsyncDisposable"/>.</param>
    extension(IDisposable disposable)
    {
        /// <summary>Converts an <see cref="IDisposable"/> instance to an <see cref="IAsyncDisposable"/> wrapper.</summary>
        /// <returns>An <see cref="IAsyncDisposable"/> that disposes the underlying <see cref="IDisposable"/> when disposed
        /// asynchronously.</returns>
        /// <remarks>Disposal runs synchronously on the caller's thread; the returned handle only adapts the shape.</remarks>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "The suffix names the IAsyncDisposable the method returns, not asynchronous work.")]
        public IAsyncDisposable ToDisposableAsync()
        {
            ArgumentExceptionHelper.ThrowIfNull(disposable);

            return new DisposableToDisposableAsync(disposable);
        }
    }

    /// <summary>Presents a synchronous <see cref="IDisposable"/> as an <see cref="IAsyncDisposable"/>, calling <see cref="IDisposable.Dispose"/> inline and completing synchronously.</summary>
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
