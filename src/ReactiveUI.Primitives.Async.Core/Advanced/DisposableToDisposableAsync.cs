// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Presents a synchronous <see cref="IDisposable"/> as an <see cref="IAsyncDisposable"/>, calling <see cref="IDisposable.Dispose"/> inline and completing synchronously.</summary>
[System.Diagnostics.DebuggerDisplay("DisposableToDisposableAsync: Disposable = {_disposable}")]
public sealed class DisposableToDisposableAsync : IAsyncDisposable
{
    /// <summary>The underlying synchronous disposable.</summary>
    private readonly IDisposable _disposable;

    /// <summary>Initializes a new instance of the <see cref="DisposableToDisposableAsync"/> class.</summary>
    /// <param name="disposable">The <see cref="IDisposable"/> instance to wrap for asynchronous disposal.</param>
    public DisposableToDisposableAsync(IDisposable disposable)
    {
        ArgumentExceptionHelper.ThrowIfNull(disposable);
        _disposable = disposable;
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        _disposable.Dispose();
        return default;
    }
}
