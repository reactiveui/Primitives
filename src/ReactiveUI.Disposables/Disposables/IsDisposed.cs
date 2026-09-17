// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>An <see cref="IDisposable"/> that reports whether it has been disposed.</summary>
/// <seealso cref="IDisposable" />
public interface IsDisposed : IDisposable
{
    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    bool IsDisposed { get; }
}
