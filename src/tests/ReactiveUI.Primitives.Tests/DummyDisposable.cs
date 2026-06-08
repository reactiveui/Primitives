// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Provides a reusable disposable test instance.</summary>
internal sealed class DummyDisposable : IDisposable
{
    /// <summary>Gets the shared disposable instance.</summary>
    public static DummyDisposable Instance { get; } = new();

    /// <inheritdoc/>
    public void Dispose()
    {
    }
}
