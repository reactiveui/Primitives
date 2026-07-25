// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Avalonia;
using Avalonia.Headless;

namespace ReactiveUI.Primitives.Avalonia.Reactive.Tests;

/// <summary>Provides one process-wide Avalonia headless session with a continuously pumped UI thread.</summary>
internal static class AvaloniaTestSession
{
    /// <summary>Process-wide headless session.</summary>
    private static readonly Lazy<HeadlessUnitTestSession> Session =
        new(
            static () => HeadlessUnitTestSession.StartNew(typeof(Application)),
            LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>Gets the shared headless test session.</summary>
    internal static HeadlessUnitTestSession Instance => Session.Value;
}
