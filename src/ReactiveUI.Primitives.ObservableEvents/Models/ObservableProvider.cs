// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>Stores the selected observable provider independently of event extraction.</summary>
internal enum ObservableProvider
{
    /// <summary>No supported observable factory is visible; nothing can be generated.</summary>
    None = 0,

    /// <summary>The lean ReactiveUI.Primitives implementation.</summary>
    Lean = 1,

    /// <summary>The ReactiveUI.Primitives.Reactive implementation.</summary>
    Reactive = 2,

    /// <summary>Standalone System.Reactive.</summary>
    SystemReactive = 3,
}
