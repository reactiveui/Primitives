// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>The observable implementation the generated code is written against.</summary>
/// <remarks>
/// Resolved once from the consumer's references and flowed into emission as a value, so that the far more expensive
/// event extraction does not have to re-run when only the reference set moves.
/// </remarks>
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
