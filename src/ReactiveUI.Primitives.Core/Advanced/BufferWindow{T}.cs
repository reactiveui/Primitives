// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>One open buffer window and the number of values written into it.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="Items">The window storage, sized to the window length.</param>
/// <param name="Count">The number of values written so far.</param>
internal readonly record struct BufferWindow<T>(T[] Items, int Count);
