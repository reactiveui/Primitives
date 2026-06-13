// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>A signal that is both an observer and observable of the same type.</summary>
/// <typeparam name="T">The Type.</typeparam>
public interface ISignal<T> : ISignal<T, T>;
