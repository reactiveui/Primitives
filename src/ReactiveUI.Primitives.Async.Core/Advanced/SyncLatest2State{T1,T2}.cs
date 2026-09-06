// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Bundles the source observables for the arity-2 <c>SyncLatest</c> operator.</summary>
/// <typeparam name="T1">Element type of source 1.</typeparam>
/// <typeparam name="T2">Element type of source 2.</typeparam>
/// <param name="Source1">Source observable 1.</param>
/// <param name="Source2">Source observable 2.</param>
[System.Diagnostics.DebuggerDisplay("SyncLatest2State: Source1 = {Source1}, Source2 = {Source2}")]
public readonly record struct SyncLatest2State<T1, T2>(
    IObservableAsync<T1> Source1,
    IObservableAsync<T2> Source2);
