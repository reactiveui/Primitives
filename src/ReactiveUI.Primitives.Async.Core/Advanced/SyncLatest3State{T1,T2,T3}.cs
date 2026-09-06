// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Bundles the source observables for the arity-3 <c>SyncLatest</c> operator.</summary>
/// <typeparam name="T1">Element type of source 1.</typeparam>
/// <typeparam name="T2">Element type of source 2.</typeparam>
/// <typeparam name="T3">Element type of source 3.</typeparam>
/// <param name="Source1">Source observable 1.</param>
/// <param name="Source2">Source observable 2.</param>
/// <param name="Source3">Source observable 3.</param>
[System.Diagnostics.DebuggerDisplay("SyncLatest3State: Source1 = {Source1}, Source2 = {Source2}, Source3 = {Source3}")]
public readonly record struct SyncLatest3State<T1, T2, T3>(
    IObservableAsync<T1> Source1,
    IObservableAsync<T2> Source2,
    IObservableAsync<T3> Source3);
