// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Bundles the source observables for the arity-7 <c>SyncLatest</c> operator.</summary>
/// <typeparam name="T1">Element type of source 1.</typeparam>
/// <typeparam name="T2">Element type of source 2.</typeparam>
/// <typeparam name="T3">Element type of source 3.</typeparam>
/// <typeparam name="T4">Element type of source 4.</typeparam>
/// <typeparam name="T5">Element type of source 5.</typeparam>
/// <typeparam name="T6">Element type of source 6.</typeparam>
/// <typeparam name="T7">Element type of source 7.</typeparam>
/// <param name="Source1">Source observable 1.</param>
/// <param name="Source2">Source observable 2.</param>
/// <param name="Source3">Source observable 3.</param>
/// <param name="Source4">Source observable 4.</param>
/// <param name="Source5">Source observable 5.</param>
/// <param name="Source6">Source observable 6.</param>
/// <param name="Source7">Source observable 7.</param>
public readonly record struct SyncLatest7State<T1, T2, T3, T4, T5, T6, T7>(
    IObservableAsync<T1> Source1,
    IObservableAsync<T2> Source2,
    IObservableAsync<T3> Source3,
    IObservableAsync<T4> Source4,
    IObservableAsync<T5> Source5,
    IObservableAsync<T6> Source6,
    IObservableAsync<T7> Source7);
