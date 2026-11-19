// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Groups eight consecutive observable sources for wide-arity combine-latest validation and coordinator attachment.</summary>
/// <typeparam name="T1">The first source element type.</typeparam>
/// <typeparam name="T2">The second source element type.</typeparam>
/// <typeparam name="T3">The third source element type.</typeparam>
/// <typeparam name="T4">The fourth source element type.</typeparam>
/// <typeparam name="T5">The fifth source element type.</typeparam>
/// <typeparam name="T6">The sixth source element type.</typeparam>
/// <typeparam name="T7">The seventh source element type.</typeparam>
/// <typeparam name="T8">The eighth source element type.</typeparam>
/// <param name="Source1">The first source observable.</param>
/// <param name="Source2">The second source observable.</param>
/// <param name="Source3">The third source observable.</param>
/// <param name="Source4">The fourth source observable.</param>
/// <param name="Source5">The fifth source observable.</param>
/// <param name="Source6">The sixth source observable.</param>
/// <param name="Source7">The seventh source observable.</param>
/// <param name="Source8">The eighth source observable.</param>
[SuppressMessage(
    "Maintainability",
    "SST1472:Signatures should not declare too many parameters",
    Justification = "Groups an octet of wide-arity combine-latest sources into one stack value.")]
internal readonly record struct CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8>(
    IObservable<T1> Source1,
    IObservable<T2> Source2,
    IObservable<T3> Source3,
    IObservable<T4> Source4,
    IObservable<T5> Source5,
    IObservable<T6> Source6,
    IObservable<T7> Source7,
    IObservable<T8> Source8);
