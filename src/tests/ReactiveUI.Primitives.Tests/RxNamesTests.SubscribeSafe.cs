// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;
using PrimitivesLinqExtensions = ReactiveUI.Primitives.LinqExtensions;

namespace ReactiveUI.Primitives.Tests;

/// <summary>SubscribeSafe-specific Rx-name compatibility tests.</summary>
public partial class RxNamesTests
{
    /// <summary>The string value used by nullable object subscription tests.</summary>
    private const string SubscribeSafeValue = "value";

    /// <summary>Verifies fluent <c>SubscribeSafe</c> accepts nullable object values with Rx imports present.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeFluentAcceptsNullableObjectHandlerWithRxImports()
    {
        var source = Observable.Concat(
            Observable.Return<object?>(null),
            Observable.Return<object?>(SubscribeSafeValue));
        List<object?> values = [];
        Exception? observed = null;

        void OnNext(object? value) => values.Add(value);

        using var subscription = source.SubscribeSafe(OnNext, error => observed = error);

        await Assert.That(values.SequenceEqual((object?[])[null, SubscribeSafeValue])).IsTrue();
        await Assert.That(observed).IsNull();
    }

    /// <summary>Verifies static alias <c>SubscribeSafe</c> accepts nullable object values with Rx imports present.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeStaticAliasAcceptsNullableObjectHandlerWithRxImports()
    {
        var source = Signal.FromEnumerable<object?>([null, SubscribeSafeValue]);
        List<object?> values = [];
        Exception? observed = null;

        void OnNext(object? value) => values.Add(value);

        using var subscription = PrimitivesLinqExtensions.SubscribeSafe(source, OnNext, error => observed = error);

        await Assert.That(values.SequenceEqual((object?[])[null, SubscribeSafeValue])).IsTrue();
        await Assert.That(observed).IsNull();
    }

    /// <summary>Verifies static alias <c>SubscribeSafe</c> preserves non-nullable reference handlers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeStaticAliasAcceptsNonNullableObjectHandlerWithRxImports()
    {
        var source = Signal.FromEnumerable([SubscribeSafeValue]);
        List<string> values = [];
        Exception? observed = null;

        void OnNext(string value) => values.Add(value);

        using var subscription = PrimitivesLinqExtensions.SubscribeSafe(source, OnNext, error => observed = error);

        await Assert.That(values.SequenceEqual([SubscribeSafeValue])).IsTrue();
        await Assert.That(observed).IsNull();
    }

    /// <summary>Verifies fluent <c>SubscribeSafe</c> accepts nullable value types with Rx imports present.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeFluentAcceptsNullableValueTypeHandlerWithRxImports()
    {
        var source = Signal.FromEnumerable<int?>([null, One, null, Two]);
        List<int?> values = [];
        Exception? observed = null;

        void OnNext(int? value) => values.Add(value);

        using var subscription = source.SubscribeSafe(OnNext, error => observed = error);

        await Assert.That(values.SequenceEqual((int?[])[null, One, null, Two])).IsTrue();
        await Assert.That(observed).IsNull();
    }

    /// <summary>Verifies static alias <c>SubscribeSafe</c> accepts nullable value types with Rx imports present.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeStaticAliasAcceptsNullableValueTypeHandlerWithRxImports()
    {
        var source = Signal.FromEnumerable<int?>([null, One, null, Two]);
        List<int?> values = [];
        Exception? observed = null;

        void OnNext(int? value) => values.Add(value);

        using var subscription = PrimitivesLinqExtensions.SubscribeSafe(source, OnNext, error => observed = error);

        await Assert.That(values.SequenceEqual((int?[])[null, One, null, Two])).IsTrue();
        await Assert.That(observed).IsNull();
    }

    /// <summary>Verifies static alias <c>SubscribeSafe</c> accepts nullable observer overloads with Rx imports present.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeStaticAliasAcceptsNullableObserverOverloadsWithRxImports()
    {
        var referenceSource = Signal.FromEnumerable<object?>([null, SubscribeSafeValue]);
        List<object?> referenceValues = [];
        var referenceObserver = Witness.Create<object>(
            referenceValues.Add,
            static _ => { });

        using var referenceSubscription = PrimitivesLinqExtensions.SubscribeSafe(referenceSource, referenceObserver);

        var valueSource = Signal.FromEnumerable<int?>([null, One, null, Two]);
        List<int?> valueValues = [];
        var valueObserver = Witness.Create<int?>(
            valueValues.Add,
            static _ => { });

        using var valueSubscription = PrimitivesLinqExtensions.SubscribeSafe(valueSource, valueObserver);

        await Assert.That(referenceValues.SequenceEqual((object?[])[null, SubscribeSafeValue])).IsTrue();
        await Assert.That(valueValues.SequenceEqual((int?[])[null, One, null, Two])).IsTrue();
    }

    /// <summary>Verifies static alias <c>SubscribeSafe</c> accepts nullable completion callback overloads with Rx imports present.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeStaticAliasAcceptsNullableCompletionCallbackOverloadsWithRxImports()
    {
        var referenceSource = Signal.FromEnumerable<object?>([null, SubscribeSafeValue]);
        List<object?> referenceValues = [];
        Exception? referenceObserved = null;
        var referenceCompleted = 0;

        void OnReferenceNext(object? value) => referenceValues.Add(value);

        using var referenceSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            referenceSource,
            OnReferenceNext,
            error => referenceObserved = error,
            () => referenceCompleted++);

        var valueSource = Signal.FromEnumerable<int?>([null, One, null, Two]);
        List<int?> valueValues = [];
        Exception? valueObserved = null;
        var valueCompleted = 0;

        void OnValueNext(int? value) => valueValues.Add(value);

        using var valueSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            valueSource,
            OnValueNext,
            error => valueObserved = error,
            () => valueCompleted++);

        await Assert.That(referenceValues.SequenceEqual((object?[])[null, SubscribeSafeValue])).IsTrue();
        await Assert.That(referenceObserved).IsNull();
        await Assert.That(referenceCompleted).IsEqualTo(1);
        await Assert.That(valueValues.SequenceEqual((int?[])[null, One, null, Two])).IsTrue();
        await Assert.That(valueObserved).IsNull();
        await Assert.That(valueCompleted).IsEqualTo(1);
    }

    /// <summary>Verifies static alias <c>SubscribeSafe</c> accepts nullable error-only overloads with Rx imports present.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeStaticAliasAcceptsNullableErrorOnlyOverloadsWithRxImports()
    {
        InvalidOperationException referenceError = new("reference");
        InvalidOperationException valueError = new("value");
        Exception? observedReferenceError = null;
        Exception? observedValueError = null;

        using var referenceSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            Signal.Fail<object?>(referenceError),
            error => observedReferenceError = error);
        using var valueSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            Signal.Fail<int?>(valueError),
            error => observedValueError = error);

        await Assert.That(observedReferenceError).IsSameReferenceAs(referenceError);
        await Assert.That(observedValueError).IsSameReferenceAs(valueError);
    }

    /// <summary>Verifies static alias <c>SubscribeSafe</c> accepts nullable terminal completion overloads with Rx imports present.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeStaticAliasAcceptsNullableTerminalCompletionOverloadsWithRxImports()
    {
        Exception? referenceObserved = null;
        Exception? valueObserved = null;
        var referenceCompleted = 0;
        var valueCompleted = 0;

        using var referenceSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            Signal.FromEnumerable<object?>([null, SubscribeSafeValue]),
            error => referenceObserved = error,
            () => referenceCompleted++);
        using var valueSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            Signal.FromEnumerable<int?>([null, One, null, Two]),
            error => valueObserved = error,
            () => valueCompleted++);

        await Assert.That(referenceObserved).IsNull();
        await Assert.That(referenceCompleted).IsEqualTo(1);
        await Assert.That(valueObserved).IsNull();
        await Assert.That(valueCompleted).IsEqualTo(1);
    }
}
