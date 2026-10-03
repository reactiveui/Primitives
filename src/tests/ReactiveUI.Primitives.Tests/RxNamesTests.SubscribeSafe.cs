// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Reactive;
using System.Reactive.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;
using PrimitivesLinqExtensions = ReactiveUI.Primitives.LinqExtensions;

namespace ReactiveUI.Primitives.Tests;

/// <summary>SubscribeSafe-specific Rx-name compatibility tests.</summary>
public partial class RxNamesTests
{
    /// <summary>The number of completing subscriptions made by the SubscribeSafeErrors dispatch test.</summary>
    private const int CompletingSubscriptionCount = 3;

    /// <summary>The string value used by nullable object subscription tests.</summary>
    private const string SubscribeSafeValue = "value";

    /// <summary>A consumer that uses the observer-taking SubscribeSafe with System.Reactive also in scope.</summary>
    private const string AmbiguousSubscribeSafeConsumer = """
        using System;
        using ReactiveUI.Primitives;

        public static class Consumer
        {
            public static IDisposable Subscribe(IObservable<int> source, IObserver<int> observer) =>
                source.SubscribeSafe(observer);
        }
        """;

    /// <summary>A consumer that selects the Primitives observer overload by its explicit name.</summary>
    private const string SubscribeSafePrimitivesConsumer = """
        using System;
        using ReactiveUI.Primitives;

        public static class Consumer
        {
            public static IDisposable Subscribe(IObservable<int> source, IObserver<int> observer) =>
                source.SubscribeSafePrimitives(observer);
        }
        """;

    /// <summary>A C# 12 consumer covering non-nullable and nullable value-type static calls.</summary>
    private const string CSharp12ValueTypeConsumer = """
        using System;
        using System.Reactive;
        using PrimitivesLinqExtensions = ReactiveUI.Primitives.LinqExtensions;

        public static class Consumer
        {
            public static IDisposable SubscribeObserver(IObservable<Unit> source, IObserver<Unit> observer) =>
                PrimitivesLinqExtensions.SubscribeSafe(source, observer);

            public static IDisposable SubscribeCallbacks(
                IObservable<Unit> source,
                Action<Unit> onNext,
                Action<Exception> onError) =>
                PrimitivesLinqExtensions.SubscribeSafe(source, onNext, onError);

            public static IDisposable SubscribeCallbacks(
                IObservable<Unit> source,
                Action<Unit> onNext,
                Action<Exception> onError,
                Action onCompleted) =>
                PrimitivesLinqExtensions.SubscribeSafe(source, onNext, onError, onCompleted);

            public static IDisposable SubscribeNext(IObservable<Unit> source, Action<Unit> onNext) =>
                PrimitivesLinqExtensions.SubscribeSafe(source, onNext);

            public static IDisposable SubscribeError(IObservable<Unit> source, Action<Exception> onError) =>
                PrimitivesLinqExtensions.SubscribeSafeErrors(source, onError);

            public static IDisposable SubscribeTerminal(
                IObservable<Unit> source,
                Action<Exception> onError,
                Action onCompleted) =>
                PrimitivesLinqExtensions.SubscribeSafeErrors(source, onError, onCompleted);

            public static IDisposable SubscribeNullableObserver(
                IObservable<int?> source,
                IObserver<int?> observer) =>
                PrimitivesLinqExtensions.SubscribeSafe(source, observer);

            public static IDisposable SubscribeNullableCallbacks(
                IObservable<int?> source,
                Action<int?> onNext,
                Action<Exception> onError) =>
                PrimitivesLinqExtensions.SubscribeSafe(source, onNext, onError);

            public static IDisposable SubscribeNullableCallbacks(
                IObservable<int?> source,
                Action<int?> onNext,
                Action<Exception> onError,
                Action onCompleted) =>
                PrimitivesLinqExtensions.SubscribeSafe(source, onNext, onError, onCompleted);

            public static IDisposable SubscribeNullableNext(IObservable<int?> source, Action<int?> onNext) =>
                PrimitivesLinqExtensions.SubscribeSafe(source, onNext);

            public static IDisposable SubscribeNullableError(
                IObservable<int?> source,
                Action<Exception> onError) =>
                PrimitivesLinqExtensions.SubscribeSafeErrors(source, onError);

            public static IDisposable SubscribeNullableTerminal(
                IObservable<int?> source,
                Action<Exception> onError,
                Action onCompleted) =>
                PrimitivesLinqExtensions.SubscribeSafeErrors(source, onError, onCompleted);
        }
        """;

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

    /// <summary>Verifies static alias <c>SubscribeSafe</c> accepts every non-nullable value-type overload.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeStaticAliasAcceptsNonNullableValueTypeOverloads()
    {
        var source = Observable.Return(Unit.Default);
        List<Unit> values = [];
        Exception? observed = null;
        var completed = 0;

        void OnError(Exception error) => observed = error;

        using var callbackSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            source,
            values.Add,
            OnError,
            (byte)0);
        using var completionSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            source,
            values.Add,
            OnError,
            () => completed++,
            (byte)0);
        using var observerSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            source,
            Observer.Create<Unit>(values.Add, OnError),
            (byte)0);

        InvalidOperationException expected = new("expected");
        using var errorSubscription = PrimitivesLinqExtensions.SubscribeSafeErrors(
            Observable.Throw<Unit>(expected),
            OnError,
            (byte)0);
        using var terminalSubscription = PrimitivesLinqExtensions.SubscribeSafeErrors(
            source,
            OnError,
            () => completed++,
            (byte)0);

        await Assert.That(values.SequenceEqual([Unit.Default, Unit.Default, Unit.Default])).IsTrue();
        await Assert.That(observed).IsSameReferenceAs(expected);
        await Assert.That(completed).IsEqualTo(Two);
    }

    /// <summary>Verifies C# 12 consumers can statically call non-nullable and nullable value-type forms without ambiguity.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles("Builds metadata references from loaded assembly locations.")]
    public async Task SubscribeSafeStaticAliasCompilesForCSharp12ValueTypes()
    {
        var references = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!.ToString()!
            .Split(Path.PathSeparator)
            .Select(static path => MetadataReference.CreateFromFile(path))
            .Cast<MetadataReference>()
            .ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(Unit).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(RxVoid).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(PrimitivesLinqExtensions).Assembly.Location));

        var syntaxTree = CSharpSyntaxTree.ParseText(
            CSharp12ValueTypeConsumer,
            CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.CSharp12));
        var compilation = CSharpCompilation.Create(
            "SubscribeSafeCSharp12Consumer",
            [syntaxTree],
            references,
            new(OutputKind.DynamicallyLinkedLibrary));
        var errors = compilation.GetDiagnostics()
            .Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .Select(static diagnostic => diagnostic.ToString())
            .ToArray();

        await Assert.That(errors).IsEmpty();
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

        using var referenceSubscription = PrimitivesLinqExtensions.SubscribeSafeErrors(
            Signal.Fail<object?>(referenceError),
            error => observedReferenceError = error);
        using var valueSubscription = PrimitivesLinqExtensions.SubscribeSafeErrors(
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

        using var referenceSubscription = PrimitivesLinqExtensions.SubscribeSafeErrors(
            Signal.FromEnumerable<object?>([null, SubscribeSafeValue]),
            error => referenceObserved = error,
            () => referenceCompleted++);
        using var valueSubscription = PrimitivesLinqExtensions.SubscribeSafeErrors(
            Signal.FromEnumerable<int?>([null, One, null, Two]),
            error => valueObserved = error,
            () => valueCompleted++);

        await Assert.That(referenceObserved).IsNull();
        await Assert.That(referenceCompleted).IsEqualTo(1);
        await Assert.That(valueObserved).IsNull();
        await Assert.That(valueCompleted).IsEqualTo(1);
    }

    /// <summary>Verifies a method group passed alone to <c>SubscribeSafe</c> receives the values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafe_MethodGroupAlone_ReceivesValues()
    {
        List<int> values = [];

        using var subscription = Signal.FromEnumerable([One, Two]).SubscribeSafe(values.Add);

        await Assert.That(values.SequenceEqual([One, Two])).IsTrue();
    }

    /// <summary>Verifies a lambda passed alone to <c>SubscribeSafe</c> receives the values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafe_LambdaAlone_ReceivesValues()
    {
        List<int> values = [];

        using var subscription = Signal.FromEnumerable([One, Two]).SubscribeSafe(x =>
        {
            values.Add(x);
        });

        await Assert.That(values.SequenceEqual([One, Two])).IsTrue();
    }

    /// <summary>Verifies a discarding lambda passed alone to <c>SubscribeSafe</c> counts the values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafe_DiscardLambdaAlone_CountsValues()
    {
        int[] emitted = [One, Two, Three];
        var count = 0;

        using var subscription = Signal.FromEnumerable(emitted).SubscribeSafe(_ => count++);

        await Assert.That(count).IsEqualTo(emitted.Length);
    }

    /// <summary>Verifies the onNext-only form rethrows a source error because no error handler exists.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafe_LoneOnNextWithSourceError_RethrowsTheError()
    {
        InvalidOperationException expected = new(Boom);
        var source = Signal.Fail<int>(expected);

        var thrown = Assert.Throws<InvalidOperationException>(() => source.SubscribeSafe(static _ => { }));

        await Assert.That(thrown).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies the static onNext-only forms deliver values for reference, nullable value and value types.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafe_StaticLoneOnNext_ReceivesValuesForEveryDispatchVariant()
    {
        List<string?> referenceValues = [];
        List<int?> nullableValues = [];
        List<int> valueTypeValues = [];

        using var referenceSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            Signal.FromEnumerable<string?>([SubscribeSafeValue]),
            referenceValues.Add);
        using var nullableSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            Signal.FromEnumerable<int?>([null, One]),
            nullableValues.Add);
        using var valueTypeSubscription = PrimitivesLinqExtensions.SubscribeSafe(
            Signal.FromEnumerable([One, Two]),
            valueTypeValues.Add);

        await Assert.That(referenceValues.SequenceEqual([SubscribeSafeValue])).IsTrue();
        await Assert.That(nullableValues.SequenceEqual([null, One])).IsTrue();
        await Assert.That(valueTypeValues.SequenceEqual([One, Two])).IsTrue();
    }

    /// <summary>Verifies the fluent <c>SubscribeSafeErrors</c> forms receive the error and complete.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeErrors_Fluent_ReceivesErrorAndCompletion()
    {
        InvalidOperationException expected = new(Boom);
        Exception? observed = null;
        var completed = 0;

        using var errorSubscription = Signal.Fail<int>(expected).SubscribeSafeErrors(error => observed = error);
        using var completionSubscription = Signal.FromEnumerable([One]).SubscribeSafeErrors(
            static _ => { },
            () => completed++);

        await Assert.That(observed).IsSameReferenceAs(expected);
        await Assert.That(completed).IsEqualTo(One);
    }

    /// <summary>Verifies the static <c>SubscribeSafeErrors</c> forms receive the error and complete for every dispatch variant.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeErrors_Static_ReceivesErrorAndCompletionForEveryDispatchVariant()
    {
        InvalidOperationException expected = new(Boom);
        Exception? referenceError = null;
        Exception? nullableError = null;
        Exception? valueTypeError = null;
        var completed = 0;

        using var referenceSubscription = PrimitivesLinqExtensions.SubscribeSafeErrors(
            Signal.Fail<string?>(expected),
            error => referenceError = error);
        using var nullableSubscription = PrimitivesLinqExtensions.SubscribeSafeErrors(
            Signal.Fail<int?>(expected),
            error => nullableError = error);
        using var valueTypeSubscription = PrimitivesLinqExtensions.SubscribeSafeErrors(
            Signal.Fail<int>(expected),
            error => valueTypeError = error);
        using var referenceCompleted = PrimitivesLinqExtensions.SubscribeSafeErrors(
            Signal.FromEnumerable<string?>([SubscribeSafeValue]),
            static _ => { },
            () => completed++);
        using var nullableCompleted = PrimitivesLinqExtensions.SubscribeSafeErrors(
            Signal.FromEnumerable<int?>([One]),
            static _ => { },
            () => completed++);
        using var valueTypeCompleted = PrimitivesLinqExtensions.SubscribeSafeErrors(
            Signal.FromEnumerable([One]),
            static _ => { },
            () => completed++);

        await Assert.That(referenceError).IsSameReferenceAs(expected);
        await Assert.That(nullableError).IsSameReferenceAs(expected);
        await Assert.That(valueTypeError).IsSameReferenceAs(expected);
        await Assert.That(completed).IsEqualTo(CompletingSubscriptionCount);
    }

    /// <summary>Verifies the explicit Primitives name delivers the same notifications as the observer overload.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafePrimitivesDeliversTheSameNotificationsAsSubscribeSafe()
    {
        using Signal<int> source = new();
        RecordingWitness<int> witness = new();
        var subscription = source.SubscribeSafePrimitives(witness);

        source.OnNext(One);
        source.OnNext(Two);
        source.OnCompleted();

        await Assert.That(witness.Values.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
        await Assert.That(witness.Errors).IsEmpty();

        subscription.Dispose();
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>
    /// Verifies the explicit Primitives name resolves where the observer-taking <c>SubscribeSafe</c> is ambiguous.
    /// System.Reactive declares a competing observer overload in the <c>System</c> namespace, which an implicit
    /// <c>using System;</c> brings along.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles("Builds metadata references from loaded assembly locations.")]
    public async Task SubscribeSafePrimitivesResolvesWhereSubscribeSafeIsAmbiguous()
    {
        var (_, _, ambiguousErrors) = ConsumerCompilation.Compile(AmbiguousSubscribeSafeConsumer);

        await Assert.That(ambiguousErrors.Length).IsEqualTo(1);
        await Assert.That(ambiguousErrors[0]).Contains("CS0121");
        await Assert.That(ambiguousErrors[0]).Contains("System.ObservableExtensions.SubscribeSafe");

        var (compilation, syntaxTree, errors) = ConsumerCompilation.Compile(SubscribeSafePrimitivesConsumer);

        await Assert.That(errors).IsEmpty();
        var symbol = ConsumerCompilation.ResolveInvocation(compilation, syntaxTree, "SubscribeSafePrimitives");

        await Assert.That(symbol?.ContainingType.Name).IsEqualTo(nameof(LinqExtensions));
    }
}
