// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for observable subscription extension methods.</summary>
public class SubscribeExtensionsTests
{
    /// <summary>A consumer that uses the explicit Primitives subscription name with both extension namespaces globally imported.</summary>
    private const string SubscribePrimitivesConsumerWithGlobalUsings = """
        global using System;
        global using ReactiveUI.Primitives;
        global using System.Reactive.Linq;

        public static class Consumer
        {
            public static IDisposable SubscribeWithoutCallbacks(IObservable<Exception> source) =>
                source.SubscribePrimitives();

            public static IDisposable SubscribeNext(IObservable<Exception> source, Action<Exception> onNext) =>
                source.SubscribePrimitives(onNext);

            public static IDisposable SubscribeNextError(
                IObservable<Exception> source,
                Action<Exception> onNext,
                Action<Exception> onError) =>
                source.SubscribePrimitives(onNext, onError);

            public static IDisposable SubscribeNextCompleted(
                IObservable<Exception> source,
                Action<Exception> onNext,
                Action onCompleted) =>
                source.SubscribePrimitives(onNext, onCompleted);

            public static IDisposable SubscribeAll(
                IObservable<Exception> source,
                Action<Exception> onNext,
                Action<Exception> onError,
                Action onCompleted) =>
                source.SubscribePrimitives(onNext, onError, onCompleted);
        }
        """;

    /// <summary>A consumer that still uses the plain subscription name with both extension namespaces in scope.</summary>
    private const string AmbiguousSubscribeConsumer = """
        global using System;
        global using ReactiveUI.Primitives;
        global using System.Reactive.Linq;

        public static class Consumer
        {
            public static IDisposable SubscribeNext(IObservable<Exception> source, Action<Exception> onNext) =>
                source.Subscribe(onNext);
        }
        """;

    /// <summary>Verifies every callback shape preserves value and completion delivery.</summary>
    /// <param name="overload">The subscription callback shape to use.</param>
    /// <param name="expectedValues">The number of value callbacks expected.</param>
    /// <param name="expectedCompletions">The number of completion callbacks expected.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments("none", 0, 0)]
    [Arguments("next", 1, 0)]
    [Arguments("next-error", 1, 0)]
    [Arguments("next-completed", 1, 1)]
    [Arguments("all", 1, 1)]
    public async Task SubscribePrimitivesPreservesCallbackShapes(
        string overload,
        int expectedValues,
        int expectedCompletions)
    {
        using Signal<int> source = new();
        List<int> values = [];
        List<Exception> errors = [];
        var completions = 0;
        using var subscription = overload switch
        {
            "none" => source.SubscribePrimitives(),
            "next" => source.SubscribePrimitives(values.Add),
            "next-error" => source.SubscribePrimitives(values.Add, errors.Add),
            "next-completed" => source.SubscribePrimitives(values.Add, () => completions++),
            _ => source.SubscribePrimitives(values.Add, errors.Add, () => completions++),
        };

        source.OnNext(1);
        source.OnCompleted();
        await Assert.That(values.Count).IsEqualTo(expectedValues);
        await Assert.That(completions).IsEqualTo(expectedCompletions);
        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Verifies exception values stay distinct from terminal errors and disposal removes the observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribePrimitivesHandlesExceptionValuesAndDisposal()
    {
        using Signal<Exception> source = new();
        var value = new InvalidOperationException("value");
        var error = new InvalidOperationException("error");
        List<Exception> values = [];
        List<Exception> errors = [];
        var subscription = source.SubscribePrimitives(values.Add, errors.Add);

        source.OnNext(value);
        await Assert.That(values.Single()).IsSameReferenceAs(value);
        await Assert.That(errors).IsEmpty();

        subscription.Dispose();
        await Assert.That(source.HasObservers).IsFalse();
        source.OnError(error);
        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Verifies the explicit Primitives subscription name avoids System.Reactive Subscribe ambiguity.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles("Builds metadata references from loaded assembly locations.")]
    public async Task SubscribePrimitivesCallbacksCompileWithSystemReactiveAndPrimitivesGlobalUsings()
    {
        var (compilation, syntaxTree, errors) =
            ConsumerCompilation.Compile(SubscribePrimitivesConsumerWithGlobalUsings);

        await Assert.That(errors).IsEmpty();
        var symbol = ConsumerCompilation.ResolveInvocation(compilation, syntaxTree, "SubscribePrimitives");

        await Assert.That(symbol?.ContainingNamespace.ToDisplayString()).IsEqualTo("ReactiveUI.Primitives");
        await Assert.That(symbol?.ContainingType.Name).IsEqualTo(nameof(SubscribeExtensions));
    }

    /// <summary>Verifies the plain Subscribe name stays ambiguous, which is what the explicit name exists to avoid.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles("Builds metadata references from loaded assembly locations.")]
    public async Task SubscribeRemainsAmbiguousWithSystemReactiveInScope()
    {
        var (_, _, errors) = ConsumerCompilation.Compile(AmbiguousSubscribeConsumer);

        await Assert.That(errors.Length).IsEqualTo(1);
        await Assert.That(errors[0]).Contains("CS0121");
        await Assert.That(errors[0]).Contains("System.ObservableExtensions.Subscribe");
    }
}
