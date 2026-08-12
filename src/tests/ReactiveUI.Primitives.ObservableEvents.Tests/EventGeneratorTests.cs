// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using DisposableScope = ReactiveUI.Primitives.Disposables.Scope;
using LeanSignal = ReactiveUI.Primitives.Signals.Signal;
using ReactiveRxVoid = ReactiveUI.Primitives.RxVoid;
using ReactiveSignal = ReactiveUI.Primitives.Reactive.Signals.Signal;
using ReactiveUnit = System.Reactive.Unit;

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies observable-event source generation through in-memory Roslyn compilations.</summary>
public sealed partial class EventGeneratorTests
{
    /// <summary>The diagnostic reported when no supported observable provider is referenced.</summary>
    private const string NoProviderDiagnosticId = "RXOE001";

    /// <summary>The diagnostic reported when a requested type has no supported accessible events.</summary>
    private const string NoEventsDiagnosticId = "RXOE002";

    /// <summary>The diagnostic reported for an event whose delegate signature cannot be adapted.</summary>
    private const string UnsupportedSignatureDiagnosticId = "RXOE003";

    /// <summary>The activation API source emitted during generator post-initialization.</summary>
    private const string ActivationHintName = "ObservableEvents.Activation.g.cs";

    /// <summary>The suffix applied to an instance wrapper hint.</summary>
    private const string InstanceHintSuffix = ".Instance.g.cs";

    /// <summary>The suffix applied to a static wrapper hint.</summary>
    private const string StaticHintSuffix = ".Static.g.cs";

    /// <summary>The number of deliberately unsupported events in the combined signature test.</summary>
    private const int UnsupportedEventCount = 4;

    /// <summary>The number of distinct wrappers expected from the sanitization-collision regression.</summary>
    private const int SanitizationCollisionWrapperCount = 2;

    /// <summary>Source that exercises a conventional instance event.</summary>
    private const string InstanceEventSource = """
        using System;
        using ReactiveUI.Primitives.ObservableEvents;

        namespace Samples;

        public sealed class EventSource
        {
            public event Action? Empty;

            public event EventHandler<EventArgs>? Changed;

            public void Raise() => Changed?.Invoke(this, EventArgs.Empty);
        }

        public static class Consumer
        {
            public static IObservable<EventArgs> Observe(EventSource source) => source.Events().Changed;
        }
        """;

    /// <summary>Controls which optional observable provider assemblies are visible to Roslyn.</summary>
    private enum ProviderMode
    {
        /// <summary>No observable implementation beyond the BCL contracts.</summary>
        None,

        /// <summary>The lean ReactiveUI.Primitives implementation.</summary>
        Lean,

        /// <summary>The ReactiveUI.Primitives.Reactive implementation.</summary>
        Reactive,

        /// <summary>Both ReactiveUI.Primitives implementations, with lean expected to win.</summary>
        LeanAndReactive,

        /// <summary>Standalone System.Reactive.</summary>
        SystemReactive
    }

    /// <summary>Verifies lean Primitives is preferred and produces compiling instance-event output.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorUsesLeanPrimitivesProviderWhenAvailable()
    {
        var result = RunGenerator(InstanceEventSource, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("ReactiveUI.Primitives.Signals.Signal.Create");
        await Assert.That(result.GeneratedText).Contains("ReactiveUI.Primitives.Disposables.Scope.Create");
        await Assert.That(result.GeneratedText).DoesNotContain("System.Reactive.Linq.Observable.Create");
    }

    /// <summary>Verifies the Reactive package provider produces compiling output.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorUsesPrimitivesReactiveProviderWhenLeanIsAbsent()
    {
        var result = RunGenerator(InstanceEventSource, ProviderMode.Reactive);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("ReactiveUI.Primitives.Reactive.Signals.Signal.Create");
    }

    /// <summary>Verifies standalone System.Reactive consumers require no Primitives reference.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorUsesStandaloneSystemReactiveProvider()
    {
        var result = RunGenerator(InstanceEventSource, ProviderMode.SystemReactive);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("System.Reactive.Linq.Observable.Create");
        await Assert.That(result.GeneratedText).DoesNotContain("ReactiveUI.Primitives.Signals.Signal.Create");
    }

    /// <summary>Verifies a missing provider produces one actionable diagnostic and no broken wrapper.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorReportsNoProviderWithoutEmittingEventWrappers()
    {
        var result = RunGenerator(InstanceEventSource, ProviderMode.None);

        await Assert.That(result.Diagnostics.Any(IsDiagnostic(NoProviderDiagnosticId))).IsTrue();
        await Assert.That(result.GeneratedHintNames.Any(IsEventWrapperHint)).IsFalse();
    }

    /// <summary>Verifies instance and static requests generate independently usable observable wrappers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsInstanceAndStaticEvents()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives.ObservableEvents;

                              [assembly: GenerateStaticEventObservables(typeof(Samples.EventSource))]

                              namespace Samples;

                              public sealed class EventSource
                              {
                                  public event EventHandler<EventArgs>? Changed;

                                  public static event Action<int>? GlobalChanged;
                              }

                              public static class Consumer
                              {
                                  public static IObservable<EventArgs> Observe(EventSource source) => source.Events().Changed;

                                  public static IObservable<int> ObserveGlobal() => RxEvents.T11EventSource13GlobalChanged;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(Array.Exists(result.GeneratedHintNames, static name =>
            name.EndsWith(InstanceHintSuffix, StringComparison.Ordinal))).IsTrue();
        await Assert.That(Array.Exists(result.GeneratedHintNames, static name =>
            name.EndsWith(StaticHintSuffix, StringComparison.Ordinal))).IsTrue();
    }

    /// <summary>Verifies static requests report provider, generic-host, and empty-host diagnostics.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorReportsStaticRequestDiagnostics()
    {
        const string MissingProviderSource = """
                                             using ReactiveUI.Primitives.ObservableEvents;

                                             [assembly: GenerateStaticEventObservables(typeof(StaticHost))]

                                             public static class StaticHost
                                             {
                                                 public static event System.Action<int>? Changed;
                                             }
                                             """;
        const string UnsupportedHostsSource = """
                                              using ReactiveUI.Primitives.ObservableEvents;

                                              [assembly: GenerateStaticEventObservables(typeof(GenericHost<>))]
                                              [assembly: GenerateStaticEventObservables(typeof(EmptyHost))]

                                              public static class GenericHost<T>
                                              {
                                                  public static event System.Action<T>? Changed;
                                              }

                                              public static class EmptyHost
                                              {
                                              }
                                              """;
        var missingProvider = RunGenerator(MissingProviderSource, ProviderMode.None);
        var unsupportedHosts = RunGenerator(UnsupportedHostsSource, ProviderMode.Lean);

        await Assert.That(missingProvider.Diagnostics.Any(IsDiagnostic(NoProviderDiagnosticId))).IsTrue();
        await Assert.That(unsupportedHosts.Diagnostics.Any(IsDiagnostic(UnsupportedSignatureDiagnosticId))).IsTrue();
        await Assert.That(unsupportedHosts.Diagnostics.Any(IsDiagnostic(NoEventsDiagnosticId))).IsTrue();
    }

    /// <summary>Verifies global-namespace wrappers and escaped event identifiers compile.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsGlobalNamespaceAndKeywordEventNames()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives.ObservableEvents;

                              [assembly: GenerateStaticEventObservables(typeof(EventSource))]

                              public sealed class EventSource
                              {
                                  public event Action<int>? @event;

                                  public static event Action<string>? Global;
                              }

                              public static class Consumer
                              {
                                  public static IObservable<int> Observe(EventSource source) => source.Events().@event;

                                  public static IObservable<string> ObserveGlobal() => RxEvents.T11EventSource6Global;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("@event");
    }

    /// <summary>Verifies malformed static attributes are ignored without crashing generation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorIgnoresMalformedStaticRequestAttributes()
    {
        const string Source = """
                              using ReactiveUI.Primitives.ObservableEvents;

                              [assembly: GenerateStaticEventObservables]
                              [assembly: GenerateStaticEventObservables(null)]

                              public static class EventSource
                              {
                                  public static event System.Action? Changed;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.GeneratedHintNames.Any(IsEventWrapperHint)).IsFalse();
    }

    /// <summary>Verifies generic targets preserve their type parameters and constraints.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsGenericTargetsAndConstraints()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public sealed class EventSource<T>
                                  where T : class, new()
                              {
                                  public event Action<T>? Changed;
                              }

                              public static class Consumer
                              {
                                  public static IObservable<T> Observe<T>(EventSource<T> source)
                                      where T : class, new() => source.Events().Changed;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("where T : class, new()");
    }

    /// <summary>Verifies every supported generic constraint form is preserved in generated declarations.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorPreservesGenericConstraintForms()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public sealed class EventSource<TUnmanaged, TStruct, TNullable, TNotNull, TComparable>
                                  where TUnmanaged : unmanaged
                                  where TStruct : struct
                                  where TNullable : class?
                                  where TNotNull : notnull
                                  where TComparable : IComparable
                              {
                                  public event Action<TUnmanaged>? Changed;
                              }

                              public static class Consumer
                              {
                                  public static object Observe<TUnmanaged, TStruct, TNullable, TNotNull, TComparable>(
                                      EventSource<TUnmanaged, TStruct, TNullable, TNotNull, TComparable> source)
                                      where TUnmanaged : unmanaged
                                      where TStruct : struct
                                      where TNullable : class?
                                      where TNotNull : notnull
                                      where TComparable : IComparable => source.Events();
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("where TUnmanaged : unmanaged");
        await Assert.That(result.GeneratedText).Contains("where TStruct : struct");
        await Assert.That(result.GeneratedText).Contains("where TNullable : class?");
        await Assert.That(result.GeneratedText).Contains("where TNotNull : notnull");
    }

    /// <summary>Verifies a derived request includes accessible events inherited from its base type.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorIncludesInheritedEvents()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public class EventSourceBase
                              {
                                  public event EventHandler<EventArgs>? BaseChanged;
                              }

                              public sealed class EventSource : EventSourceBase
                              {
                                  public event Action<int>? Changed;
                              }

                              public static class Consumer
                              {
                                  public static IObservable<EventArgs> ObserveBase(EventSource source) => source.Events().BaseChanged;

                                  public static IObservable<int> ObserveDerived(EventSource source) => source.Events().Changed;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("BaseChanged");
        await Assert.That(result.GeneratedText).Contains("Changed");
    }

    /// <summary>Verifies zero-, one-, and multi-parameter delegates generate valid payload types.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsDelegatePayloadShapes()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives;
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public delegate void ManyHandler(string name, int value);

                              public sealed class EventSource
                              {
                                  public event Action? Empty;

                                  public event Action<int>? Single;

                                  public event ManyHandler? Many;
                              }

                              public static class Consumer
                              {
                                  public static IObservable<RxVoid> ObserveEmpty(EventSource source) => source.Events().Empty;

                                  public static IObservable<int> ObserveSingle(EventSource source) => source.Events().Single;

                                  public static IObservable<(string name, int value)> ObserveMany(EventSource source) => source.Events().Many;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("RxVoid.Default");
        await Assert.That(result.GeneratedText).Contains("(string name, int value)");
    }

    /// <summary>Verifies task-returning event delegates generate handlers with matching return values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsTaskAndValueTaskEventDelegates()
    {
        const string Source = """
                              using System;
                              using System.Threading.Tasks;
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public delegate Task TaskHandler(int value);

                              public delegate ValueTask ValueTaskHandler(string value);

                              public sealed class EventSource
                              {
                                  public event TaskHandler? TaskChanged;

                                  public event ValueTaskHandler? ValueTaskChanged;
                              }

                              public static class Consumer
                              {
                                  public static IObservable<int> ObserveTask(EventSource source) => source.Events().TaskChanged;

                                  public static IObservable<string> ObserveValueTask(EventSource source) => source.Events().ValueTaskChanged;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("Task.CompletedTask");
        await Assert.That(result.GeneratedText).Contains("ValueTask");
    }

    /// <summary>Verifies repeated activation calls for one type do not duplicate generated declarations.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorDeduplicatesRepeatedRequestsForOneType()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public sealed class EventSource
                              {
                                  public event Action<int>? Changed;
                              }

                              public static class Consumer
                              {
                                  public static IObservable<int> ObserveFirst(EventSource source) => source.Events().Changed;

                                  public static IObservable<int> ObserveSecond(EventSource source) => source.Events().Changed;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedHintNames.Count(static name =>
            name.EndsWith(InstanceHintSuffix, StringComparison.Ordinal))).IsEqualTo(1);
    }

    /// <summary>Verifies inaccessible events are skipped while public events remain available.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSkipsInaccessibleEvents()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public sealed class EventSource
                              {
                                  private event Action? PrivateChanged;

                                  internal event Action? InternalChanged;

                                  public event Action<int>? Changed;
                              }

                              public static class Consumer
                              {
                                  public static IObservable<int> Observe(EventSource source) => source.Events().Changed;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).DoesNotContain("PrivateChanged");
        await Assert.That(result.GeneratedText).DoesNotContain("InternalChanged");
    }

    /// <summary>Verifies requesting a type with no events reports the no-events diagnostic.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorReportsRequestedTypeWithNoEvents()
    {
        const string Source = """
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public sealed class EventSource
                              {
                              }

                              public static class Consumer
                              {
                                  public static object Observe(EventSource source) => source.Events();
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Diagnostics.Any(IsDiagnostic(NoEventsDiagnosticId))).IsTrue();
        await Assert.That(result.GeneratedHintNames.Any(IsEventWrapperHint)).IsFalse();
    }

    /// <summary>Verifies by-reference delegate parameters report a diagnostic without invalid generated output.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorReportsUnsupportedEventSignatures()
    {
        const string Source = """
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public delegate void UnsupportedHandler(ref int value);

                              public sealed class EventSource
                              {
                                  public event UnsupportedHandler? Changed;
                              }

                              public static class Consumer
                              {
                                  public static object Observe(EventSource source) => source.Events();
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Diagnostics.Any(IsDiagnostic(UnsupportedSignatureDiagnosticId))).IsTrue();
        await Assert.That(result.GeneratedHintNames.Any(IsEventWrapperHint)).IsFalse();
    }

    /// <summary>Verifies ref-like payloads and non-task return types are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorReportsAdditionalUnsupportedEventSignatures()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public delegate void SpanHandler(ReadOnlySpan<int> value);

                              public unsafe delegate void PointerHandler(int* value);

                              public unsafe delegate void FunctionPointerHandler(delegate*<void> value);

                              public delegate int ReturningHandler(int value);

                              public sealed class EventSource
                              {
                                  public event SpanHandler? SpanChanged;

                                  public event PointerHandler? PointerChanged;

                                  public event FunctionPointerHandler? FunctionPointerChanged;

                                  public event ReturningHandler? ReturningChanged;
                              }

                              public static class Consumer
                              {
                                  public static object Observe(EventSource source) => source.Events();
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Diagnostics.Count(IsDiagnostic(UnsupportedSignatureDiagnosticId)))
            .IsEqualTo(UnsupportedEventCount);
        await Assert.That(result.GeneratedHintNames.Any(IsEventWrapperHint)).IsFalse();
    }

    /// <summary>Verifies an invalid non-delegate event symbol is rejected without a generator exception.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorReportsNonDelegateEventTypes()
    {
        const string Source = """
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public sealed class EventSource
                              {
                                  public event int Broken;

                                  public event dynamic DynamicBroken;
                              }

                              public static class Consumer
                              {
                                  public static object Observe(EventSource source) => source.Events();
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Diagnostics.Any(IsDiagnostic(UnsupportedSignatureDiagnosticId))).IsTrue();
        await Assert.That(result.GeneratedHintNames.Any(IsEventWrapperHint)).IsFalse();
    }

    /// <summary>Verifies unresolved and non-named receivers do not become generation requests.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorIgnoresUnresolvableEventReceivers()
    {
        const string Source = """
                              using ReactiveUI.Primitives.ObservableEvents;

                              public static class Consumer
                              {
                                  public static object Observe<T>(T source) => source.Events();

                                  public static object ObserveMissing() => Missing.Events();
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.GeneratedHintNames.Any(IsEventWrapperHint)).IsFalse();
    }

    /// <summary>Verifies unrelated methods named Events do not activate generation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorIgnoresUnrelatedEventsMethods()
    {
        const string Source = """
                              namespace Samples;

                              public sealed class EventSource
                              {
                                  public event System.Action? Changed;

                                  public string Events() => string.Empty;
                              }

                              public static class Consumer
                              {
                                  public static string Observe(EventSource source) => source.Events();
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedHintNames.Length).IsEqualTo(1);
        await Assert.That(result.GeneratedHintNames).Contains(ActivationHintName);
    }

    /// <summary>Verifies static host and event name boundaries cannot produce property collisions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorAvoidsStaticHostPropertyCollisions()
    {
        const string Source = """
                              using ReactiveUI.Primitives.ObservableEvents;

                              [assembly: GenerateStaticEventObservables(typeof(Samples.OuterOne.Host))]
                              [assembly: GenerateStaticEventObservables(typeof(Samples.OuterTwo.Host))]
                              [assembly: GenerateStaticEventObservables(typeof(Samples.A))]
                              [assembly: GenerateStaticEventObservables(typeof(Samples.AB))]

                              namespace Samples;

                              public static class OuterOne
                              {
                                  public static class Host
                                  {
                                      public static event System.Action<int>? Changed;
                                  }
                              }

                              public static class OuterTwo
                              {
                                  public static class Host
                                  {
                                      public static event System.Action<int>? Changed;
                                  }
                              }

                              public static class A
                              {
                                  public static event System.Action? BC;
                              }

                              public static class AB
                              {
                                  public static event System.Action? C;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("T8OuterOne4Host7Changed");
        await Assert.That(result.GeneratedText).Contains("T8OuterTwo4Host7Changed");
        await Assert.That(result.GeneratedText).Contains("T1A2BC");
        await Assert.That(result.GeneratedText).Contains("T2AB1C");
    }

    /// <summary>Verifies shadowed nested generic parameter names are uniquely renamed in generated source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsShadowedNestedGenericParameters()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples;

                              public class Outer<T>
                              {
                                  public class Inner<T>
                                  {
                                      public event Action<T>? Changed;
                                  }
                              }

                              public static class Consumer
                              {
                                  public static IObservable<int> Observe(Outer<string>.Inner<int> source) =>
                                      source.Events().Changed;
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("<T, T2>");
        await Assert.That(result.GeneratedText).Contains("Outer<T>.Inner<T2>");
    }

    /// <summary>Verifies distinct symbol identities cannot collide after readable hint-name sanitization.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorAvoidsSanitizedHintNameCollisions()
    {
        const string Source = """
                              using ReactiveUI.Primitives.ObservableEvents;

                              namespace Samples.A_B
                              {
                                  public sealed class C
                                  {
                                      public event System.Action? Changed;
                                  }
                              }

                              namespace Samples.A
                              {
                                  public sealed class B_C
                                  {
                                      public event System.Action? Changed;
                                  }
                              }

                              public static class Consumer
                              {
                                  public static object ObserveOne(Samples.A_B.C source) => source.Events();

                                  public static object ObserveTwo(Samples.A.B_C source) => source.Events();
                              }
                              """;
        var result = RunGenerator(Source, ProviderMode.Lean);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedHintNames.Count(IsInstanceHint)).IsEqualTo(SanitizationCollisionWrapperCount);
    }

    /// <summary>Runs the production generator for one source and provider-reference mode.</summary>
    /// <param name="source">The consumer source to compile.</param>
    /// <param name="providerMode">The provider assemblies made visible to the consumer.</param>
    /// <param name="additionalReferencePaths">Additional consumer assembly paths to reference.</param>
    /// <returns>The generator diagnostics, compile errors, hint names, and generated source.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static GeneratorTestResult RunGenerator(
        string source,
        ProviderMode providerMode,
        params string[] additionalReferencePaths)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var compilation = CSharpCompilation.Create(
            $"ObservableEventsSmoke_{providerMode}",
            [CSharpSyntaxTree.ParseText(source, parseOptions)],
            CreateReferences(providerMode, additionalReferencePaths),
            new(OutputKind.DynamicallyLinkedLibrary, allowUnsafe: true));
        var driver = CSharpGeneratorDriver.Create(
            [new EventGenerator().AsSourceGenerator()],
            parseOptions: parseOptions);
        driver = (CSharpGeneratorDriver)driver.RunGeneratorsAndUpdateCompilation(
            compilation,
            out var updatedCompilation,
            out var generatorDiagnostics);
        var runResult = driver.GetRunResult();
        var generatedSources = runResult.Results
            .SelectMany(static result => result.GeneratedSources)
            .ToArray();
        var errors = updatedCompilation.GetDiagnostics()
            .Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToImmutableArray();
        return new(
            generatorDiagnostics,
            errors,
            [.. generatedSources.Select(static generated => generated.HintName)],
            string.Join(Environment.NewLine, generatedSources.Select(static generated => generated.SourceText.ToString())));
    }

    /// <summary>Creates a deliberately isolated metadata-reference set for a provider mode.</summary>
    /// <param name="providerMode">The provider assemblies to add after filtering trusted platform assemblies.</param>
    /// <param name="additionalReferencePaths">Additional consumer assembly paths to reference.</param>
    /// <returns>The compilation metadata references.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static List<MetadataReference> CreateReferences(
        ProviderMode providerMode,
        IReadOnlyCollection<string> additionalReferencePaths)
    {
        var references = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!.ToString()!
            .Split(System.IO.Path.PathSeparator)
            .Where(static path => !IsOptionalProviderAssembly(path))
            .Select(static path => MetadataReference.CreateFromFile(path))
            .Cast<MetadataReference>()
            .ToList();

        if (providerMode is ProviderMode.Lean or ProviderMode.LeanAndReactive)
        {
            AddReference(references, typeof(LeanSignal).Assembly.Location);
            AddReference(references, typeof(ReactiveRxVoid).Assembly.Location);
            AddReference(references, typeof(DisposableScope).Assembly.Location);
        }

        if (providerMode is ProviderMode.Reactive or ProviderMode.LeanAndReactive)
        {
            AddReference(references, typeof(ReactiveSignal).Assembly.Location);
            AddReference(references, typeof(DisposableScope).Assembly.Location);
            AddReference(references, typeof(ReactiveUnit).Assembly.Location);
        }
        else if (providerMode == ProviderMode.SystemReactive)
        {
            AddReference(references, typeof(ReactiveUnit).Assembly.Location);
        }

        foreach (var path in additionalReferencePaths)
        {
            AddReference(references, path);
        }

        return references;
    }

    /// <summary>Adds a metadata reference unless the same assembly path is already present.</summary>
    /// <param name="references">The destination references.</param>
    /// <param name="path">The assembly path to add.</param>
    private static void AddReference(List<MetadataReference> references, string path)
    {
        if (references.OfType<PortableExecutableReference>().Any(reference =>
                string.Equals(reference.FilePath, path, StringComparison.OrdinalIgnoreCase)))
        {
            return;
        }

        references.Add(MetadataReference.CreateFromFile(path));
    }

    /// <summary>Checks whether a trusted platform assembly belongs to an optional observable provider.</summary>
    /// <param name="path">The assembly path.</param>
    /// <returns><see langword="true"/> when the assembly must be excluded from the base reference set.</returns>
    private static bool IsOptionalProviderAssembly(string path)
    {
        var fileName = System.IO.Path.GetFileName(path);
        return fileName.StartsWith("ReactiveUI.Primitives", StringComparison.OrdinalIgnoreCase)
            || fileName.StartsWith("ReactiveUI.Disposables", StringComparison.OrdinalIgnoreCase)
            || fileName.StartsWith("System.Reactive", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Creates a predicate matching one diagnostic identifier.</summary>
    /// <param name="diagnosticId">The identifier to match.</param>
    /// <returns>A diagnostic predicate.</returns>
    private static Func<Diagnostic, bool> IsDiagnostic(string diagnosticId) =>
        diagnostic => diagnostic.Id == diagnosticId;

    /// <summary>Checks whether a generated hint denotes an event wrapper.</summary>
    /// <param name="hintName">The generated source hint name.</param>
    /// <returns><see langword="true"/> for instance or static event wrapper hints.</returns>
    private static bool IsEventWrapperHint(string hintName) =>
        hintName.EndsWith(InstanceHintSuffix, StringComparison.Ordinal)
        || hintName.EndsWith(StaticHintSuffix, StringComparison.Ordinal);

    /// <summary>Determines whether a generated-source hint identifies an instance wrapper.</summary>
    /// <param name="hintName">The hint name.</param>
    /// <returns><see langword="true"/> for an instance wrapper hint.</returns>
    private static bool IsInstanceHint(string hintName) =>
        hintName.EndsWith(InstanceHintSuffix, StringComparison.Ordinal);

    /// <summary>Captures the observable generator result needed by the tests.</summary>
    /// <param name="Diagnostics">Generator diagnostics.</param>
    /// <param name="Errors">Compilation errors after generation.</param>
    /// <param name="GeneratedHintNames">Generated source hint names.</param>
    /// <param name="GeneratedText">All generated source concatenated for inspection.</param>
    private sealed record GeneratorTestResult(
        ImmutableArray<Diagnostic> Diagnostics,
        ImmutableArray<Diagnostic> Errors,
        string[] GeneratedHintNames,
        string GeneratedText);
}
