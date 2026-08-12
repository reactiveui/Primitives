// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using Avalonia.Interactivity;
using AvaloniaButton = Avalonia.Controls.Button;
using LeanAsyncContext = ReactiveUI.Primitives.Async.AsyncContext;
using ReactiveAsyncContext = ReactiveUI.Primitives.Async.Reactive.AsyncContext;

#if NET10_0_OR_GREATER
using Microsoft.Maui.Dispatching;
#endif

#if WINDOWS
using WinFormsButton = System.Windows.Forms.Button;
using WpfButton = System.Windows.Controls.Button;
using WpfRoutedEventArgs = System.Windows.RoutedEventArgs;
using WpfVisual = System.Windows.Media.Visual;
#endif

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies observable-event generation against platform and async consumer assemblies.</summary>
public sealed partial class EventGeneratorTests
{
    /// <summary>Generated property-name fragment shared by the desktop UI assertions.</summary>
    private const string GeneratedClickProperty = " Click";

    /// <summary>Consumer source containing Task and ValueTask event delegates.</summary>
    private const string AsyncEventSource = """
        using System;
        using System.Threading.Tasks;
        using ReactiveUI.Primitives.ObservableEvents;

        namespace Samples;

        public delegate Task TaskEventHandler(int value);

        public delegate ValueTask ValueTaskEventHandler(string value);

        public sealed class AsyncEventSource
        {
            public event TaskEventHandler? TaskChanged;

            public event ValueTaskEventHandler? ValueTaskChanged;
        }

        public static class Consumer
        {
            public static IObservable<int> ObserveTask(AsyncEventSource source) => source.Events().TaskChanged;

            public static IObservable<string> ObserveValueTask(AsyncEventSource source) =>
                source.Events().ValueTaskChanged;
        }
        """;

#if WINDOWS
    /// <summary>Verifies generation for a genuine inherited WPF routed event.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsWpfEvents()
    {
        const string Source = """
            using System;
            using System.Windows;
            using System.Windows.Controls;
            using ReactiveUI.Primitives.ObservableEvents;

            namespace Samples;

            public static class WpfConsumer
            {
                public static IObservable<RoutedEventArgs> Observe(Button button) => button.Events().Click;
            }
            """;
        var result = RunGenerator(
            Source,
            ProviderMode.Lean,
            typeof(WpfButton).Assembly.Location,
            typeof(WpfRoutedEventArgs).Assembly.Location,
            typeof(WpfVisual).Assembly.Location);

        await Assert.That(result.Diagnostics).IsEmpty();
        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("global::System.Windows.RoutedEventArgs");
        await Assert.That(result.GeneratedText).Contains(GeneratedClickProperty);
    }

    /// <summary>Verifies generation for a genuine inherited Windows Forms event.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsWinFormsEvents()
    {
        const string Source = """
            using System;
            using System.Windows.Forms;
            using ReactiveUI.Primitives.ObservableEvents;

            namespace Samples;

            public static class WinFormsConsumer
            {
                public static IObservable<EventArgs> Observe(Button button) => button.Events().Click;
            }
            """;
        var result = RunGenerator(
            Source,
            ProviderMode.Lean,
            typeof(WinFormsButton).Assembly.Location);

        await Assert.That(result.Diagnostics).IsEmpty();
        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("global::System.EventArgs");
        await Assert.That(result.GeneratedText).Contains(GeneratedClickProperty);
    }
#endif

    /// <summary>Verifies generation for a genuine inherited Avalonia routed event.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsAvaloniaEvents()
    {
        const string Source = """
            using System;
            using Avalonia.Controls;
            using Avalonia.Interactivity;
            using ReactiveUI.Primitives.ObservableEvents;

            namespace Samples;

            public static class AvaloniaConsumer
            {
                public static IObservable<RoutedEventArgs> Observe(Button button) => button.Events().Click;
            }
            """;
        var result = RunGenerator(
            Source,
            ProviderMode.Lean,
            typeof(AvaloniaButton).Assembly.Location,
            typeof(RoutedEventArgs).Assembly.Location);

        await Assert.That(result.Diagnostics).IsEmpty();
        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("global::Avalonia.Interactivity.RoutedEventArgs");
        await Assert.That(result.GeneratedText).Contains(GeneratedClickProperty);
    }

#if NET10_0_OR_GREATER
    /// <summary>Verifies generation for a genuine MAUI dispatcher event without starting a UI host.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsMauiEvents()
    {
        const string Source = """
            using System;
            using Microsoft.Maui.Dispatching;
            using ReactiveUI.Primitives.ObservableEvents;

            namespace Samples;

            public static class MauiConsumer
            {
                public static IObservable<EventArgs> Observe(IDispatcherTimer timer) => timer.Events().Tick;
            }
            """;
        var result = RunGenerator(
            Source,
            ProviderMode.Lean,
            typeof(IDispatcherTimer).Assembly.Location);

        await Assert.That(result.Diagnostics).IsEmpty();
        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("global::System.EventArgs");
        await Assert.That(result.GeneratedText).Contains(" Tick");
    }
#endif

    /// <summary>Verifies async events in a consumer referencing the lean async leaf.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsLeanAsyncEvents()
    {
        const string Source = AsyncEventSource + """

            public static class AsyncPackageConsumer
            {
                public static global::ReactiveUI.Primitives.Async.AsyncContext Context =>
                    global::ReactiveUI.Primitives.Async.AsyncContext.Default;
            }
            """;
        var result = RunGenerator(
            Source,
            ProviderMode.Lean,
            typeof(LeanAsyncContext).Assembly.Location);

        await Assert.That(result.Diagnostics).IsEmpty();
        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("ReactiveUI.Primitives.Signals.Signal.Create");
        await Assert.That(result.GeneratedText).Contains("Task.CompletedTask");
        await Assert.That(result.GeneratedText).Contains("ValueTask");
    }

    /// <summary>Verifies async events in a consumer referencing the System.Reactive async leaf.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorSupportsReactiveAsyncEvents()
    {
        const string Source = AsyncEventSource + """

            public static class AsyncPackageConsumer
            {
                public static global::ReactiveUI.Primitives.Async.Reactive.AsyncContext Context =>
                    global::ReactiveUI.Primitives.Async.Reactive.AsyncContext.Default;
            }
            """;
        var result = RunGenerator(
            Source,
            ProviderMode.Reactive,
            typeof(ReactiveAsyncContext).Assembly.Location);

        await Assert.That(result.Diagnostics).IsEmpty();
        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("ReactiveUI.Primitives.Reactive.Signals.Signal.Create");
        await Assert.That(result.GeneratedText).Contains("Task.CompletedTask");
        await Assert.That(result.GeneratedText).Contains("ValueTask");
    }

    /// <summary>Verifies lean provider precedence when both async leaf packages are referenced.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorPrefersLeanProviderWhenBothAsyncPackagesAreReferenced()
    {
        const string Source = AsyncEventSource + """

            public static class AsyncPackageConsumer
            {
                public static global::ReactiveUI.Primitives.Async.AsyncContext LeanContext =>
                    global::ReactiveUI.Primitives.Async.AsyncContext.Default;

                public static global::ReactiveUI.Primitives.Async.Reactive.AsyncContext ReactiveContext =>
                    global::ReactiveUI.Primitives.Async.Reactive.AsyncContext.Default;
            }
            """;
        var result = RunGenerator(
            Source,
            ProviderMode.LeanAndReactive,
            typeof(LeanAsyncContext).Assembly.Location,
            typeof(ReactiveAsyncContext).Assembly.Location);

        await Assert.That(result.Diagnostics).IsEmpty();
        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains("ReactiveUI.Primitives.Signals.Signal.Create");
        await Assert.That(result.GeneratedText).DoesNotContain(
            "ReactiveUI.Primitives.Reactive.Signals.Signal.Create");
    }
}
