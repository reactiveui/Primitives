// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.Logging.Abstractions;
using ReactiveUI.Primitives.Signals;
using PrimitivesComponentBase = ReactiveUI.Primitives.Blazor.Components.ReactiveComponentBase;
using ReactiveShimComponentBase = ReactiveUI.Primitives.Blazor.Reactive.Components.ReactiveComponentBase;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures a component attached to an <see cref="HtmlRenderer"/> re-rendering once per value its source signals.</summary>
[MemoryDiagnoser]
public class ReactiveComponentRenderBenchmarks : IDisposable
{
    /// <summary>The number of values pushed per case.</summary>
    private const int Count = 1000;

    /// <summary>The parameter name carrying the observed source.</summary>
    private const string SourceParameter = "Source";

    /// <summary>The parameter name carrying the render tally.</summary>
    private const string TallyParameter = "Tally";

    /// <summary>The source observed by the hand-written component.</summary>
    private readonly Signal<int> _handWrittenSource = new();

    /// <summary>The source observed by the Primitives component.</summary>
    private readonly Signal<int> _primitivesSource = new();

    /// <summary>The source observed by the System.Reactive-flavoured component.</summary>
    private readonly Signal<int> _reactiveShimSource = new();

    /// <summary>The renders recorded by the hand-written component.</summary>
    private readonly RenderTally _handWrittenTally = new();

    /// <summary>The renders recorded by the Primitives component.</summary>
    private readonly RenderTally _primitivesTally = new();

    /// <summary>The renders recorded by the System.Reactive-flavoured component.</summary>
    private readonly RenderTally _reactiveShimTally = new();

    /// <summary>The renderer hosting every component.</summary>
    private HtmlRenderer? _renderer;

    /// <summary>Attaches one component of each kind to a fresh renderer.</summary>
    /// <returns>A task that completes when every component has rendered.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var renderer = new HtmlRenderer(ComponentServices.Instance, NullLoggerFactory.Instance);
        _renderer = renderer;
        await renderer.Dispatcher.InvokeAsync(() =>
        {
            _ = renderer.BeginRenderingComponent<HandWrittenRenderComponent>(Parameters(_handWrittenSource, _handWrittenTally));
            _ = renderer.BeginRenderingComponent<PrimitivesRenderComponent>(Parameters(_primitivesSource, _primitivesTally));
            _ = renderer.BeginRenderingComponent<ReactiveShimRenderComponent>(Parameters(_reactiveShimSource, _reactiveShimTally));
        }).ConfigureAwait(false);
    }

    /// <summary>Benchmarks a component that subscribes and marshals each value to the renderer itself.</summary>
    /// <returns>The number of renders the pushed values caused.</returns>
    [Benchmark(Baseline = true)]
    public int HandWrittenComponentRender()
    {
        var start = _handWrittenTally.Renders;
        for (var i = 0; i < Count; i++)
        {
            _handWrittenSource.OnNext(i);
        }

        return _handWrittenTally.Renders - start;
    }

    /// <summary>Benchmarks a Primitives reactive component re-rendering per observed value.</summary>
    /// <returns>The number of renders the pushed values caused.</returns>
    [Benchmark]
    public int PrimitivesReactiveComponentRender()
    {
        var start = _primitivesTally.Renders;
        for (var i = 0; i < Count; i++)
        {
            _primitivesSource.OnNext(i);
        }

        return _primitivesTally.Renders - start;
    }

    /// <summary>Benchmarks the System.Reactive-flavoured reactive component re-rendering per observed value.</summary>
    /// <returns>The number of renders the pushed values caused.</returns>
    [Benchmark]
    public int ReactiveShimComponentRender()
    {
        var start = _reactiveShimTally.Renders;
        for (var i = 0; i < Count; i++)
        {
            _reactiveShimSource.OnNext(i);
        }

        return _reactiveShimTally.Renders - start;
    }

    /// <summary>Disposes the renderer, its components, and the sources.</summary>
    [GlobalCleanup]
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Releases the renderer and the sources.</summary>
    /// <param name="disposing"><see langword="true"/> when managed resources should be released.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        _renderer?.Dispose();
        _renderer = null;
        _handWrittenSource.Dispose();
        _primitivesSource.Dispose();
        _reactiveShimSource.Dispose();
    }

    /// <summary>Builds the root parameters for a component.</summary>
    /// <param name="source">The source the component observes.</param>
    /// <param name="tally">The tally the component records renders in.</param>
    /// <returns>The parameter view.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ParameterView Parameters(IObservable<int> source, RenderTally tally) =>
        ParameterView.FromDictionary(new Dictionary<string, object?> { [SourceParameter] = source, [TallyParameter] = tally });

    /// <summary>Counts component renders.</summary>
    private sealed class RenderTally
    {
        /// <summary>Gets or sets the number of renders.</summary>
        public int Renders { get; set; }
    }

    /// <summary>A component that subscribes to its source and marshals each value through the renderer by hand.</summary>
    private sealed class HandWrittenRenderComponent : ComponentBase, IObserver<int>, IDisposable
    {
        /// <summary>The source subscription.</summary>
        private IDisposable? _subscription;

        /// <summary>The last observed value.</summary>
        private int _value;

        /// <summary>Gets or sets the observed source.</summary>
        [Parameter]
        public IObservable<int>? Source { get; set; }

        /// <summary>Gets or sets the render tally.</summary>
        [Parameter]
        public RenderTally? Tally { get; set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(int value) => _ = InvokeAsync(() =>
        {
            _value = value;
            StateHasChanged();
        });

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _ = DispatchExceptionAsync(error);

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _subscription?.Dispose();

        /// <inheritdoc/>
        protected override void OnInitialized() => _subscription = Source!.Subscribe(this);

        /// <inheritdoc/>
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.AddContent(0, _value);
            Tally!.Renders++;
        }
    }

    /// <summary>A Primitives reactive component that observes its source.</summary>
    private sealed class PrimitivesRenderComponent : PrimitivesComponentBase
    {
        /// <summary>The last observed value.</summary>
        private int _value;

        /// <summary>Gets or sets the observed source.</summary>
        [Parameter]
        public IObservable<int>? Source { get; set; }

        /// <summary>Gets or sets the render tally.</summary>
        [Parameter]
        public RenderTally? Tally { get; set; }

        /// <inheritdoc/>
        protected override void OnInitialized() => _ = Observe(Source!, value => _value = value);

        /// <inheritdoc/>
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.AddContent(0, _value);
            Tally!.Renders++;
        }
    }

    /// <summary>A System.Reactive-flavoured reactive component that observes its source.</summary>
    private sealed class ReactiveShimRenderComponent : ReactiveShimComponentBase
    {
        /// <summary>The last observed value.</summary>
        private int _value;

        /// <summary>Gets or sets the observed source.</summary>
        [Parameter]
        public IObservable<int>? Source { get; set; }

        /// <summary>Gets or sets the render tally.</summary>
        [Parameter]
        public RenderTally? Tally { get; set; }

        /// <inheritdoc/>
        protected override void OnInitialized() => _ = Observe(Source!, value => _value = value);

        /// <inheritdoc/>
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.AddContent(0, _value);
            Tally!.Renders++;
        }
    }

    /// <summary>Creates the benchmark components without reflection.</summary>
    private sealed class BenchmarkComponentActivator : IComponentActivator
    {
        /// <summary>Gets the shared activator.</summary>
        public static BenchmarkComponentActivator Instance { get; } = new();

        /// <inheritdoc/>
        public IComponent CreateInstance([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type componentType)
        {
            if (componentType == typeof(HandWrittenRenderComponent))
            {
                return new HandWrittenRenderComponent();
            }

            if (componentType == typeof(PrimitivesRenderComponent))
            {
                return new PrimitivesRenderComponent();
            }

            return componentType == typeof(ReactiveShimRenderComponent)
                ? new ReactiveShimRenderComponent()
                : (IComponent)Activator.CreateInstance(componentType)!;
        }
    }

    /// <summary>A service provider that supplies only the benchmark component activator.</summary>
    private sealed class ComponentServices : IServiceProvider
    {
        /// <summary>Gets the shared provider.</summary>
        public static ComponentServices Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object? GetService(Type serviceType) =>
            serviceType == typeof(IComponentActivator) ? BenchmarkComponentActivator.Instance : null;
    }
}
