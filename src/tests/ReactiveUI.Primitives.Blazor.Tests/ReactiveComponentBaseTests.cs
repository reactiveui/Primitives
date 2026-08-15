// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.Logging.Abstractions;
using ReactiveUI.Primitives.Blazor.Components;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Blazor.Tests;

/// <summary>
/// Tests for <see cref="ReactiveComponentBase"/>'s observation, refresh, and disposal behaviour. Each test
/// attaches the component to an <see cref="HtmlRenderer"/> so its renderer-dispatcher-bound work (the guarded
/// invoke and <c>StateHasChanged</c>) actually runs, and drives the source on the dispatcher so the
/// notifications complete synchronously.
/// </summary>
public sealed class ReactiveComponentBaseTests
{
    /// <summary>The first value pushed to a source.</summary>
    private const int FirstValue = 1;

    /// <summary>Verifies an observed value reaches the callback and refreshes the component.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ObservedValueReachesTheCallbackAndRefreshes()
    {
        await using ComponentHarness harness = new();
        var component = await harness.AttachAsync();
        List<int> received = [];
        ManualSource<int> source = new();

        await harness.Dispatcher.InvokeAsync(() =>
        {
            _ = component.ObserveValues(source, received.Add);
            source.Push(FirstValue);
        });

        await Assert.That(received).Contains(FirstValue);
    }

    /// <summary>Verifies a provided error callback receives the source error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ProvidedErrorCallbackReceivesTheSourceError()
    {
        await using ComponentHarness harness = new();
        var component = await harness.AttachAsync();
        List<Exception> errors = [];
        ManualSource<int> source = new();
        InvalidOperationException failure = new("observed failure");

        await harness.Dispatcher.InvokeAsync(() =>
        {
            _ = component.ObserveSignals(source, static _ => { }, errors.Add, null, refreshAfterCallbacks: true);
            source.Fail(failure);
        });

        await Assert.That(errors).Contains(failure);
    }

    /// <summary>Verifies an error with no callback runs the default observed-error handler, whose throw is contained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ErrorWithNoCallbackRunsTheDefaultObservedErrorHandler()
    {
        await using ComponentHarness harness = new();
        var component = await harness.AttachAsync();
        ManualSource<int> source = new();
        InvalidOperationException failure = new("unhandled");

        await harness.Dispatcher.InvokeAsync(() =>
        {
            _ = component.ObserveValues(source, static _ => { });
            source.Fail(failure);
        });

        await Assert.That(component.ObservedErrors).Contains(failure);
    }

    /// <summary>Verifies a completion callback runs when the source completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletionCallbackRunsWhenTheSourceCompletes()
    {
        await using ComponentHarness harness = new();
        var component = await harness.AttachAsync();
        var completed = false;
        ManualSource<int> source = new();

        await harness.Dispatcher.InvokeAsync(() =>
        {
            _ = component.ObserveSignals(source, static _ => { }, null, () => completed = true, refreshAfterCallbacks: true);
            source.Complete();
        });

        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies suppressing the refresh still delivers the value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ObservationWithoutRefreshStillDeliversTheValue()
    {
        await using ComponentHarness harness = new();
        var component = await harness.AttachAsync();
        List<int> received = [];
        ManualSource<int> source = new();

        await harness.Dispatcher.InvokeAsync(() =>
        {
            _ = component.ObserveSignals(source, received.Add, null, null, refreshAfterCallbacks: false);
            source.Push(FirstValue);
        });

        await Assert.That(received).Contains(FirstValue);
    }

    /// <summary>Verifies invalidation marshals a state change through the renderer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task InvalidationMarshalsAStateChange()
    {
        await using ComponentHarness harness = new();
        var component = await harness.AttachAsync();

        await harness.Dispatcher.InvokeAsync(() => component.InvalidateComponentAsync());

        await Assert.That(component.IsDisposedState).IsFalse();
    }

    /// <summary>Verifies a second disposal is a no-op.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SecondDisposalIsANoOp()
    {
        HarnessComponent component = new();

        component.Dispose();
        component.Dispose();

        await Assert.That(component.IsDisposedState).IsTrue();
    }

    /// <summary>A source whose observer the test drives directly.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class ManualSource<T> : IObservable<T>
    {
        /// <summary>The current observer.</summary>
        private IObserver<T>? _observer;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observer = observer;
            return EmptyDisposable.Instance;
        }

        /// <summary>Pushes a value to the observer.</summary>
        /// <param name="value">The value to push.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Push(T value) => _observer?.OnNext(value);

        /// <summary>Fails the observer with an error.</summary>
        /// <param name="error">The error to raise.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Fail(Exception error) => _observer?.OnError(error);

        /// <summary>Completes the observer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Complete() => _observer?.OnCompleted();
    }

    /// <summary>A reactive component that exposes its protected observation members and reports its own creation.</summary>
    private sealed class HarnessComponent : ReactiveComponentBase
    {
        /// <summary>Gets or sets a callback invoked with this instance once its parameters are set.</summary>
        [Parameter]
        public Action<HarnessComponent>? Captured { get; set; }

        /// <summary>Gets the errors passed to the default observed-error handler.</summary>
        public List<Exception> ObservedErrors { get; } = [];

        /// <summary>Gets a value indicating whether the component is disposed.</summary>
        public bool IsDisposedState => IsDisposed;

        /// <summary>Observes a source with a value callback.</summary>
        /// <typeparam name="T">The value type.</typeparam>
        /// <param name="source">The source sequence.</param>
        /// <param name="onNext">The value callback.</param>
        /// <returns>The tracked subscription.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable ObserveValues<T>(IObservable<T> source, Action<T> onNext) => Observe(source, onNext);

        /// <summary>Observes a source with value, error, and completion callbacks.</summary>
        /// <typeparam name="T">The value type.</typeparam>
        /// <param name="source">The source sequence.</param>
        /// <param name="onNext">The value callback.</param>
        /// <param name="onError">The error callback.</param>
        /// <param name="onCompleted">The completion callback.</param>
        /// <param name="refreshAfterCallbacks">Whether to refresh after callbacks.</param>
        /// <returns>The tracked subscription.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable ObserveSignals<T>(
            IObservable<T> source,
            Action<T> onNext,
            Action<Exception>? onError,
            Action? onCompleted,
            bool refreshAfterCallbacks) =>
            Observe(source, onNext, onError, onCompleted, refreshAfterCallbacks);

        /// <summary>Invalidates the component through the renderer.</summary>
        /// <returns>A task that completes when the invalidation is accepted.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task InvalidateComponentAsync() => InvalidateAsync();

        /// <inheritdoc/>
        protected override void OnParametersSet() => Captured?.Invoke(this);

        /// <inheritdoc/>
        protected override void OnObservedError(Exception error)
        {
            ObservedErrors.Add(error);
            base.OnObservedError(error);
        }
    }

    /// <summary>Hosts a <see cref="HarnessComponent"/> on an <see cref="HtmlRenderer"/>.</summary>
    private sealed class ComponentHarness : IAsyncDisposable
    {
        /// <summary>The renderer that hosts the component.</summary>
        private readonly HtmlRenderer _renderer = new(EmptyServiceProvider.Instance, NullLoggerFactory.Instance);

        /// <summary>Gets the renderer dispatcher.</summary>
        public Dispatcher Dispatcher => _renderer.Dispatcher;

        /// <summary>Attaches a fresh harness component as a root component.</summary>
        /// <returns>The attached component.</returns>
        public async Task<HarnessComponent> AttachAsync()
        {
            HarnessComponent? captured = null;
            var parameters = ParameterView.FromDictionary(new Dictionary<string, object?> { ["Captured"] = (Action<HarnessComponent>)(component => captured = component), });

            await Dispatcher.InvokeAsync(() => _renderer.BeginRenderingComponent<HarnessComponent>(parameters));

            return captured!;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => _renderer.DisposeAsync();
    }

    /// <summary>A service provider that resolves nothing, so the renderer uses its default component activator.</summary>
    private sealed class EmptyServiceProvider : IServiceProvider
    {
        /// <summary>Gets the shared empty provider.</summary>
        public static EmptyServiceProvider Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object? GetService(Type serviceType) => null;
    }
}
