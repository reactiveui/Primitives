// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.Components;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Blazor.Reactive.Components;
#else
namespace ReactiveUI.Primitives.Blazor.Components;
#endif

/// <summary>Base component that tracks reactive subscriptions and refreshes through Blazor's renderer dispatcher.</summary>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public class ReactiveComponentBase : ComponentBase, IDisposable
{
    /// <summary>Tracks subscriptions owned by the component.</summary>
    private readonly MultipleDisposable _subscriptions = [];

    /// <summary>Value indicating whether the component has been disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="ReactiveComponentBase"/> class.</summary>
    protected ReactiveComponentBase() =>
        RendererSequencer = new BlazorRendererSequencer(InvokeGuardedAsync);

    /// <summary>Gets a value indicating whether the component has been disposed.</summary>
    protected bool IsDisposed => _disposed;

    /// <summary>Gets a sequencer that schedules work through the Blazor renderer dispatcher.</summary>
    protected ISequencer RendererSequencer { get; }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => $"IsDisposed = {IsDisposed}";

    /// <summary>Disposes the component and all tracked subscriptions.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Tracks a subscription so it is disposed when the component is disposed.</summary>
    /// <param name="subscription">The subscription to track.</param>
    /// <returns>The supplied subscription, or <see cref="EmptyDisposable.Instance"/> when the component has already been disposed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="subscription"/> is <see langword="null"/>.</exception>
    protected IDisposable Track(IDisposable subscription)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscription);

        if (IsDisposed)
        {
            subscription.Dispose();
            return EmptyDisposable.Instance;
        }

        _subscriptions.Add(subscription);
        return subscription;
    }

    /// <summary>Subscribes to a source and refreshes the component after each value.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">Action invoked for each value on the Blazor renderer dispatcher.</param>
    /// <returns>A tracked subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected IDisposable Observe<T>(IObservable<T> source, Action<T> onNext) =>
        Observe(source, onNext, null, null);

    /// <summary>Subscribes to a source and refreshes the component after each observed signal.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">Action invoked for each value on the Blazor renderer dispatcher.</param>
    /// <param name="onError">Optional action invoked when the source errors.</param>
    /// <param name="onCompleted">Optional action invoked when the source completes.</param>
    /// <returns>A tracked subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected IDisposable Observe<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception>? onError,
        Action? onCompleted) =>
        Observe(source, onNext, onError, onCompleted, true);

    /// <summary>Subscribes to a source and refreshes the component after each observed signal.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">Action invoked for each value on the Blazor renderer dispatcher.</param>
    /// <param name="onError">Optional action invoked when the source errors.</param>
    /// <param name="onCompleted">Optional action invoked when the source completes.</param>
    /// <param name="refreshAfterCallbacks">A value indicating whether to call <see cref="ComponentBase.StateHasChanged"/> after callbacks.</param>
    /// <returns>A tracked subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
    protected IDisposable Observe<T>(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception>? onError,
        Action? onCompleted,
        bool refreshAfterCallbacks)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        return Track(source.Subscribe(
            value => _ = InvokeGuardedAsync(() =>
            {
                onNext(value);
                Refresh(refreshAfterCallbacks);
            }),
            error => _ = InvokeGuardedAsync(() =>
            {
                if (onError is null)
                {
                    OnObservedError(error);
                }
                else
                {
                    onError(error);
                }

                Refresh(refreshAfterCallbacks);
            }),
            () => _ = InvokeGuardedAsync(() =>
            {
                onCompleted?.Invoke();
                Refresh(refreshAfterCallbacks);
            })));
    }

    /// <summary>Invalidates the component through Blazor's renderer dispatcher.</summary>
    /// <returns>A task that completes when the renderer has accepted the invalidation callback.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected Task InvalidateAsync() => InvokeAsync(StateHasChanged);

    /// <summary>Handles an unhandled subscription error.</summary>
    /// <param name="error">The observed error.</param>
    /// <exception cref="InvalidOperationException">Always thrown to surface the subscription error.</exception>
    protected virtual void OnObservedError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        throw new InvalidOperationException("The reactive subscription failed.", error);
    }

    /// <summary>Releases resources used by the component.</summary>
    /// <param name="disposing"><see langword="true"/> when managed resources should be released.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing || _disposed)
        {
            return;
        }

        _disposed = true;
        _subscriptions.Dispose();
    }

    /// <summary>
    /// Runs a callback through the renderer and routes failures into Blazor's error handling
    /// (<see cref="ComponentBase.DispatchExceptionAsync(Exception)"/>) so error boundaries observe them
    /// instead of the fault being lost with the discarded task.
    /// </summary>
    /// <param name="callback">Callback to run on the renderer dispatcher.</param>
    /// <returns>A task that completes when the callback (or its failure dispatch) has finished.</returns>
    private async Task InvokeGuardedAsync(Action callback)
    {
        try
        {
            await InvokeAsync(callback).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await DispatchExceptionAsync(ex).ConfigureAwait(false);
        }
    }

    /// <summary>Refreshes the component when requested and when it is still active.</summary>
    /// <param name="shouldRefresh">A value indicating whether refresh is requested.</param>
    private void Refresh(bool shouldRefresh)
    {
        if (!shouldRefresh || IsDisposed)
        {
            return;
        }

        StateHasChanged();
    }
}
