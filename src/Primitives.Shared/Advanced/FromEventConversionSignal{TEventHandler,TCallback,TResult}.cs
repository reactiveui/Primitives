// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>
/// Bridges an event whose handler is built by a caller-supplied conversion, so the handler can do
/// synchronous work inside the event before the notification is published downstream. Supplying the
/// conversion removes the need to derive a delegate reflectively, keeping the bridge trim- and AOT-safe.
/// </summary>
/// <typeparam name="TEventHandler">The delegate type used by the event.</typeparam>
/// <typeparam name="TCallback">The notification callback type handed to the conversion.</typeparam>
/// <typeparam name="TResult">The element type published downstream.</typeparam>
/// <param name="conversion">Converts the notification callback into the event's handler type.</param>
/// <param name="addHandler">The action that attaches the converted handler.</param>
/// <param name="removeHandler">The action that detaches the converted handler.</param>
/// <param name="callback">Builds the notification callback for a subscribed observer.</param>
/// <param name="scheduler">The sequencer that attaches and detaches the handler, or <see langword="null"/> to use the calling thread.</param>
[System.Diagnostics.DebuggerDisplay("FromEventConversionSignal: Conversion = {_conversion}, Scheduler = {_scheduler}")]
public sealed class FromEventConversionSignal<TEventHandler, TCallback, TResult>(
    Func<TCallback, TEventHandler> conversion,
    Action<TEventHandler> addHandler,
    Action<TEventHandler> removeHandler,
    Func<IObserver<TResult>, TCallback> callback,
    ISequencer? scheduler) : IObservable<TResult>
    where TEventHandler : Delegate
{
    /// <summary>Converts the notification callback into the event's handler type.</summary>
    private readonly Func<TCallback, TEventHandler> _conversion = conversion;

    /// <summary>The action that attaches the converted handler.</summary>
    private readonly Action<TEventHandler> _addHandler = addHandler;

    /// <summary>The action that detaches the converted handler.</summary>
    private readonly Action<TEventHandler> _removeHandler = removeHandler;

    /// <summary>Builds the notification callback for a subscribed observer.</summary>
    private readonly Func<IObserver<TResult>, TCallback> _callback = callback;

    /// <summary>The sequencer that attaches and detaches the handler, or <see langword="null"/> for the calling thread.</summary>
    private readonly ISequencer? _scheduler = scheduler;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var handler = _conversion(_callback(observer));
        return EventHandlerScope.Attach(handler, _addHandler, _removeHandler, _scheduler);
    }
}
