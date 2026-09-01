// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Owns one subscription's event-handler attachment for the event bridge signals.</summary>
public static class EventHandlerScope
{
    /// <summary>Attaches a handler and returns the disposable that detaches that same handler.</summary>
    /// <typeparam name="TEventHandler">The delegate type used by the event.</typeparam>
    /// <param name="handler">The handler this subscription owns.</param>
    /// <param name="addHandler">The action that attaches the handler.</param>
    /// <param name="removeHandler">The action that detaches the handler.</param>
    /// <param name="scheduler">The sequencer that attaches and detaches the handler, or <see langword="null"/> to use the calling thread.</param>
    /// <returns>The disposable that detaches the handler.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="addHandler"/> or <paramref name="removeHandler"/> is <see langword="null"/>.</exception>
    public static IDisposable Attach<TEventHandler>(
        TEventHandler handler,
        Action<TEventHandler> addHandler,
        Action<TEventHandler> removeHandler,
        ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(addHandler);

        ArgumentExceptionHelper.ThrowIfNull(removeHandler);

        if (scheduler is not { } sequencer)
        {
            addHandler(handler);
            return Scope.Create((handler, removeHandler), static state => state.removeHandler(state.handler));
        }

        return AttachScheduled(handler, addHandler, removeHandler, sequencer);
    }

    /// <summary>Attaches and later detaches the handler on the sequencer instead of the calling thread.</summary>
    /// <typeparam name="TEventHandler">The delegate type used by the event.</typeparam>
    /// <param name="handler">The handler this subscription owns.</param>
    /// <param name="addHandler">The action that attaches the handler.</param>
    /// <param name="removeHandler">The action that detaches the handler.</param>
    /// <param name="sequencer">The sequencer that attaches and detaches the handler.</param>
    /// <returns>The disposable that detaches the handler.</returns>
    /// <remarks>
    /// Disposing cancels a still-pending attach before requesting the detach, so a subscription torn down
    /// before the sequencer ran the attach cannot leave the handler on the event.
    /// </remarks>
    private static IDisposable AttachScheduled<TEventHandler>(
        TEventHandler handler,
        Action<TEventHandler> addHandler,
        Action<TEventHandler> removeHandler,
        ISequencer sequencer)
    {
        var attach = sequencer.Schedule(
            (handler, addHandler),
            static (_, state) =>
            {
                state.addHandler(state.handler);
                return EmptyDisposable.Instance;
            });

        return Scope.Create(
            (handler, removeHandler, attach, sequencer),
            static state =>
            {
                state.attach.Dispose();
                _ = state.sequencer.Schedule(
                    (state.handler, state.removeHandler),
                    static (_, detach) =>
                    {
                        detach.removeHandler(detach.handler);
                        return EmptyDisposable.Instance;
                    });
            });
    }
}
