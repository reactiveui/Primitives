// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Specialized;
using System.ComponentModel;
using System.Reflection;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Converts an event add/remove pair into event-pattern notifications.</summary>
/// <typeparam name="TEventHandler">The event delegate type.</typeparam>
/// <typeparam name="TEventArgs">The event argument type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AddHandler = {AddHandler}, RemoveHandler = {RemoveHandler}")]
public sealed class FromEventPatternSignal<TEventHandler, TEventArgs> : IObservable<EventPattern<TEventArgs>>
    where TEventHandler : Delegate
    where TEventArgs : EventArgs
{
    /// <summary>The compatible handler forwarder method for this closed generic signal.</summary>
    private static readonly MethodInfo ForwarderOnEvent = typeof(Forwarder).GetMethod(nameof(Forwarder.OnEvent))!;

    /// <summary>Initializes a new instance of the <see cref="FromEventPatternSignal{TEventHandler, TEventArgs}"/> class.</summary>
    /// <param name="addHandler">The action that attaches the generated handler.</param>
    /// <param name="removeHandler">The action that detaches the generated handler.</param>
    public FromEventPatternSignal(Action<TEventHandler> addHandler, Action<TEventHandler> removeHandler)
        : this(addHandler, removeHandler, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="FromEventPatternSignal{TEventHandler, TEventArgs}"/> class.</summary>
    /// <param name="addHandler">The action that attaches the generated handler.</param>
    /// <param name="removeHandler">The action that detaches the generated handler.</param>
    /// <param name="scheduler">The sequencer that attaches and detaches the handler, or <see langword="null"/> to use the subscribing thread.</param>
    public FromEventPatternSignal(
        Action<TEventHandler> addHandler,
        Action<TEventHandler> removeHandler,
        ISequencer? scheduler)
    {
        AddHandler = addHandler;
        RemoveHandler = removeHandler;
        Scheduler = scheduler;
    }

    /// <summary>Gets the action that attaches the generated handler.</summary>
    private Action<TEventHandler> AddHandler { get; }

    /// <summary>Gets the action that detaches the generated handler.</summary>
    private Action<TEventHandler> RemoveHandler { get; }

    /// <summary>Gets the sequencer that attaches and detaches the handler.</summary>
    private ISequencer? Scheduler { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<EventPattern<TEventArgs>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var handler = CreateHandler(observer);
        return EventHandlerScope.Attach(handler, AddHandler, RemoveHandler, Scheduler);
    }

    /// <summary>Creates a supported event delegate for the observer.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The generated event handler.</returns>
    private static TEventHandler CreateHandler(IObserver<EventPattern<TEventArgs>> observer)
    {
        if (typeof(TEventHandler) == typeof(EventHandler))
        {
            EventHandler typed = (sender, args) => observer.OnNext(new(sender, (TEventArgs)args));
            return (TEventHandler)(object)typed;
        }

        if (typeof(TEventHandler) == typeof(PropertyChangedEventHandler))
        {
            PropertyChangedEventHandler typed = (sender, args) =>
                observer.OnNext(new(sender, (TEventArgs)(EventArgs)args));
            return (TEventHandler)(object)typed;
        }

        if (typeof(TEventHandler) == typeof(NotifyCollectionChangedEventHandler))
        {
            NotifyCollectionChangedEventHandler typed = (sender, args) =>
                observer.OnNext(new(sender, (TEventArgs)(EventArgs)args));
            return (TEventHandler)(object)typed;
        }

        if (typeof(TEventHandler) == typeof(ListChangedEventHandler))
        {
            ListChangedEventHandler typed = (sender, args) => observer.OnNext(new(sender, (TEventArgs)(EventArgs)args));
            return (TEventHandler)(object)typed;
        }

        if (typeof(TEventHandler) == typeof(EventHandler<TEventArgs>))
        {
            EventHandler<TEventArgs> typed = (sender, args) => observer.OnNext(new(sender, args));
            return (TEventHandler)(object)typed;
        }

        return CreateCompatibleHandler(observer);
    }

    /// <summary>Creates a delegate for event handler shapes compatible with <c>void Handler(object, TEventArgs)</c>.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The generated event handler.</returns>
    /// <exception cref="NotSupportedException"><typeparamref name="TEventHandler"/> is not compatible with the
    /// <c>void Handler(object, TEventArgs)</c> shape.</exception>
    private static TEventHandler CreateCompatibleHandler(IObserver<EventPattern<TEventArgs>> observer)
    {
        var forwarder = new Forwarder(observer);

        try
        {
#if NET8_0_OR_GREATER
            return ForwarderOnEvent.CreateDelegate<TEventHandler>(forwarder);
#else
            return (TEventHandler)Delegate.CreateDelegate(typeof(TEventHandler), forwarder, ForwarderOnEvent);
#endif
        }
        catch (ArgumentException ex)
        {
            throw new NotSupportedException($"Event handler type '{typeof(TEventHandler)}' is not supported.", ex);
        }
    }

    /// <summary>Forwards compatible delegate invocations to the subscribed observer.</summary>
    /// <param name="observer">The downstream observer.</param>
    private sealed class Forwarder(IObserver<EventPattern<TEventArgs>> observer)
    {
        /// <summary>Forwards event arguments to the observer.</summary>
        /// <param name="sender">The event sender.</param>
        /// <param name="args">The event arguments.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnEvent(object? sender, TEventArgs args) => observer.OnNext(new(sender, args));
    }
}
