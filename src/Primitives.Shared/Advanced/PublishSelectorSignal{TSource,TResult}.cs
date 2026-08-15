// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Connects a published source around a selector subscription.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The selected value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Source = {Source}, Selector = {Selector}")]
public sealed class PublishSelectorSignal<TSource, TResult> : IObservable<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="PublishSelectorSignal{TSource,TResult}"/> class.</summary>
    /// <param name="source">Source sequence to publish.</param>
    /// <param name="selector">Selector applied to the connectable source.</param>
    public PublishSelectorSignal(IObservable<TSource> source, Func<IObservable<TSource>, IObservable<TResult>> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        Source = source;
        Selector = selector;
    }

    /// <summary>Gets the source sequence to publish.</summary>
    private IObservable<TSource> Source { get; }

    /// <summary>Gets the selector applied to the connectable source.</summary>
    private Func<IObservable<TSource>, IObservable<TResult>> Selector { get; }

    /// <summary>Subscribes an observer to the selected published sequence.</summary>
    /// <param name="observer">Observer to subscribe.</param>
    /// <returns>A disposable that releases the selected subscription and source connection.</returns>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var connectable = new ConnectableSignal<TSource>(Source, new Signal<TSource>());
        IObservable<TResult> selected;
        try
        {
            selected = Selector(connectable);
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            observer.OnError(error);
            return EmptyDisposable.Instance;
        }

        if (selected is null)
        {
            observer.OnError(new InvalidOperationException("Publish selector returned null."));
            return EmptyDisposable.Instance;
        }

        CreateWitness<TResult> selectedObserver = new(observer);
        var subscription = selected.Subscribe(selectedObserver);
        var connection = connectable.Connect();
        selectedObserver.SetCancel(new MultipleDisposable(subscription, connection));
        return selectedObserver;
    }
}
