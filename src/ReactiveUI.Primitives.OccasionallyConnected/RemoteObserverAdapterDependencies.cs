// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Composes the observers, publication callbacks, and owned producer factory for a remote adapter.</summary>
/// <typeparam name="T">The remote input type.</typeparam>
internal sealed record RemoteObserverAdapterDependencies<T>
{
    /// <summary>Gets the observer receiving decoded committed remote values.</summary>
    public required IObserver<T> Observer { get; init; }

    /// <summary>Gets the adapter stream identity.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the committed remote message source.</summary>
    public required IObservable<RemoteMessage<T>> RemoteMessages { get; init; }

    /// <summary>Gets the default observer input admission options.</summary>
    public required ObserverInputOptions InputOptions { get; init; }

    /// <summary>Gets the owned input capture provider.</summary>
    public required IOccasionallyConnectedInputCapture<T> Capture { get; init; }

    /// <summary>Gets the serialized input publisher.</summary>
    public required IOccasionallyConnectedSerializedInputPublisher Publisher { get; init; }

    /// <summary>Gets the typed publication callback.</summary>
    public required Func<T, RemotePublishOptions?, CancellationToken, ValueTask<PublishReceipt>> PublishAsync { get; init; }

    /// <summary>Gets the factory for independently owned input producers.</summary>
    public required Func<OccasionallyConnectedInputProducerOptions<T>, IOccasionallyConnectedInputProducer<T>?> CreateProducer { get; init; }
}
