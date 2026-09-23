// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the durable HTTP lost acknowledgement recovery scenario.</summary>
internal static partial class DurableHttpLostAckScenario
{
    /// <summary>Starts the real observer client from owned resources and preserves start and cleanup failures.</summary>
    /// <param name="observerStorePath">The observer SQLite path.</param>
    /// <param name="host">The real HTTP host.</param>
    /// <param name="clock">The shared clock.</param>
    /// <param name="factory">The concrete client resource constructors.</param>
    /// <param name="cancellationToken">The start cancellation token.</param>
    /// <returns>The running observer session.</returns>
    /// <exception cref="AggregateException">Observer startup and cleanup both fail.</exception>
    internal static async ValueTask<ClientSession> StartObserverWithResourcesAsync(
        string observerStorePath,
        DurableHttpLostAckHost host,
        MutableTimeProvider clock,
        ClientSessionResourceFactory factory,
        CancellationToken cancellationToken)
    {
        ClientSession? session = null;
        try
        {
            session = await CreateClientSessionAsync(
                observerStorePath,
                host.BaseAddress,
                clock,
                new(ObserverClientId, ObserverStoreIdentity, ObserverSubscription, ObserverCredential, ThrowingOperationIdSource.Instance),
                factory).ConfigureAwait(false);
            await session.Context.StartAsync(cancellationToken).ConfigureAwait(false);
            await host.WaitForSubscribeObservedAsync(ObserverCredential, cancellationToken).ConfigureAwait(false);
            return session;
        }
        catch (Exception startFailure)
        {
            if (session is not null)
            {
                await DisposeFailedObserverAsync(session, startFailure).ConfigureAwait(false);
            }

            throw;
        }
    }
}
