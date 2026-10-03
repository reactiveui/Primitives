// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.Hosting;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Owns asynchronous runtime startup and shutdown for the ASP.NET host.</summary>
[System.Diagnostics.DebuggerDisplay("Started={_runtime is not null}")]
internal sealed class CollaborationServerRuntimeService : IHostedService, IAsyncDisposable
{
    /// <summary>The configured server options.</summary>
    private readonly CollaborationServerOptions _options;

    /// <summary>The runtime created during host startup.</summary>
    private CollaborationServerRuntime? _runtime;

    /// <summary>Initializes a new instance of the <see cref="CollaborationServerRuntimeService"/> class.</summary>
    /// <param name="options">The configured server options.</param>
    internal CollaborationServerRuntimeService(CollaborationServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <summary>Gets the development credential store used by the ASP.NET bridge.</summary>
    /// <exception cref="InvalidOperationException">The runtime has not started.</exception>
    internal DevelopmentCredentialStore Credentials => Runtime.Credentials;

    /// <summary>Gets the started runtime.</summary>
    /// <exception cref="InvalidOperationException">The runtime has not started.</exception>
    private CollaborationServerRuntime Runtime =>
        Volatile.Read(ref _runtime) ?? throw new InvalidOperationException("The collaboration server runtime has not started.");

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => DisposeRuntimeAsyncCore();

    /// <summary>Dispatches one portable HTTP request to the runtime endpoint.</summary>
    /// <param name="request">The request.</param>
    /// <param name="client">The host-authenticated client.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The portable response.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<HttpResponseMessage> HandleAsync(
        HttpRequestMessage request,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken) =>
        Runtime.HandleAsync(request, client, cancellationToken);

    /// <inheritdoc/>
    async Task IHostedService.StartAsync(CancellationToken cancellationToken) =>
        Volatile.Write(ref _runtime, await CollaborationServerRuntime.CreateAsync(_options, cancellationToken).ConfigureAwait(false));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    Task IHostedService.StopAsync(CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return DisposeAsync().AsTask();
    }

    /// <summary>Disposes the runtime once.</summary>
    /// <returns>The asynchronous disposal task.</returns>
    private async ValueTask DisposeRuntimeAsyncCore()
    {
        var runtime = Interlocked.Exchange(ref _runtime, null);
        if (runtime is not null)
        {
            await runtime.DisposeAsync().ConfigureAwait(false);
        }
    }
}
