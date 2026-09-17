// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Creates the runnable ASP.NET Core collaboration server example.</summary>
public static class CollaborationServerExample
{
    /// <summary>Creates the configured web application.</summary>
    /// <param name="options">The example options.</param>
    /// <returns>The configured web application.</returns>
    public static WebApplication CreateWebApplication(CollaborationServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        var builder = WebApplication.CreateSlimBuilder();
        _ = builder.WebHost.UseUrls(options.ListenUri.ToString());
        _ = builder.Services
            .AddSingleton(options)
            .AddSingleton(static services => new CollaborationServerRuntimeService(services.GetRequiredService<CollaborationServerOptions>()))
            .AddSingleton<IHostedService>(static services => services.GetRequiredService<CollaborationServerRuntimeService>())
            .AddSingleton<AspNetHttpRequestBridge>();

        var app = builder.Build();
        var pathBase = CreatePathBase(options.PathBase);
        if (pathBase.HasValue)
        {
            _ = app.UsePathBase(pathBase);
        }

        _ = app.Use(HandleRequestAsync);
        return app;
    }

    /// <summary>Runs the configured web application until the host shuts down.</summary>
    /// <param name="options">The example options.</param>
    /// <returns>The asynchronous run task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task RunAsync(CollaborationServerOptions options) =>
        RunAsync(options, CancellationToken.None);

    /// <summary>Runs the configured web application until cancellation requests shutdown.</summary>
    /// <param name="options">The example options.</param>
    /// <param name="cancellationToken">The cancellation token that stops the host.</param>
    /// <returns>The asynchronous run task.</returns>
    public static async Task RunAsync(CollaborationServerOptions options, CancellationToken cancellationToken)
    {
        await using var app = CreateWebApplication(options);
        await app.RunAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Creates the ASP.NET path base from the normalized server option.</summary>
    /// <param name="pathBase">The normalized relative path base.</param>
    /// <returns>The ASP.NET path string.</returns>
    private static PathString CreatePathBase(string pathBase)
    {
        var relative = pathBase.Trim().Trim('/');
        return relative.Length == 0 ? PathString.Empty : new($"/{relative}");
    }

    /// <summary>Handles health checks and delegates protocol requests to the portable endpoint bridge.</summary>
    /// <param name="context">The ASP.NET Core request context.</param>
    /// <param name="next">The next middleware delegate.</param>
    /// <returns>The asynchronous middleware task.</returns>
    private static async Task HandleRequestAsync(HttpContext context, Func<Task> next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);
        if (HttpMethods.IsGet(context.Request.Method) && string.Equals(context.Request.Path.Value, "/healthz", StringComparison.Ordinal))
        {
            context.Response.ContentType = "text/plain";
            await context.Response.WriteAsync("ok", context.RequestAborted).ConfigureAwait(false);
            return;
        }

        var runtime = context.RequestServices.GetRequiredService<CollaborationServerRuntimeService>();
        var bridge = context.RequestServices.GetRequiredService<AspNetHttpRequestBridge>();
        await bridge.InvokeAsync(context, runtime.Credentials, runtime.HandleAsync).ConfigureAwait(false);
    }
}
