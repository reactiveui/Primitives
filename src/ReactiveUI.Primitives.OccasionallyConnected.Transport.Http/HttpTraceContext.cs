// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Propagates W3C trace context through HTTP headers without attaching payload data.</summary>
internal static class HttpTraceContext
{
    /// <summary>The W3C trace parent header.</summary>
    internal const string TraceParentHeader = "traceparent";

    /// <summary>The W3C trace state header.</summary>
    internal const string TraceStateHeader = "tracestate";

    /// <summary>The shared activity source name defined by the diagnostics design.</summary>
    internal const string DiagnosticSourceName = "ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>The server request activity name.</summary>
    internal const string ServerActivityName = "oc.transport.server";

    /// <summary>The maximum accepted length of an incoming trace header value.</summary>
    internal const int MaximumHeaderLength = 512;

    /// <summary>The lowest printable ASCII character accepted in trace state.</summary>
    private const char MinimumPrintable = ' ';

    /// <summary>The highest printable ASCII character accepted in trace state.</summary>
    private const char MaximumPrintable = '~';

    /// <summary>The activity source used for server request activities.</summary>
    private static readonly ActivitySource Source = new(DiagnosticSourceName);

    /// <summary>Gets a value indicating whether any listener observes server request activities.</summary>
    internal static bool HasServerListeners => Source.HasListeners();

    /// <summary>Adds the current W3C trace context to an outgoing request when the caller has not supplied one.</summary>
    /// <param name="request">The outgoing request.</param>
    /// <param name="httpClient">The client that sends the request.</param>
    internal static void Inject(HttpRequestMessage request, HttpClient httpClient)
    {
        var activity = Activity.Current;
        if (activity is null
            || activity.IdFormat != ActivityIdFormat.W3C
            || request.Headers.Contains(TraceParentHeader)
            || httpClient.DefaultRequestHeaders.Contains(TraceParentHeader))
        {
            return;
        }

        DistributedContextPropagator.Current.Inject(activity, request, SetTraceHeader);
    }

    /// <summary>Starts the server request activity parented to a valid incoming W3C trace context.</summary>
    /// <param name="request">The incoming request.</param>
    /// <returns>The caller-owned activity, or <see langword="null"/> when no listener records it.</returns>
    internal static Activity? StartServerActivity(HttpRequestMessage request) =>
        TryExtract(request, out var parentContext)
            ? Source.StartActivity(ServerActivityName, ActivityKind.Server, parentContext)
            : Source.StartActivity(ServerActivityName, ActivityKind.Server);

    /// <summary>Parses the incoming W3C trace context, ignoring missing, repeated, oversized, or malformed values.</summary>
    /// <param name="request">The incoming request.</param>
    /// <param name="context">The parsed remote context.</param>
    /// <returns><see langword="true"/> when a valid trace parent was found; otherwise, <see langword="false"/>.</returns>
    internal static bool TryExtract(HttpRequestMessage request, out ActivityContext context)
    {
        context = default;
        if (HttpReplayHeaders.ReadValueCount(request.Headers, TraceParentHeader, out var traceParent) != 1
            || traceParent is null
            || traceParent.Length > MaximumHeaderLength)
        {
            return false;
        }

        string? traceState = null;
        if (HttpReplayHeaders.ReadValueCount(request.Headers, TraceStateHeader, out var candidate) == 1
            && IsAcceptedTraceState(candidate))
        {
            traceState = candidate;
        }

        return ActivityContext.TryParse(traceParent, traceState, isRemote: true, out context);
    }

    /// <summary>Determines whether a trace state value is bounded printable ASCII.</summary>
    /// <param name="value">The candidate value.</param>
    /// <returns><see langword="true"/> when the value may be forwarded; otherwise, <see langword="false"/>.</returns>
    private static bool IsAcceptedTraceState(string? value)
    {
        if (value is null || value.Length > MaximumHeaderLength)
        {
            return false;
        }

        foreach (var character in value)
        {
            if (character is < MinimumPrintable or > MaximumPrintable)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Writes only W3C trace context fields to an outgoing request.</summary>
    /// <param name="carrier">The outgoing request.</param>
    /// <param name="fieldName">The propagated field name.</param>
    /// <param name="fieldValue">The propagated field value.</param>
    private static void SetTraceHeader(object? carrier, string fieldName, string fieldValue)
    {
        if (carrier is not HttpRequestMessage request
            || string.IsNullOrEmpty(fieldValue)
            || !(StringComparer.OrdinalIgnoreCase.Equals(fieldName, TraceParentHeader)
                || StringComparer.OrdinalIgnoreCase.Equals(fieldName, TraceStateHeader))
            || request.Headers.Contains(fieldName))
        {
            return;
        }

        _ = request.Headers.TryAddWithoutValidation(fieldName, fieldValue);
    }
}
