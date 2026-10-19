// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Describes a safe replay rejection without secret or payload-bearing diagnostics.</summary>
/// <param name="StatusCode">The HTTP status code to return.</param>
/// <param name="Kind">The transport failure classification.</param>
internal sealed record HttpReplayFailure(HttpStatusCode StatusCode, HttpTransportFailureKind Kind);
