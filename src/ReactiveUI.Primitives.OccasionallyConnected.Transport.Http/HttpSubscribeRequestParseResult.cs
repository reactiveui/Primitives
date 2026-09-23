// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Captures a validated subscribe request with its exact decoded query fields.</summary>
/// <param name="Request">The validated subscribe request.</param>
/// <param name="QueryFields">The decoded query fields used for replay canonicalization.</param>
internal sealed record HttpSubscribeRequestParseResult(
    RemoteSubscribeRequest Request,
    IReadOnlyList<KeyValuePair<string, string>> QueryFields);
