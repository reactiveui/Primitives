// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.ObservableEvents.Benchmarks;

/// <summary>How much event-bearing source the benchmarks put in front of the generator.</summary>
/// <remarks>Public because BenchmarkDotNet reads it off a public benchmark parameter.</remarks>
public enum CorpusSize
{
    /// <summary>One wrapped host, the shape a small view model has.</summary>
    Small = 0,

    /// <summary>Ten wrapped hosts, the shape a feature area has.</summary>
    Medium = 1,

    /// <summary>Fifty wrapped hosts, the shape a large application or a control library has.</summary>
    Large = 2,
}
