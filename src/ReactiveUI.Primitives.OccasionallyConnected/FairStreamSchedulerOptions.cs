// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures the internal fair stream scheduler.</summary>
/// <param name="MaximumStreams">The maximum number of registered streams.</param>
/// <param name="MaximumEncodedDescriptorBytes">The maximum retained encoded bytes for stream and head descriptors.</param>
/// <param name="MinimumPriority">The minimum permitted stream priority.</param>
/// <param name="MaximumPriority">The maximum permitted stream priority.</param>
/// <param name="MaximumWeight">The maximum permitted stream scheduling weight.</param>
/// <param name="AgingInterval">The interval after which a waiting head earns one aging weight.</param>
internal readonly record struct FairStreamSchedulerOptions(
    int MaximumStreams,
    long MaximumEncodedDescriptorBytes,
    int MinimumPriority = -10,
    int MaximumPriority = 10,
    int MaximumWeight = 100,
    TimeSpan AgingInterval = default)
{
    /// <summary>Defines the default finite aging interval.</summary>
    private static readonly TimeSpan DefaultAgingInterval = TimeSpan.FromSeconds(30);

    /// <summary>Gets validated options with a finite default aging interval.</summary>
    internal FairStreamSchedulerOptions Validated
    {
        get
        {
            var options = AgingInterval == default ? this with { AgingInterval = DefaultAgingInterval } : this;
            options.Validate();
            return options;
        }
    }

    /// <summary>Validates scheduler option bounds.</summary>
    /// <exception cref="InvalidOperationException">The option record contains invalid values.</exception>
    internal void Validate()
    {
        if (MaximumStreams <= 0)
        {
            throw new InvalidOperationException("MaximumStreams must be positive.");
        }

        if (MaximumEncodedDescriptorBytes <= 0)
        {
            throw new InvalidOperationException("MaximumEncodedDescriptorBytes must be positive.");
        }

        if (MinimumPriority > MaximumPriority)
        {
            throw new InvalidOperationException("MinimumPriority cannot exceed MaximumPriority.");
        }

        if (MaximumWeight <= 0)
        {
            throw new InvalidOperationException("MaximumWeight must be positive.");
        }

        var validAgingInterval = AgingInterval > TimeSpan.Zero && AgingInterval != Timeout.InfiniteTimeSpan && AgingInterval != TimeSpan.MaxValue;
        if (validAgingInterval)
        {
            return;
        }

        throw new InvalidOperationException("AgingInterval must be positive and finite.");
    }
}
