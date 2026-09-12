// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures a local stream committer.</summary>
/// <typeparam name="TState">The projected local state type.</typeparam>
/// <typeparam name="TInput">The local input type.</typeparam>
internal sealed record LocalStreamCommitterOptions<TState, TInput>
{
    /// <summary>Gets the stream identifier.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the logical subscription identifier supplied to recovery.</summary>
    public required SubscriptionId SubscriptionId { get; init; }

    /// <summary>Gets the client identity already bound to the initialized store, or null for a legacy stream.</summary>
    public string? ClientId { get; init; }

    /// <summary>Gets the payload and snapshot contracts.</summary>
    public required LocalStreamCommitterContracts Contracts { get; init; }

    /// <summary>Gets the committer dependencies.</summary>
    public required LocalStreamCommitterDependencies<TState, TInput> Dependencies { get; init; }

    /// <summary>Gets the inclusive minimum priority.</summary>
    public int MinimumPriority { get; init; } = OperationPolicy.MinimumPriority;

    /// <summary>Gets the inclusive maximum priority.</summary>
    public int MaximumPriority { get; init; } = OperationPolicy.MaximumPriority;

    /// <summary>Validates this option set.</summary>
    /// <exception cref="InvalidOperationException">The option set is malformed.</exception>
    internal void Validate()
    {
        if (string.IsNullOrEmpty(StreamId.Value))
        {
            throw new InvalidOperationException("StreamId must be non-empty.");
        }

        if (SubscriptionId.Value == Guid.Empty)
        {
            throw new InvalidOperationException("SubscriptionId must be non-empty.");
        }

        if (MinimumPriority > MaximumPriority)
        {
            throw new InvalidOperationException("MinimumPriority must not exceed MaximumPriority.");
        }

        ValidateRequired(Contracts, nameof(Contracts));
        ValidateRequired(Dependencies, nameof(Dependencies));
        Contracts.Validate();
        Dependencies.Validate();
    }

    /// <summary>Validates a required object.</summary>
    /// <param name="value">The object value.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="InvalidOperationException">The object is missing.</exception>
    private static void ValidateRequired(object? value, string parameterName)
    {
        if (value is not null)
        {
            return;
        }

        throw new InvalidOperationException($"{parameterName} must be supplied.");
    }
}
