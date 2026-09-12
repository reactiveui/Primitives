// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Groups dependencies used by a local stream committer.</summary>
/// <typeparam name="TState">The projected local state type.</typeparam>
/// <typeparam name="TInput">The local input type.</typeparam>
internal sealed record LocalStreamCommitterDependencies<TState, TInput>
{
    /// <summary>Gets the transactional local store.</summary>
    public required ILocalStoreAdapter Store { get; init; }

    /// <summary>Gets the payload serializer.</summary>
    public required IPayloadSerializer Serializer { get; init; }

    /// <summary>Gets the local projection.</summary>
    public required ILocalProjection<TState, TInput> Projection { get; init; }

    /// <summary>Gets the time provider used for operation timestamps.</summary>
    public required TimeProvider TimeProvider { get; init; }

    /// <summary>Gets the operation identifier source.</summary>
    public IOperationIdSource OperationIdSource { get; init; } = GuidOperationIdSource.Instance;

    /// <summary>Validates all dependencies.</summary>
    /// <exception cref="InvalidOperationException">A dependency is missing.</exception>
    internal void Validate()
    {
        Validate(Store, nameof(Store));
        Validate(Serializer, nameof(Serializer));
        Validate(Projection, nameof(Projection));
        Validate(TimeProvider, nameof(TimeProvider));
        Validate(OperationIdSource, nameof(OperationIdSource));
    }

    /// <summary>Validates a single dependency.</summary>
    /// <param name="dependency">The dependency.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="InvalidOperationException">The dependency is missing.</exception>
    private static void Validate(object? dependency, string parameterName)
    {
        if (dependency is not null)
        {
            return;
        }

        throw new InvalidOperationException($"{parameterName} must be supplied.");
    }
}
