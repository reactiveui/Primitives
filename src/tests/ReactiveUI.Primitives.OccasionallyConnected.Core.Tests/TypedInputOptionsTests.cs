// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="TypedInputOptions"/>.</summary>
public sealed class TypedInputOptionsTests
{
    /// <summary>The explicit typed-input retained byte declaration used by valid definitions.</summary>
    private const long DeclaredTypedInputBytes = 4096;

    /// <summary>The typed input buffer capacity used by structural equality tests.</summary>
    private const int StructuralBufferCapacity = 4;

    /// <summary>The typed input buffer byte capacity used by structural equality tests.</summary>
    private const long StructuralBufferCapacityBytes = 4096;

    /// <summary>The input contract identifier used by typed input tests.</summary>
    private const string InputContractId = "typed-input";

    /// <summary>The state contract identifier used by typed input tests.</summary>
    private const string StateContractId = "typed-state";

    /// <summary>The stream identifier used by typed input tests.</summary>
    private static readonly StreamId StreamId = new("typed/main");

    /// <summary>Verifies typed input requires an explicit retained-input byte declaration.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ValidTypedInputOptionsRequireDeclaredRetainedBytes()
    {
        var options = new TypedInputOptions { MaximumRetainedInputBytes = DeclaredTypedInputBytes };

        options.Validate();

        await Assert.That(options.BufferCapacity).IsGreaterThan(0);
        await Assert.That(options.BufferCapacityBytes).IsGreaterThan(0);
        await Assert.That(options.MaximumRetainedInputBytes).IsEqualTo(DeclaredTypedInputBytes);
    }

    /// <summary>Verifies invalid typed input queue limits are rejected.</summary>
    /// <param name="bufferCapacity">The candidate buffer capacity.</param>
    /// <param name="bufferCapacityBytes">The candidate retained byte capacity.</param>
    /// <param name="maximumRetainedInputBytes">The candidate retained bytes charged for one typed input.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(0, 1L, 1L)]
    [Arguments(1, 0L, 1L)]
    [Arguments(1, 1L, 0L)]
    [Arguments(1, 1L, 2L)]
    public async Task InvalidTypedInputOptionsThrow(
        int bufferCapacity,
        long bufferCapacityBytes,
        long maximumRetainedInputBytes)
    {
        var options = CreateOptions(bufferCapacity, bufferCapacityBytes, maximumRetainedInputBytes);

        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies typed retained-input bounds reject missing and over-capacity declarations.</summary>
    /// <param name="maximumRetainedInputBytes">The declared retained input charge.</param>
    /// <param name="bufferCapacityBytes">The retained byte queue capacity.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(0L, 128L)]
    [Arguments(256L, 128L)]
    public async Task TypedInputOptionsRejectInvalidRetainedByteDeclarations(
        long maximumRetainedInputBytes,
        long bufferCapacityBytes)
    {
        var options = CreateOptions(1, bufferCapacityBytes, maximumRetainedInputBytes);

        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies low-level stream descriptors remain valid without a typed input declaration.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamDefinitionAllowsMissingTypedInputDeclaration()
    {
        var definition = CreateDefinitionWithoutTypedInput();

        definition.Validate();

        await Assert.That(definition.TypedInput).IsNull();
    }

    /// <summary>Verifies stream definitions validate typed input options with the other stream contracts.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamDefinitionValidatesTypedInputDeclaration()
    {
        var definition = CreateDefinition() with { TypedInput = new() { MaximumRetainedInputBytes = 0 } };

        await Assert.That(definition.Validate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies typed input options participate in record equality for stream compatibility checks.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task TypedInputOptionsUseStructuralEquality()
    {
        var left = CreateOptions(StructuralBufferCapacity, StructuralBufferCapacityBytes, DeclaredTypedInputBytes);
        var right = left with { };
        var different = left with { MaximumRetainedInputBytes = DeclaredTypedInputBytes + 1 };

        await Assert.That(left).IsEqualTo(right);
        await Assert.That(left).IsNotEqualTo(different);
    }

    /// <summary>Creates typed input options with all bounded queue values assigned.</summary>
    /// <param name="capacity">The typed input buffer capacity.</param>
    /// <param name="bytes">The typed input byte capacity.</param>
    /// <param name="retainedBytes">The retained input byte charge.</param>
    /// <returns>The typed input options.</returns>
    private static TypedInputOptions CreateOptions(int capacity, long bytes, long retainedBytes) =>
        new() { BufferCapacity = capacity, BufferCapacityBytes = bytes, MaximumRetainedInputBytes = retainedBytes };

    /// <summary>Creates a valid stream definition.</summary>
    /// <returns>A valid stream definition.</returns>
    private static StreamDefinition<int, string> CreateDefinition() => new()
    {
        StreamId = StreamId,
        Projection = new Projection(),
        InputContractId = InputContractId,
        StateContractId = StateContractId,
        TypedInput = new() { MaximumRetainedInputBytes = DeclaredTypedInputBytes },
    };

    /// <summary>Creates a definition whose typed input declaration is missing at runtime.</summary>
    /// <returns>The malformed stream definition.</returns>
    private static StreamDefinition<int, string> CreateDefinitionWithoutTypedInput() =>
        CreateDefinition() with { TypedInput = null };

    /// <summary>Projects unchanged integer state for typed input contract tests.</summary>
    private sealed class Projection : ILocalProjection<int, string>
    {
        /// <inheritdoc />
        public int InitialState => 0;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ApplyLocal(int state, string input, SyncOperation operation) => state;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ApplyRemote(int state, string input, RemoteEvent remoteEvent) => state;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Reconcile(int state, ConflictResolutionResult result) => state;
    }
}
