// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Maui.Storage;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Implements the secure storage abstraction only for deterministic integration tests.</summary>
internal sealed class MobileTestSecureStorage : ISecureStorage
{
    /// <summary>Gets stored test values.</summary>
    internal Dictionary<string, string> Values { get; } = [];

    /// <summary>Gets or sets an injected secure-store failure.</summary>
    internal Exception? Failure { get; set; }

    /// <summary>Gets or sets a hook before committing a secure write.</summary>
    internal Func<Task>? BeforeWrite { get; set; }

    /// <summary>Gets the number of writes.</summary>
    internal int Writes { get; private set; }

    /// <inheritdoc/>
    public Task<string?> GetAsync(string key) =>
        Failure is null ? Task.FromResult(Values.GetValueOrDefault(key)) : Task.FromException<string?>(Failure);

    /// <inheritdoc/>
    public async Task SetAsync(string key, string value)
    {
        if (Failure is not null)
        {
            throw Failure;
        }

        if (BeforeWrite is not null)
        {
            await BeforeWrite();
        }

        Values[key] = value;
        Writes++;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Remove(string key) => Values.Remove(key);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RemoveAll() => Values.Clear();
}
