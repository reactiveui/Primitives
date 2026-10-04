// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NET8_0_OR_GREATER
using System.Security.Cryptography;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Owns one derived native cipher and the provider material used to validate cache hits.</summary>
internal sealed class SqliteRecordKey : IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="SqliteRecordKey"/> class.</summary>
    /// <param name="material">The privately copied provider key.</param>
    /// <param name="cipher">The derived cipher.</param>
    internal SqliteRecordKey(byte[] material, AesGcm cipher)
    {
        Material = material;
        Cipher = cipher;
    }

    /// <summary>Gets the privately copied provider material.</summary>
    internal byte[] Material { get; }

    /// <summary>Gets the native cipher owned by this entry.</summary>
    internal AesGcm Cipher { get; }

    /// <inheritdoc/>
    public void Dispose()
    {
        try
        {
            Cipher.Dispose();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(Material);
        }
    }
}
#endif
