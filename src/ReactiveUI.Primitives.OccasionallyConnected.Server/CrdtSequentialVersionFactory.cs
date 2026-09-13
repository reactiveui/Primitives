// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Creates simple checked sequential CRDT server versions.</summary>
[System.Diagnostics.DebuggerDisplay("{_prefix,nq}")]
public sealed class CrdtSequentialVersionFactory : IServerConflictVersionFactory
{
    /// <summary>The decimal radix used by textual CRDT versions.</summary>
    private const int DecimalRadix = 10;

    /// <summary>The factory options.</summary>
    private readonly string _prefix;

    /// <summary>Initializes a new instance of the <see cref="CrdtSequentialVersionFactory"/> class.</summary>
    public CrdtSequentialVersionFactory()
        : this(new())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CrdtSequentialVersionFactory"/> class.</summary>
    /// <param name="options">The version options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">The prefix is invalid.</exception>
    public CrdtSequentialVersionFactory(CrdtSequentialVersionFactoryOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        ServerCommitJournalGuard.ValidateText(options.Prefix, nameof(options.Prefix));
        _prefix = options.Prefix;
    }

    /// <inheritdoc/>
    public string CreateNextVersion(ConflictContext context, SyncOperation operation)
    {
        ArgumentExceptionHelper.ThrowIfNull(context);
        ArgumentExceptionHelper.ThrowIfNull(operation);
        if (!context.Current.Version.StartsWith(_prefix, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The current CRDT server version has an incompatible prefix.");
        }

        var suffix = context.Current.Version.Substring(_prefix.Length);
        if (suffix.Length == 0)
        {
            throw new InvalidOperationException("The current CRDT server version has no sequence.");
        }

        if (suffix.Length > 1 && suffix[0] == '0')
        {
            throw new InvalidOperationException("The current CRDT server version sequence is not canonical.");
        }

        var value = ParseNonnegativeVersion(suffix);
        var next = checked(value + 1);
        return _prefix + next.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>Parses a nonnegative version suffix.</summary>
    /// <param name="suffix">The version suffix.</param>
    /// <returns>The parsed value.</returns>
    /// <exception cref="InvalidOperationException">The suffix is not canonical.</exception>
    /// <exception cref="OverflowException">The suffix exceeds the supported range.</exception>
    private static long ParseNonnegativeVersion(string suffix)
    {
        long value = 0;
        for (var index = 0; index < suffix.Length; index++)
        {
            var character = suffix[index];
            if (character is < '0' or > '9')
            {
                throw new InvalidOperationException("The current CRDT server version sequence is invalid.");
            }

            value = checked((value * DecimalRadix) + (character - '0'));
        }

        return value;
    }
}
