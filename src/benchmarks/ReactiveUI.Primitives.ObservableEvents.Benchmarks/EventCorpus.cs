// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.ObservableEvents.Benchmarks;

/// <summary>Builds the consumer source the generator is measured against.</summary>
/// <remarks>
/// <para>
/// Every host carries one of each delegate shape the generator handles - the conventional sender and arguments
/// pair, a parameterless action, a single payload, a multi-parameter delegate that becomes a tuple, and a
/// task-returning handler - because the per-event work differs between them and a corpus of only the easy shape
/// would flatter the emitter.
/// </para>
/// <para>
/// One host per file, as real code is laid out. That is what makes the incremental cases mean what they claim:
/// editing one host has to be editing one file, or the measurement is of re-parsing the whole corpus instead.
/// </para>
/// </remarks>
internal static class EventCorpus
{
    /// <summary>The file name of the shared declarations every host file depends on.</summary>
    internal const string SharedFileName = "Shared.cs";

    /// <summary>Source that no request depends on, standing in for an unrelated keystroke.</summary>
    internal const string UnrelatedSource = """
        namespace Corpus
        {
            internal static class Unrelated
            {
                internal static int Value => 1;
            }
        }
        """;

    /// <summary>The file name the unrelated source is given.</summary>
    internal const string UnrelatedFileName = "Unrelated.cs";

    /// <summary>The delegates, static host, and static request every host file shares.</summary>
    private const string SharedSource = """
        using System;
        using System.Threading.Tasks;
        using ReactiveUI.Primitives.ObservableEvents;

        [assembly: GenerateStaticEventObservables(typeof(Corpus.StaticHost))]

        namespace Corpus
        {
            public delegate void ManyHandler(string name, int value);

            public delegate Task AsyncHandler(int value);

            public static class StaticHost
            {
                public static event Action<int>? GlobalChanged;
            }
        }
        """;

    /// <summary>The number of hosts in the small corpus.</summary>
    private const int SmallHostCount = 1;

    /// <summary>The number of hosts in the medium corpus.</summary>
    private const int MediumHostCount = 10;

    /// <summary>The number of hosts in the large corpus.</summary>
    private const int LargeHostCount = 50;

    /// <summary>Builds every source file in a corpus, shared declarations first.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The file name and text of each file.</returns>
    internal static List<(string Path, string Text)> FilesFor(CorpusSize size)
    {
        var hosts = HostCountFor(size);
        var files = new List<(string Path, string Text)>(hosts + 1) { (SharedFileName, SharedSource) };
        for (var index = 0; index < hosts; index++)
        {
            files.Add((HostFileName(index), HostSource(index, false)));
        }

        return files;
    }

    /// <summary>Builds one host's file with an extra event on it.</summary>
    /// <param name="index">The host index.</param>
    /// <returns>The file text.</returns>
    /// <remarks>
    /// The edit that has to invalidate exactly one wrapper: the activation overload's signature is untouched, every
    /// other host is untouched, and only this host's own generated file has anything new to say.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string HostSourceWithAddedEvent(int index) => HostSource(index, true);

    /// <summary>Gets the file name a host is declared in.</summary>
    /// <param name="index">The host index.</param>
    /// <returns>The file name.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string HostFileName(int index) =>
        $"Host{index.ToString(CultureInfo.InvariantCulture)}.cs";

    /// <summary>Gets the number of hosts a corpus size contains.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The host count.</returns>
    internal static int HostCountFor(CorpusSize size) => size switch
    {
        CorpusSize.Small => SmallHostCount,
        CorpusSize.Medium => MediumHostCount,
        _ => LargeHostCount,
    };

    /// <summary>Builds one host, its events, and the call site that asks for its wrapper.</summary>
    /// <param name="index">The host index, which makes every declared name unique.</param>
    /// <param name="withAddedEvent">Whether to declare one more event than the corpus normally has.</param>
    /// <returns>The file text.</returns>
    private static string HostSource(int index, bool withAddedEvent)
    {
        var suffix = index.ToString(CultureInfo.InvariantCulture);
        var added = withAddedEvent
            ? $"\n        public event Action<long>? Added{suffix};\n"
            : string.Empty;

        return $$"""
            using System;
            using ReactiveUI.Primitives.ObservableEvents;

            namespace Corpus
            {
                public sealed class Host{{suffix}}
                {
                    public event EventHandler<EventArgs>? Changed{{suffix}};

                    public event Action? Ready{{suffix}};

                    public event Action<int>? Counted{{suffix}};

                    public event ManyHandler? Many{{suffix}};

                    public event AsyncHandler? Awaited{{suffix}};
            {{added}}    }

                public static class Consumer{{suffix}}
                {
                    public static IObservable<EventArgs> Observe(Host{{suffix}} host) =>
                        host.Events().Changed{{suffix}};
                }
            }
            """;
    }
}
