#!/usr/bin/env python3
"""One-off generator for the arity-N CombineLatest operator files.

Emits CombineLatest2.cs .. CombineLatest16.cs next to this script, in the modern
C# 14 extension-block style. The public CombineLatest operator lives in
`SignalAsyncExtensions` as an `extension<T1>(IObservableAsync<T1> src1) { ... }`
block; the supporting internal observable + coordinator types are nested inside
the same partial class.

The emitted files are intended to look hand-written: there is deliberately no
"auto-generated" banner. Run from anywhere; paths are resolved relative to this
script's directory.

    python3 Operators/gen_combinelatest.py
"""

import os

NUMBER_WORDS = {
    2: "two",
    3: "three",
    4: "four",
    5: "five",
    6: "six",
    7: "seven",
    8: "eight",
    9: "nine",
    10: "ten",
    11: "eleven",
    12: "twelve",
    13: "thirteen",
    14: "fourteen",
    15: "fifteen",
    16: "sixteen",
}

HEADER = (
    "// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.\n"
    "// ReactiveUI Association Incorporated licenses this file to you under the MIT license.\n"
    "// See the LICENSE file in the project root for full license information.\n"
)


def type_list(n, *, with_result=True):
    parts = [f"T{i}" for i in range(1, n + 1)]
    if with_result:
        parts.append("TResult")
    return ", ".join(parts)


def func_type(n):
    return f"Func<{type_list(n)}>"


def signal_type(n):
    return f"CombineLatest{n}SignalAsync<{type_list(n)}>"


def generate(n):
    word = NUMBER_WORDS[n]
    lines = []
    a = lines.append

    a(HEADER.rstrip("\n"))
    a("")
    a("using System.Diagnostics.CodeAnalysis;")
    a("")
    a("using ReactiveUI.Primitives.Async.Internals;")
    a("")
    a("namespace ReactiveUI.Primitives.Async;")
    a("")
    a("/// <summary>")
    a(f"/// Provides the arity-{n} (<c>{word}</c>-source) <c>CombineLatest</c> extension method")
    a("/// and its supporting internal observable + subscription types.")
    a("/// </summary>")
    a("public static partial class SignalAsyncExtensions")
    a("{")

    # Receiver extension block docs (hoisted: summary, src1 param, T1 typeparam).
    a("    /// <summary>Combines the latest values from multiple asynchronous observable sources.</summary>")
    a('    /// <param name="src1">Source observable 1 whose latest value is combined.</param>')
    a('    /// <typeparam name="T1">The element type of source 1.</typeparam>')
    a("    extension<T1>(IObservableAsync<T1> src1)")
    a("    {")

    # Member docs.
    a("        /// <summary>")
    a(f"        /// Combines the latest values from {word} asynchronous observable sources into a single")
    a("        /// sequence, projecting them through <paramref name=\"selector\"/> whenever any source emits.")
    a("        /// </summary>")
    a("        /// <remarks>")
    a("        /// The returned sequence does not produce a value until every source has emitted at least")
    a("        /// once. After that, each new value from any source produces a fresh projection using the")
    a("        /// most recent value from each. Completion / failure of any source propagates downstream.")
    a("        /// </remarks>")
    for i in range(2, n + 1):
        a(f'        /// <typeparam name="T{i}">The element type of source {i}.</typeparam>')
    a('        /// <typeparam name="TResult">The projected element type.</typeparam>')
    for i in range(2, n + 1):
        a(f'        /// <param name="src{i}">Source observable {i} whose latest value is combined.</param>')
    a('        /// <param name="selector">Projects the latest value of every source into a result.</param>')
    a('        /// <returns>An observable sequence of projected results.</returns>')
    a("        [SuppressMessage(")
    a('            "Major Code Smell",')
    a('            "S107:Methods should not have too many parameters",')
    a('            Justification = "Has more than 7 parameters - just expected for arity-N CombineLatest operator surface.")]')
    a(f"        public IObservableAsync<TResult> CombineLatest<{type_list(n)[len('T1, '):]}>(")
    for i in range(2, n + 1):
        a(f"            IObservableAsync<T{i}> src{i},")
    a(f"            {func_type(n)} selector) =>")
    a(f"            new {signal_type(n)}(")
    src_args = ", ".join(f"src{i}" for i in range(1, n + 1))
    a(f"                new({src_args}),")
    a("                selector);")
    a("    }")
    a("")

    # Nested internal SignalAsync class.
    a(f"    /// <summary>Async observable that combines the latest values from {word} source sequences using a selector.</summary>")
    for i in range(1, n + 1):
        a(f'    /// <typeparam name="T{i}">Element type of source {i}.</typeparam>')
    a('    /// <typeparam name="TResult">The projected element type.</typeparam>')
    a('    /// <param name="sources">The bundled source observables.</param>')
    a('    /// <param name="selector">The selector that projects the latest values.</param>')
    a(f"    internal sealed class {signal_type(n)}(")
    a(f"        {signal_type(n)}.Sources sources,")
    a(f"        {func_type(n)} selector) : SignalAsync<TResult>")
    a("    {")

    # SubscribeAsyncCore. Methods precede nested types (SST1201), so this comes
    # before the nested Sources record below.
    a("        /// <inheritdoc/>")
    a("        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(")
    a("            IObserverAsync<TResult> observer,")
    a("            CancellationToken cancellationToken)")
    a("        {")
    a("            var subscription = new CombineLatestCoordinator(observer, sources, selector);")
    a("            subscription.Lifecycle.LinkExternalCancellation(cancellationToken);")
    a("            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(")
    a("                subscription,")
    a("                () => subscription.SubscribeSourcesAsync(cancellationToken));")
    a("        }")
    a("")

    # Sources record (a nested type, so it follows the methods per SST1201).
    a("        /// <summary>")
    a(f"        /// Bundles the {word} source observables so the subscription constructor stays at three")
    a("        /// parameters (observer, sources, selector) regardless of arity. Sonar S107 caps method /")
    a("        /// constructor parameter count; the bundle keeps the internal types compliant.")
    a("        /// </summary>")
    for i in range(1, n + 1):
        a(f'        /// <param name="Src{i}">Source observable {i}.</param>')
    a("        internal readonly record struct Sources(")
    for i in range(1, n + 1):
        suffix = ");" if i == n else ","
        a(f"            IObservableAsync<T{i}> Src{i}{suffix}")
    a("")

    # Coordinator.
    a("        /// <summary>")
    a("        /// Per-arity subscription holding the typed Optional slots, the pre-built indexed")
    a("        /// observers, the SubscribeAtAsync switch, and the selector invocation. Shared scaffolding")
    a("        /// (gate, lifecycle, ValuesLock, OnErrorResume, SubscribeSourcesAsync, DisposeAsync) lives")
    a("        /// in <see cref=\"CombineLatestCoordinatorBase{TResult}\"/>; the per-source OnNext / OnError /")
    a("        /// OnCompleted forwarding lives in <see cref=\"CombineLatestIndexedWitness{TSource, TResult}\"/>.")
    a("        /// </summary>")
    a("        internal sealed class CombineLatestCoordinator : CombineLatestCoordinatorBase<TResult>")
    a("        {")

    # Source bits.
    for i in range(1, n + 1):
        a(f"            /// <summary>Bit owned by source {i} inside the lifecycle's completion bitmask.</summary>")
        a(f"            private const int Source{i}Bit = 1 << {i - 1};")
        a("")

    # Sources field.
    a("            /// <summary>Bundled source observables.</summary>")
    a("            private readonly Sources _sources;")
    a("")

    # Selector field.
    a("            /// <summary>The result selector function.</summary>")
    a(f"            private readonly {func_type(n)} _selector;")
    a("")

    # Indexed observers.
    for i in range(1, n + 1):
        a(f"            /// <summary>Indexed observer for source {i}.</summary>")
        a(f"            private readonly CombineLatestIndexedWitness<T{i}, TResult> _obs{i};")
        a("")

    # Optional value slots.
    for i in range(1, n + 1):
        a(f"            /// <summary>Latest value from source {i}.</summary>")
        a(f"            private Optional<T{i}> _val{i} = Optional<T{i}>.Empty;")
        a("")

    # Constructor.
    a('            /// <summary>Initializes a new instance of the <see cref="CombineLatestCoordinator"/> class.</summary>')
    a('            /// <param name="observer">The downstream observer.</param>')
    a('            /// <param name="sources">The bundled source observables.</param>')
    a('            /// <param name="selector">The selector that projects the latest values.</param>')
    a("            public CombineLatestCoordinator(")
    a("                IObserverAsync<TResult> observer,")
    a("                Sources sources,")
    a(f"                {func_type(n)} selector)")
    a(f"                : base(observer, sourceCount: {n})")
    a("            {")
    a("                _sources = sources;")
    a("                _selector = selector;")
    for i in range(1, n + 1):
        a(f"                _obs{i} = new(this, Source{i}Bit, v => _val{i} = new(v));")
    a("            }")
    a("")

    # EmitLatestAsync. Low arities use a compact expression body; once the single
    # projection line would exceed the 200-char limit (S103) the statement body with
    # one selector argument per line is used instead, matching the arity-16 shape.
    a("            /// <inheritdoc/>")
    sel_args = ", ".join(f"values.V{i}" for i in range(1, n + 1))
    expr_line = f"                    ? Lifecycle.EmitDownstreamAsync(_selector({sel_args}))"
    if len(expr_line) <= 200:
        a("            internal override ValueTask EmitLatestAsync() =>")
        a("                TryReadValues(out var values)")
        a(expr_line)
        a("                    : default;")
    else:
        a("            internal override ValueTask EmitLatestAsync()")
        a("            {")
        a("                if (!TryReadValues(out var values))")
        a("                {")
        a("                    return default;")
        a("                }")
        a("")
        a("                var projected = _selector(")
        for i in range(1, n + 1):
            suffix = ");" if i == n else ","
            a(f"                            values.V{i}{suffix}")
        a("                return Lifecycle.EmitDownstreamAsync(projected);")
        a("            }")
    a("")

    # SubscribeAtAsync.
    a("            /// <inheritdoc/>")
    a("            [SuppressMessage(")
    a('                "Minor Code Smell",')
    a('                "S109:Magic numbers should not be used",')
    a('                Justification = "Switch dispatches on the 0..N-1 source index; naming each numeric arm would just rename the obvious.")]')
    a("            [SuppressMessage(")
    a('                "Major Code Smell",')
    a('                "S1541:Methods and properties should not be too complex",')
    a('                Justification = "Switch arm per source — the high arms-count IS the dispatch surface; splitting hurts readability more than it helps.")]')
    a("            protected override ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken) =>")
    a("                index switch")
    a("                {")
    for i in range(1, n + 1):
        label = "_" if i == n else str(i - 1)
        a(f"                    {label} => _sources.Src{i}.SubscribeAsync(_obs{i}, cancellationToken),")
    a("                };")
    a("")

    # TryReadValues.
    a("            /// <summary>")
    a("            /// Reads every source's latest value into a single snapshot. Returns <see langword=\"false\"/>")
    a("            /// (with <paramref name=\"values\"/> set to <see langword=\"default\"/>) until every source has")
    a("            /// produced at least one value.")
    a("            /// </summary>")
    a('            /// <param name="values">When the method returns <see langword="true"/>, the snapshot.</param>')
    a('            /// <returns><see langword="true"/> when every source has produced a value; otherwise <see langword="false"/>.</returns>')
    a("            [SuppressMessage(")
    a('                "Major Code Smell",')
    a('                "S1541:Methods and properties should not be too complex",')
    a('                Justification = "Short-circuited && chain over every source\'s Optional; the high condition count IS the snapshot semantic.")]')
    a("            private bool TryReadValues(out Values values)")
    a("            {")
    for i in range(1, n + 1):
        if i == 1:
            opener = "                if ("
        else:
            opener = "                    && "
        suffix = ")" if i == n else ""
        a(f"{opener}_val{i}.TryGetValue(out var v{i}){suffix}")
    a("                {")
    val_args = ", ".join(f"v{i}" for i in range(1, n + 1))
    a(f"                    values = new({val_args});")
    a("                    return true;")
    a("                }")
    a("")
    a("                values = default;")
    a("                return false;")
    a("            }")
    a("")

    # Values record (a nested type, so it follows the methods per SST1201).
    a("            /// <summary>Latest-value snapshot taken when every source has produced at least one value.</summary>")
    for i in range(1, n + 1):
        a(f'            /// <param name="V{i}">Latest value from source {i}.</param>')
    a("            internal readonly record struct Values(")
    for i in range(1, n + 1):
        suffix = ");" if i == n else ","
        a(f"                T{i} V{i}{suffix}")
    a("        }")
    a("    }")
    a("}")

    return "\n".join(lines) + "\n"


def main():
    out_dir = os.path.dirname(os.path.abspath(__file__))
    for n in range(2, 17):
        path = os.path.join(out_dir, f"CombineLatest{n}.cs")
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(generate(n))
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
