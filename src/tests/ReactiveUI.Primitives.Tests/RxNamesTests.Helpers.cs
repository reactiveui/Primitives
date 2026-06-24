// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Helper methods and case records for Rx-name parity tests.</summary>
public partial class RxNamesTests
{
    /// <summary>Runs a unary operator over a cold source and collects the forwarded values.</summary>
    /// <param name = "op">The operator under test.</param>
    /// <param name = "input">The source values.</param>
    /// <returns>The forwarded values.</returns>
    private static List<int> RunUnary(Func<IObservable<int>, IObservable<int>> op, int[] input)
    {
        List<int> values = [];
        _ = op(Signal.FromEnumerable(input)).Subscribe(values.Add);
        return values;
    }

    /// <summary>Runs a higher-order operator over a source of cold inner sources and collects the forwarded values.</summary>
    /// <param name = "op">The operator under test.</param>
    /// <param name = "inners">The inner source values.</param>
    /// <returns>The forwarded values.</returns>
    private static List<int> RunHigherOrder(Func<IObservable<IObservable<int>>, IObservable<int>> op, int[][] inners)
    {
        var outer = Signal.FromEnumerable(Array.ConvertAll(inners, ToSource));
        List<int> values = [];
        _ = op(outer).Subscribe(values.Add);
        return values;
    }

    /// <summary>Wraps an inner value array in a cold source.</summary>
    /// <param name = "inner">The inner values.</param>
    /// <returns>A cold source over the inner values.</returns>
    private static IObservable<int> ToSource(int[] inner) => Signal.FromEnumerable(inner);

    /// <summary>Runs a binary operator over two manual subjects driven by a script and collects the forwarded values.</summary>
    /// <param name = "op">The operator under test.</param>
    /// <param name = "drive">The script that pushes values into the subjects.</param>
    /// <returns>The forwarded values.</returns>
    private static List<int> RunBinary(
        Func<IObservable<int>, IObservable<int>, IObservable<int>> op,
        Action<Signal<int>, Signal<int>> drive)
    {
        Signal<int> left = new();
        Signal<int> right = new();
        List<int> values = [];
        using var subscription = op(left, right).Subscribe(values.Add);
        drive(left, right);
        return values;
    }

    /// <summary>Runs a time-based operator against a virtual clock and collects the forwarded values and any error.</summary>
    /// <param name = "op">The operator under test.</param>
    /// <param name = "source">The source factory.</param>
    /// <returns>The forwarded values and any terminal error.</returns>
    private static (List<int> Values, Exception? Error) RunTimed(
        Func<IObservable<int>, ISequencer, IObservable<int>> op,
        Func<IObservable<int>> source)
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        List<int> values = [];
        Exception? error = null;
        using var subscription = op(source(), clock).Subscribe(values.Add, captured => error = captured, () => { });
        clock.AdvanceBy(TimeSpan.FromTicks(AdvanceTicks));
        return (values, error);
    }

    /// <summary>Pushes one value then an error through a stateful sink and reports whether both were forwarded.</summary>
    /// <param name = "op">The stateful operator under test.</param>
    /// <returns><see langword="true"/> when one value and the error were forwarded.</returns>
    private static bool RunStatefulError(Func<IObservable<int>, IObservable<int>> op)
    {
        Signal<int> source = new();
        List<int> values = [];
        Exception? error = null;
        using var subscription = op(source).Subscribe(values.Add, captured => error = captured, () => { });
        source.OnNext(Two);
        source.OnError(new InvalidOperationException(Boom));
        return values.Count == One && error is InvalidOperationException;
    }

    /// <summary>Pushes a value through a sink whose projection throws and reports whether the error was forwarded.</summary>
    /// <param name = "op">The stateful operator under test.</param>
    /// <returns><see langword="true"/> when the thrown error was forwarded downstream.</returns>
    private static bool RunStatefulThrow(Func<IObservable<int>, IObservable<int>> op)
    {
        Signal<int> source = new();
        Exception? error = null;
        using var subscription = op(source).Subscribe(
            static _ => { },
            captured => error = captured,
            () => { });
        source.OnNext(One);
        return error is InvalidOperationException;
    }

    /// <summary>A stateful projection that always throws (drives the sink catch path).</summary>
    /// <param name = "state">The unused state.</param>
    /// <param name = "value">The unused value.</param>
    /// <returns>Never returns; always throws.</returns>
    private static int ThrowProjection(int state, int value) => throw new InvalidOperationException(Boom);

    /// <summary>A stateful predicate that always throws (drives the sink catch path).</summary>
    /// <param name = "state">The unused state.</param>
    /// <param name = "value">The unused value.</param>
    /// <returns>Never returns; always throws.</returns>
    private static bool ThrowPredicate(int state, int value) => throw new InvalidOperationException(Boom);

    /// <summary>Runs a sampling operator against a virtual clock with a fixed drive and collects the sampled values.</summary>
    /// <param name = "op">The sampling operator under test.</param>
    /// <returns>The sampled values.</returns>
    private static List<int> RunSampling(Func<IObservable<int>, ISequencer, IObservable<int>> op)
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];
        using var subscription = op(source, clock).Subscribe(values.Add);
        source.OnNext(One);
        clock.AdvanceBy(TimeSpan.FromTicks(Two));
        source.OnNext(Three);
        clock.AdvanceBy(TimeSpan.FromTicks(Two));
        return values;
    }

    /// <summary>Combines a source value with an inner value (result selector for the 3-arg SelectMany/FlatMap).</summary>
    /// <param name = "source">The source value.</param>
    /// <param name = "inner">The inner value.</param>
    /// <returns>The combined value.</returns>
    private static int AddPair(int source, int inner) => source + inner;

    /// <summary>Creates the requested multi-source CombineLatest overload.</summary>
    /// <param name="arity">The arity to create.</param>
    /// <param name="sources">The source signals.</param>
    /// <returns>The combined observable.</returns>
    [SuppressMessage(
        "Style",
        "S1541:Methods and properties should not be too complex",
        Justification = "Compile-time overload coverage intentionally invokes each generated arity.")]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "Compile-time overload coverage intentionally invokes high-arity selector lambdas.")]
    [SuppressMessage(
        "Major Code Smell",
        "S109:Magic numbers should not be used",
        Justification = "Compile-time overload coverage indexes each source slot by arity.")]
    [SuppressMessage(
        "Major Code Smell",
        "S138:Functions should not have too many lines of code",
        Justification = "Compile-time overload coverage keeps the arity switch in one audited helper.")]
    private static IObservable<int> CreateCombineLatest(int arity, Signal<int>[] sources) =>
        arity switch
        {
            4 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                static (value1, value2, value3, value4) =>
                    value1 + value2 + value3 + value4),
            5 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                static (value1, value2, value3, value4, value5) =>
                    value1 + value2 + value3 + value4 + value5),
            6 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                static (value1, value2, value3, value4, value5, value6) =>
                    value1 + value2 + value3 + value4 + value5 + value6),
            7 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                static (value1, value2, value3, value4, value5, value6, value7) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7),
            8 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                static (value1, value2, value3, value4, value5, value6, value7, value8) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8),
            9 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                static (value1, value2, value3, value4, value5, value6, value7, value8, value9) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9),
            10 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                static (value1, value2, value3, value4, value5, value6, value7, value8, value9, value10) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10),
            11 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                static (value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10 +
                    value11),
            12 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                static (
                    value1,
                    value2,
                    value3,
                    value4,
                    value5,
                    value6,
                    value7,
                    value8,
                    value9,
                    value10,
                    value11,
                    value12) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10 +
                    value11 + value12),
            13 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                sources[12],
                static (
                    value1,
                    value2,
                    value3,
                    value4,
                    value5,
                    value6,
                    value7,
                    value8,
                    value9,
                    value10,
                    value11,
                    value12,
                    value13) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10 +
                    value11 + value12 + value13),
            14 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                sources[12],
                sources[13],
                static (
                    value1,
                    value2,
                    value3,
                    value4,
                    value5,
                    value6,
                    value7,
                    value8,
                    value9,
                    value10,
                    value11,
                    value12,
                    value13,
                    value14) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10 +
                    value11 + value12 + value13 + value14),
            15 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                sources[12],
                sources[13],
                sources[14],
                static (
                    value1,
                    value2,
                    value3,
                    value4,
                    value5,
                    value6,
                    value7,
                    value8,
                    value9,
                    value10,
                    value11,
                    value12,
                    value13,
                    value14,
                    value15) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10 +
                    value11 + value12 + value13 + value14 + value15),
            _ => throw new ArgumentOutOfRangeException(nameof(arity), arity, null)
        };

    /// <summary>Sums sixteen values for the widest CombineLatest overload.</summary>
    /// <param name="value1">Value 1.</param>
    /// <param name="value2">Value 2.</param>
    /// <param name="value3">Value 3.</param>
    /// <param name="value4">Value 4.</param>
    /// <param name="value5">Value 5.</param>
    /// <param name="value6">Value 6.</param>
    /// <param name="value7">Value 7.</param>
    /// <param name="value8">Value 8.</param>
    /// <param name="value9">Value 9.</param>
    /// <param name="value10">Value 10.</param>
    /// <param name="value11">Value 11.</param>
    /// <param name="value12">Value 12.</param>
    /// <param name="value13">Value 13.</param>
    /// <param name="value14">Value 14.</param>
    /// <param name="value15">Value 15.</param>
    /// <param name="value16">Value 16.</param>
    /// <returns>The sum of all values.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "Has more than 7 parameters - required to exercise the arity-16 CombineLatest selector.")]
    private static int SumSixteen(
        int value1,
        int value2,
        int value3,
        int value4,
        int value5,
        int value6,
        int value7,
        int value8,
        int value9,
        int value10,
        int value11,
        int value12,
        int value13,
        int value14,
        int value15,
        int value16) =>
        value1 +
        value2 +
        value3 +
        value4 +
        value5 +
        value6 +
        value7 +
        value8 +
        value9 +
        value10 +
        value11 +
        value12 +
        value13 +
        value14 +
        value15 +
        value16;

    /// <summary>Subscribes to a source and collects its forwarded values.</summary>
    /// <param name = "source">The source sequence.</param>
    /// <returns>The forwarded values.</returns>
    private static List<int> Collect(IObservable<int> source)
    {
        List<int> values = [];
        _ = source.Subscribe(values.Add);
        return values;
    }

    /// <summary>Builds a source of two int-range inner sources (exercises the synchronous Switch range fast path).</summary>
    /// <returns>An outer source of two range inners.</returns>
    private static IObservable<IObservable<int>> RangeInners() =>
        Signal.FromEnumerable([Signal.Sequence(One, Two), Signal.Sequence(Three, Two)]);

    /// <summary>
    /// Drives a stateful sink through a value, a terminal completion, and then further notifications, reporting
    /// whether the post-terminal notifications were dropped (exactly one completion, no leaked error).
    /// </summary>
    /// <param name = "op">The stateful operator under test.</param>
    /// <returns><see langword="true"/> when notifications after the terminal were dropped.</returns>
    private static bool RunStopGuards(Func<IObservable<int>, IObservable<int>> op)
    {
        ManualSource<int> source = new();
        var completed = 0;
        Exception? error = null;
        using var subscription = op(source).Subscribe(
            static _ => { },
            captured => error = captured,
            () => completed++);
        source.Next(Two);
        source.Complete();
        source.Next(Three);
        source.Error(new InvalidOperationException(Boom));
        source.Complete();
        return completed == One && error is null;
    }

    /// <summary>
    /// An observable whose subscription retains its observer and ignores disposal, letting a test push raw
    /// notifications (including ones after a terminal notification) to exercise a sink's terminal guards.
    /// </summary>
    /// <typeparam name = "T">The element type.</typeparam>
    private sealed class ManualSource<T> : IObservable<T>
    {
        /// <summary>The observer retained from the most recent subscription.</summary>
        private IObserver<T>? _observer;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observer = observer;
            return EmptyDisposable.Instance;
        }

        /// <summary>Pushes a value to the retained observer.</summary>
        /// <param name = "value">The value to push.</param>
        public void Next(T value) => _observer?.OnNext(value);

        /// <summary>Pushes an error to the retained observer.</summary>
        /// <param name = "exception">The error to push.</param>
        public void Error(Exception exception) => _observer?.OnError(exception);

        /// <summary>Pushes completion to the retained observer.</summary>
        public void Complete() => _observer?.OnCompleted();
    }

    /// <summary>A source that reports it requires current-thread subscription (drives the sink's propagation check).</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    private sealed class CurrentThreadSource<T> : IRequireCurrentThread<T>
    {
        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => true;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer) => EmptyDisposable.Instance;
    }

    /// <summary>A unary parity case: a Primitives-named builder and its Rx-named twin over one source.</summary>
    /// <param name = "Name">The pair name.</param>
    /// <param name = "Deviant">The Primitives-named builder.</param>
    /// <param name = "Rx">The Rx/LINQ-named builder.</param>
    /// <param name = "Input">The source values.</param>
    /// <param name = "Expected">The expected forwarded values.</param>
    public sealed record UnaryCase(
        string Name,
        Func<IObservable<int>, IObservable<int>> Deviant,
        Func<IObservable<int>, IObservable<int>> Rx,
        int[] Input,
        int[] Expected)
    {
        /// <inheritdoc/>
        public override string ToString() => Name;
    }

    /// <summary>A higher-order parity case operating over a source of inner sources.</summary>
    /// <param name = "Name">The pair name.</param>
    /// <param name = "Deviant">The Primitives-named builder.</param>
    /// <param name = "Rx">The Rx/LINQ-named builder.</param>
    /// <param name = "Inners">The inner source values.</param>
    /// <param name = "Expected">The expected forwarded values.</param>
    [SuppressMessage(
        "Major Code Smell",
        "S2368:Public methods should not have multidimensional array parameters",
        Justification = "The jagged array is the public TUnit method-data shape for higher-order parity cases.")]
    public sealed record HigherOrderCase(
        string Name,
        Func<IObservable<IObservable<int>>, IObservable<int>> Deviant,
        Func<IObservable<IObservable<int>>, IObservable<int>> Rx,
        int[][] Inners,
        int[] Expected)
    {
        /// <inheritdoc/>
        public override string ToString() => Name;
    }

    /// <summary>A binary parity case driven by a scripted interleaving of two manual subjects.</summary>
    /// <param name = "Name">The pair name.</param>
    /// <param name = "Deviant">The Primitives-named builder.</param>
    /// <param name = "Rx">The Rx/LINQ-named builder.</param>
    /// <param name = "Drive">The script that pushes values into the left and right subjects.</param>
    /// <param name = "Expected">The expected forwarded values.</param>
    public sealed record BinaryCase(
        string Name,
        Func<IObservable<int>, IObservable<int>, IObservable<int>> Deviant,
        Func<IObservable<int>, IObservable<int>, IObservable<int>> Rx,
        Action<Signal<int>, Signal<int>> Drive,
        int[] Expected)
    {
        /// <inheritdoc/>
        public override string ToString() => Name;
    }

    /// <summary>A time-based parity case driven by a virtual clock.</summary>
    /// <param name = "Name">The pair name.</param>
    /// <param name = "Deviant">The Primitives-named builder.</param>
    /// <param name = "Rx">The Rx/LINQ-named builder.</param>
    /// <param name = "Source">The source factory.</param>
    /// <param name = "Expected">The expected forwarded values.</param>
    /// <param name = "ExpectsTimeout">Whether a <see cref = "TimeoutException"/> is expected.</param>
    public sealed record TimeCase(
        string Name,
        Func<IObservable<int>, ISequencer, IObservable<int>> Deviant,
        Func<IObservable<int>, ISequencer, IObservable<int>> Rx,
        Func<IObservable<int>> Source,
        int[] Expected,
        bool ExpectsTimeout)
    {
        /// <inheritdoc/>
        public override string ToString() => Name;
    }
}
