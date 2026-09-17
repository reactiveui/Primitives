// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text.RegularExpressions;
using BenchmarkDotNet.Attributes;
using R3;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using R3IntSubject = R3.Subject<int>;
using RxIntSubject = System.Reactive.Subjects.Subject<int>;
using RxObservable = System.Reactive.Linq.Observable;
using RxStringSubject = System.Reactive.Subjects.Subject<string>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures fused filter/projection operators and null and regex filters over hot streams.</summary>
[MemoryDiagnoser]
public partial class ExtensionValueFilterBenchmarks
{
    /// <summary>The number of values pushed through each pipeline.</summary>
    private const int Count = 256;

    /// <summary>The multiplier applied by the projection.</summary>
    private const int Multiplier = 3;

    /// <summary>The constant emitted by the constant projection.</summary>
    private const int Constant = 7;

    /// <summary>The number of distinct strings in the text payload.</summary>
    private const int TextCycle = 10;

    /// <summary>The text payload; every third entry is null and the first entries are null.</summary>
    private static readonly string?[] Text = CreateText();

    /// <summary>Filters even values and scales them in one fused operator.</summary>
    /// <returns>The sum of the projected values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesWhereSelect()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.WhereSelect(source, static value => (value & 1) == 0, static value => value * Multiplier)
            .Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Filters even values and scales them using System.Reactive.</summary>
    /// <returns>The sum of the projected values.</returns>
    [Benchmark]
    public int SystemReactiveWhereSelect()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Select(
                RxObservable.Where(source, static value => (value & 1) == 0),
                static value => value * Multiplier)
            .Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Filters even values and scales them using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the projected values.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsWhereSelect()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.WhereSelect(source, static value => (value & 1) == 0, static value => value * Multiplier)
            .Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Filters even values and scales them using R3.</summary>
    /// <returns>The sum of the projected values.</returns>
    [Benchmark]
    public int R3WhereSelect()
    {
        IntR3Witness observer = new();
        using R3IntSubject source = new();
        using var subscription = source.Where(static value => (value & 1) == 0).Select(static value => value * Multiplier)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Replaces every value with a constant.</summary>
    /// <returns>The sum of the emitted constants.</returns>
    [Benchmark]
    public int PrimitivesSelectConstant()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.SelectConstant(source, Constant).Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Replaces every value with a constant using System.Reactive.</summary>
    /// <returns>The sum of the emitted constants.</returns>
    [Benchmark]
    public int SystemReactiveSelectConstant()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Select(source, static _ => Constant).Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Replaces every value with a constant using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted constants.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsSelectConstant()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.SelectConstant(source, Constant).Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Projects even values to text and drops the null projections.</summary>
    /// <returns>The total length of the emitted strings.</returns>
    [Benchmark]
    public int PrimitivesTrySelect()
    {
        StringLengthWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.TrySelect(source, FormatEven).Subscribe(observer);
        PushRange(source);
        return observer.TotalLength;
    }

    /// <summary>Projects even values to text and drops the null projections using System.Reactive.</summary>
    /// <returns>The total length of the emitted strings.</returns>
    [Benchmark]
    public int SystemReactiveTrySelect()
    {
        StringLengthWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Where(RxObservable.Select(source, FormatEven), static text => text is not null)
            .Subscribe(observer);
        PushRange(source);
        return observer.TotalLength;
    }

    /// <summary>Projects even values to text and drops the null projections using ReactiveUI.Extensions.</summary>
    /// <returns>The total length of the emitted strings.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsTrySelect()
    {
        StringLengthWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.TrySelect(source, FormatEven).Subscribe(observer);
        PushRange(source);
        return observer.TotalLength;
    }

    /// <summary>Drops null strings from the stream.</summary>
    /// <returns>The total length of the emitted strings.</returns>
    [Benchmark]
    public int PrimitivesWhereIsNotNull()
    {
        StringLengthWitness observer = new();
        using Signal<string?> source = new();
        using var subscription = PrimitivesExtensions.WhereIsNotNull(source).Subscribe(observer);
        PushText(source);
        return observer.TotalLength;
    }

    /// <summary>Drops null strings from the stream using System.Reactive.</summary>
    /// <returns>The total length of the emitted strings.</returns>
    [Benchmark]
    public int SystemReactiveWhereIsNotNull()
    {
        StringLengthWitness observer = new();
        using System.Reactive.Subjects.Subject<string?> source = new();
        using var subscription = RxObservable.Where(source, static text => text is not null).Subscribe(observer);
        PushText(source);
        return observer.TotalLength;
    }

    /// <summary>Drops null strings from the stream using ReactiveUI.Extensions.</summary>
    /// <returns>The total length of the emitted strings.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsWhereIsNotNull()
    {
        StringLengthWitness observer = new();
        using System.Reactive.Subjects.Subject<string?> source = new();
        using var subscription = PackageExtensions.WhereIsNotNull(source).Subscribe(observer);
        PushText(source);
        return observer.TotalLength;
    }

    /// <summary>Skips the leading null strings, then forwards everything.</summary>
    /// <returns>The number of values that reached the observer.</returns>
    [Benchmark]
    public int PrimitivesSkipWhileNull()
    {
        StringLengthWitness observer = new();
        using Signal<string> source = new();
        using var subscription = PrimitivesExtensions.SkipWhileNull(source).Subscribe(observer);
        PushText(source!);
        return observer.ItemCount;
    }

    /// <summary>Skips the leading null strings using System.Reactive.</summary>
    /// <returns>The number of values that reached the observer.</returns>
    [Benchmark]
    public int SystemReactiveSkipWhileNull()
    {
        StringLengthWitness observer = new();
        using RxStringSubject source = new();
        using var subscription = RxObservable.SkipWhile(source, static text => text is null).Subscribe(observer);
        PushText(source!);
        return observer.ItemCount;
    }

    /// <summary>Skips the leading null strings using ReactiveUI.Extensions.</summary>
    /// <returns>The number of values that reached the observer.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsSkipWhileNull()
    {
        StringLengthWitness observer = new();
        using RxStringSubject source = new();
        using var subscription = PackageExtensions.SkipWhileNull(source).Subscribe(observer);
        PushText(source!);
        return observer.ItemCount;
    }

    /// <summary>Keeps the strings that match a regex.</summary>
    /// <returns>The number of matching strings.</returns>
    [Benchmark]
    public int PrimitivesRegexFilter()
    {
        StringLengthWitness observer = new();
        using Signal<string> source = new();
        using var subscription = PrimitivesExtensions.Filter(source, EvenDigitRegex()).Subscribe(observer);
        PushDigits(source);
        return observer.ItemCount;
    }

    /// <summary>Keeps the strings that match a regex using System.Reactive.</summary>
    /// <returns>The number of matching strings.</returns>
    [Benchmark]
    public int SystemReactiveRegexFilter()
    {
        StringLengthWitness observer = new();
        using RxStringSubject source = new();
        var regex = EvenDigitRegex();
        using var subscription = RxObservable.Where(source, text => regex.IsMatch(text)).Subscribe(observer);
        PushDigits(source);
        return observer.ItemCount;
    }

    /// <summary>Keeps the strings that match a regex using ReactiveUI.Extensions.</summary>
    /// <returns>The number of matching strings.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsRegexFilter()
    {
        StringLengthWitness observer = new();
        using RxStringSubject source = new();
        using var subscription = PackageExtensions.Filter(source, EvenDigitRegex()).Subscribe(observer);
        PushDigits(source);
        return observer.ItemCount;
    }

    /// <summary>Formats even values and returns null for odd values.</summary>
    /// <param name="value">The value to format.</param>
    /// <returns>The formatted value, or null when the value is odd.</returns>
    private static string? FormatEven(int value) =>
        (value & 1) == 0 ? value.ToString(CultureInfo.InvariantCulture) : null;

    /// <summary>Pushes the ascending range into the subject.</summary>
    /// <param name="source">The subject to push into.</param>
    private static void PushRange(IObserver<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }
    }

    /// <summary>Pushes the text payload into the subject.</summary>
    /// <param name="source">The subject to push into.</param>
    private static void PushText(IObserver<string?> source)
    {
        for (var i = 0; i < Text.Length; i++)
        {
            source.OnNext(Text[i]);
        }
    }

    /// <summary>Pushes single-digit strings into the subject.</summary>
    /// <param name="source">The subject to push into.</param>
    private static void PushDigits(IObserver<string> source)
    {
        for (var i = 0; i < Text.Length; i++)
        {
            source.OnNext(Text[i] ?? string.Empty);
        }
    }

    /// <summary>Builds the text payload: single digits with a null leading run and a null every third slot.</summary>
    /// <returns>The text payload.</returns>
    private static string?[] CreateText()
    {
        var text = new string?[Count];
        for (var i = 0; i < text.Length; i++)
        {
            text[i] = i % Multiplier == 0 ? null : (i % TextCycle).ToString(CultureInfo.InvariantCulture);
        }

        return text;
    }

    /// <summary>Creates the generated even-digit regex.</summary>
    /// <returns>The generated regex instance.</returns>
    [GeneratedRegex("^[02468]$")]
    private static partial Regex EvenDigitRegex();

    /// <summary>Observer that counts strings and sums their lengths, counting null as zero.</summary>
    private sealed class StringLengthWitness : IObserver<string?>
    {
        /// <summary>Gets the number of strings observed.</summary>
        public int ItemCount { get; private set; }

        /// <summary>Gets the summed length of the strings observed.</summary>
        public int TotalLength { get; private set; }

        /// <inheritdoc/>
        public void OnNext(string? value)
        {
            ItemCount++;
            TotalLength += value?.Length ?? 0;
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }
}
