// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using R3;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks the complete synchronous ReactiveUI.Primitives.Extensions public helper surface.</summary>
public partial class ReactiveExtensionsComparisonBenchmarks
{
    /// <summary>Projects the System.Reactive range to unit values and drains it.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveAsSignal() =>
        DrainPrimitiveUnit(RxObservable.Select(RxObservable.Range(0, Count), static _ => RxVoid.Default));

    /// <summary>Substitutes a fallback value for a System.Reactive failure.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveCatchAndReturn() =>
        DrainInt(RxObservable.Throw<int>(Boom).Catch(RxObservable.Return(Fallback)));

    /// <summary>Swallows a System.Reactive failure and completes empty.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveCatchIgnore() =>
        DrainInt(RxObservable.Throw<int>(Boom).Catch(RxObservable.Empty<int>()));

    /// <summary>Combines two false System.Reactive sources and tests that every value is false.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveCombineLatestValuesAreAllFalse() =>
        DrainBool(BoolSources(ExtensionsLibrary.ReactiveUIExtensions, false).CombineLatest(ValuesAreAllFalse));

    /// <summary>Combines two true System.Reactive sources and tests that every value is true.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveCombineLatestValuesAreAllTrue() =>
        DrainBool(BoolSources(ExtensionsLibrary.ReactiveUIExtensions, true).CombineLatest(ValuesAreAllTrue));

    /// <summary>Determines whether every value is false.</summary>
    /// <param name="values">The values to inspect.</param>
    /// <returns><see langword="true"/> when every value is false; otherwise, <see langword="false"/>.</returns>
    private static bool ValuesAreAllFalse(IList<bool> values)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (values[i])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Determines whether every value is true.</summary>
    /// <param name="values">The values to inspect.</param>
    /// <returns><see langword="true"/> when every value is true; otherwise, <see langword="false"/>.</returns>
    private static bool ValuesAreAllTrue(IList<bool> values)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (!values[i])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Filters the System.Reactive string source by the even-digit regex.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int SystemReactiveFilter()
    {
        var regex = EvenRegex();
        return DrainString(RxObservable.Where(RxObservable.ToObservable(StringValues), value => regex.IsMatch(value)));
    }

    /// <summary>Flattens a single batch of values through System.Reactive.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveForEach() =>
        DrainInt(RxObservable.Return(Values.AsEnumerable()).SelectMany(static values => values));

    /// <summary>Drains the shared int array through System.Reactive.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveFromArray() =>
        DrainInt(RxObservable.ToObservable(Values));

    /// <summary>Combines two System.Reactive scalars into their maximum.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveGetMax() =>
        DrainInt(RxObservable.CombineLatest(
            RxObservable.Return(FirstValue),
            RxObservable.Return(SecondValue),
            Math.Max));

    /// <summary>Combines two System.Reactive scalars into their minimum.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveGetMin() =>
        DrainInt(RxObservable.CombineLatest(
            RxObservable.Return(FirstValue),
            RxObservable.Return(SecondValue),
            Math.Min));

    /// <summary>Negates each boolean emitted by the System.Reactive source.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveNot() =>
        DrainBool(RxObservable.Select(RxObservable.ToObservable(BooleanValues), static value => !value));

    /// <summary>Rebuilds pairwise semantics from a System.Reactive sliding buffer.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int SystemReactivePairwise()
    {
        PairWitness observer = new();
        using var subscription = RxObservable.Select(
                RxObservable.Where(
                    RxObservable.Buffer(RxObservable.Range(0, Count), PairwiseWindow, 1),
                    static values => values.Count == PairwiseWindow),
                static values => (Previous: values[0], Current: values[1]))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Drains a single-value System.Reactive source.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveReturn() =>
        DrainInt(RxObservable.Return(Value));

    /// <summary>Accumulates the System.Reactive range from a seed value.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveScanWithInitial() =>
        DrainInt(RxObservable.Scan(RxObservable.Range(0, Count), 0, static (acc, value) => acc + value));

    /// <summary>Projects each System.Reactive value through a completed task.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveSelectAsyncScenario() =>
        DrainInt(RxObservable.SelectMany(
            RxObservable.Range(0, Count),
            static value => RxObservable.FromAsync(() => Task.FromResult(value + 1))));

    /// <summary>Replaces every value in the System.Reactive range with a constant.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveSelectConstant() =>
        DrainInt(RxObservable.Select(RxObservable.Range(0, Count), static _ => Value));

    /// <summary>Chains two sequential System.Reactive projections.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveSelectManyThen() =>
        DrainInt(RxObservable.SelectMany(
            RxObservable.SelectMany(RxObservable.Return(Value), static value => RxObservable.Return(value + 1)),
            static value => RxObservable.Return(value + 1)));

    /// <summary>Skips the leading nulls of the System.Reactive string source.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveSkipWhileNull() =>
        DrainString(RxObservable.Select(
            RxObservable.ToObservable(NullableStrings).SkipWhile(static value => value is null),
            static value => value!));

    /// <summary>Truncates the System.Reactive range at the match threshold.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveTakeUntil() =>
        DrainInt(RxObservable.Range(0, Count).TakeWhile(static value => value <= Match));

    /// <summary>Converts a single-value System.Reactive source to a task and waits for it.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveToHotTask() =>
        GetCompletedResult(System.Reactive.Threading.Tasks.TaskObservableExtensions.ToTask(RxObservable.Return(Value)));

    /// <summary>Takes the first System.Reactive value that matches the threshold.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveWaitUntil() =>
        DrainInt(RxObservable.Range(0, Count).FirstAsync(static value => value == Match));

    /// <summary>Keeps only the false values of the System.Reactive boolean source.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveWhereFalse() =>
        DrainBool(RxObservable.Where(RxObservable.ToObservable(BooleanValues), static value => !value));

    /// <summary>Keeps only the non-null values of the System.Reactive string source.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveWhereIsNotNull() =>
        DrainString(RxObservable.Select(
            RxObservable.Where(RxObservable.ToObservable(NullableStrings), static value => value is not null),
            static value => value!));

    /// <summary>Filters the System.Reactive range to even values and scales them.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveWhereSelect() =>
        DrainInt(RxObservable.Select(
            RxObservable.Where(RxObservable.Range(0, Count), static value => (value & 1) == 0),
            static value => value * ResultMultiplier));

    /// <summary>Keeps only the true values of the System.Reactive boolean source.</summary>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SystemReactiveWhereTrue() =>
        DrainBool(RxObservable.Where(RxObservable.ToObservable(BooleanValues), static value => value));

    /// <summary>Projects the R3 range to a constant and drains it.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3AsSignal()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Select(R3.Observable.Range(0, Count), static _ => 1)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Substitutes a fallback value for an R3 failure.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3CatchAndReturn()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Catch<int, Exception>(
                R3.Observable.Throw<int>(Boom),
                static _ => R3.Observable.Return(Fallback))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Swallows an R3 failure and completes empty.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3CatchIgnore()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Catch<int, Exception>(
                R3.Observable.Throw<int>(Boom),
                static _ => R3.Observable.Empty<int>())
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Drains the shared int array through R3.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3FromArray()
    {
        IntR3Witness observer = new();
        using var subscription = R3.Observable.ToObservable(Values, CancellationToken.None).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Negates each boolean emitted by the R3 source.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3Not()
    {
        R3BoolWitness observer = new();
        using var subscription = R3.ObservableExtensions.Select(
                R3.Observable.ToObservable(BooleanValues, CancellationToken.None),
                static value => !value)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Drains a single-value R3 source.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3Return()
    {
        IntR3Witness observer = new();
        using var subscription = R3.Observable.Return(Value).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Replaces every value in the R3 range with a constant.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3SelectConstant()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Select(R3.Observable.Range(0, Count), static _ => Value)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Keeps only the false values of the R3 boolean source.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3WhereFalse()
    {
        R3BoolWitness observer = new();
        using var subscription = R3.ObservableExtensions.Where(
                R3.Observable.ToObservable(BooleanValues, CancellationToken.None),
                static value => !value)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Keeps only the non-null values of the R3 string source.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3WhereIsNotNull()
    {
        R3CountingWitness<string> observer = new();
        var source = R3.Observable.ToObservable(NullableStrings, CancellationToken.None);
        var filtered = R3.ObservableExtensions.Where(source, static value => value is not null);
        using var subscription = R3.ObservableExtensions.Select(filtered, static value => value!).Subscribe(observer);
        return observer.ItemCount;
    }

    /// <summary>Filters the R3 range to even values and scales them.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3WhereSelect()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Select(
                R3.ObservableExtensions.Where(R3.Observable.Range(0, Count), static value => (value & 1) == 0),
                static value => value * ResultMultiplier)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Keeps only the true values of the R3 boolean source.</summary>
    /// <returns>The scenario checksum.</returns>
    private static int R3WhereTrue()
    {
        R3BoolWitness observer = new();
        using var subscription = R3.ObservableExtensions.Where(
                R3.Observable.ToObservable(BooleanValues, CancellationToken.None),
                static value => value)
            .Subscribe(observer);
        return observer.Total;
    }
}
