// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Text.RegularExpressions;
using R3;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks the complete synchronous ReactiveUI.Primitives.Extensions public helper surface.</summary>
public partial class ReactiveExtensionsComparisonBenchmarks
{
    /// <summary>Executes the <c>SystemReactiveAsSignal</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveAsSignal</c> result.</returns>
    private static int SystemReactiveAsSignal() =>
        DrainPrimitiveUnit(RxObservable.Select(RxObservable.Range(0, Count), static _ => RxVoid.Default));

    /// <summary>Executes the <c>SystemReactiveCatchAndReturn</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveCatchAndReturn</c> result.</returns>
    private static int SystemReactiveCatchAndReturn() =>
        DrainInt(RxObservable.Throw<int>(Boom).Catch(RxObservable.Return(Fallback)));

    /// <summary>Executes the <c>SystemReactiveCatchIgnore</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveCatchIgnore</c> result.</returns>
    private static int SystemReactiveCatchIgnore() =>
        DrainInt(RxObservable.Throw<int>(Boom).Catch(RxObservable.Empty<int>()));

    /// <summary>Executes the <c>SystemReactiveCombineLatestValuesAreAllFalse</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveCombineLatestValuesAreAllFalse</c> result.</returns>
    private static int SystemReactiveCombineLatestValuesAreAllFalse() =>
        DrainBool(BoolSources(ExtensionsLibrary.ReactiveUIExtensions, false).CombineLatest(ValuesAreAllFalse));

    /// <summary>Executes the <c>SystemReactiveCombineLatestValuesAreAllTrue</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveCombineLatestValuesAreAllTrue</c> result.</returns>
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

    /// <summary>Executes the <c>SystemReactiveFilter</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveFilter</c> result.</returns>
    private static int SystemReactiveFilter()
    {
        var regex = EvenRegex();
        return DrainString(RxObservable.Where(RxObservable.ToObservable(StringValues), value => regex.IsMatch(value)));
    }

    /// <summary>Executes the <c>SystemReactiveForEach</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveForEach</c> result.</returns>
    private static int SystemReactiveForEach() =>
        DrainInt(RxObservable.Return(Values.AsEnumerable()).SelectMany(static values => values));

    /// <summary>Executes the <c>SystemReactiveFromArray</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveFromArray</c> result.</returns>
    private static int SystemReactiveFromArray() =>
        DrainInt(RxObservable.ToObservable(Values));

    /// <summary>Executes the <c>SystemReactiveGetMax</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveGetMax</c> result.</returns>
    private static int SystemReactiveGetMax() =>
        DrainInt(RxObservable.CombineLatest(RxObservable.Return(FirstValue), RxObservable.Return(SecondValue), static (left, right) => Math.Max(left, right)));

    /// <summary>Executes the <c>SystemReactiveGetMin</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveGetMin</c> result.</returns>
    private static int SystemReactiveGetMin() =>
        DrainInt(RxObservable.CombineLatest(RxObservable.Return(FirstValue), RxObservable.Return(SecondValue), static (left, right) => Math.Min(left, right)));

    /// <summary>Executes the <c>SystemReactiveNot</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveNot</c> result.</returns>
    private static int SystemReactiveNot() =>
        DrainBool(RxObservable.Select(RxObservable.ToObservable(BooleanValues), static value => !value));

    /// <summary>Executes the <c>SystemReactivePairwise</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactivePairwise</c> result.</returns>
    private static int SystemReactivePairwise()
    {
        PairWitness observer = new();
        using var subscription = RxObservable.Select(
                RxObservable.Where(
                    RxObservable.Buffer(RxObservable.Range(0, Count), 2, 1),
                    static values => values.Count == 2),
                static values => (Previous: values[0], Current: values[1]))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>SystemReactiveReturn</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveReturn</c> result.</returns>
    private static int SystemReactiveReturn() =>
        DrainInt(RxObservable.Return(Value));

    /// <summary>Executes the <c>SystemReactiveScanWithInitial</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveScanWithInitial</c> result.</returns>
    private static int SystemReactiveScanWithInitial() =>
        DrainInt(RxObservable.Scan(RxObservable.Range(0, Count), 0, static (acc, value) => acc + value));

    /// <summary>Executes the <c>SystemReactiveSelectAsyncScenario</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveSelectAsyncScenario</c> result.</returns>
    private static int SystemReactiveSelectAsyncScenario() =>
        DrainInt(RxObservable.SelectMany(RxObservable.Range(0, Count), static value => RxObservable.FromAsync(() => Task.FromResult(value + 1))));

    /// <summary>Executes the <c>SystemReactiveSelectConstant</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveSelectConstant</c> result.</returns>
    private static int SystemReactiveSelectConstant() =>
        DrainInt(RxObservable.Select(RxObservable.Range(0, Count), static _ => Value));

    /// <summary>Executes the <c>SystemReactiveSelectManyThen</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveSelectManyThen</c> result.</returns>
    private static int SystemReactiveSelectManyThen() =>
        DrainInt(RxObservable.SelectMany(RxObservable.SelectMany(RxObservable.Return(Value), static value => RxObservable.Return(value + 1)), static value => RxObservable.Return(value + 1)));

    /// <summary>Executes the <c>SystemReactiveSkipWhileNull</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveSkipWhileNull</c> result.</returns>
    private static int SystemReactiveSkipWhileNull() =>
        DrainString(RxObservable.Select(RxObservable.ToObservable(NullableStrings).SkipWhile(static value => value is null), static value => value!));

    /// <summary>Executes the <c>SystemReactiveTakeUntil</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveTakeUntil</c> result.</returns>
    private static int SystemReactiveTakeUntil() =>
        DrainInt(RxObservable.Range(0, Count).TakeWhile(static value => value <= Match));

    /// <summary>Executes the <c>SystemReactiveToHotTask</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveToHotTask</c> result.</returns>
    private static int SystemReactiveToHotTask() =>
        GetCompletedResult(System.Reactive.Threading.Tasks.TaskObservableExtensions.ToTask(RxObservable.Return(Value)));

    /// <summary>Executes the <c>SystemReactiveWaitUntil</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveWaitUntil</c> result.</returns>
    private static int SystemReactiveWaitUntil() =>
        DrainInt(RxObservable.Range(0, Count).FirstAsync(static value => value == Match));

    /// <summary>Executes the <c>SystemReactiveWhereFalse</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveWhereFalse</c> result.</returns>
    private static int SystemReactiveWhereFalse() =>
        DrainBool(RxObservable.Where(RxObservable.ToObservable(BooleanValues), static value => !value));

    /// <summary>Executes the <c>SystemReactiveWhereIsNotNull</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveWhereIsNotNull</c> result.</returns>
    private static int SystemReactiveWhereIsNotNull() =>
        DrainString(RxObservable.Select(RxObservable.Where(RxObservable.ToObservable(NullableStrings), static value => value is not null), static value => value!));

    /// <summary>Executes the <c>SystemReactiveWhereSelect</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveWhereSelect</c> result.</returns>
    private static int SystemReactiveWhereSelect() =>
        DrainInt(RxObservable.Select(RxObservable.Where(RxObservable.Range(0, Count), static value => (value & 1) == 0), static value => value * ResultMultiplier));

    /// <summary>Executes the <c>SystemReactiveWhereTrue</c> benchmark helper.</summary>
    /// <returns>The <c>SystemReactiveWhereTrue</c> result.</returns>
    private static int SystemReactiveWhereTrue() =>
        DrainBool(RxObservable.Where(RxObservable.ToObservable(BooleanValues), static value => value));

    /// <summary>Executes the <c>R3AsSignal</c> benchmark helper.</summary>
    /// <returns>The <c>R3AsSignal</c> result.</returns>
    private static int R3AsSignal()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Select(R3.Observable.Range(0, Count), static _ => 1).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>R3CatchAndReturn</c> benchmark helper.</summary>
    /// <returns>The <c>R3CatchAndReturn</c> result.</returns>
    private static int R3CatchAndReturn()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Catch<int, Exception>(
                R3.Observable.Throw<int>(Boom),
                static _ => R3.Observable.Return(Fallback))
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>R3CatchIgnore</c> benchmark helper.</summary>
    /// <returns>The <c>R3CatchIgnore</c> result.</returns>
    private static int R3CatchIgnore()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Catch<int, Exception>(
                R3.Observable.Throw<int>(Boom),
                static _ => R3.Observable.Empty<int>())
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>R3FromArray</c> benchmark helper.</summary>
    /// <returns>The <c>R3FromArray</c> result.</returns>
    private static int R3FromArray()
    {
        IntR3Witness observer = new();
        using var subscription = R3.Observable.ToObservable(Values, CancellationToken.None).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>R3Not</c> benchmark helper.</summary>
    /// <returns>The <c>R3Not</c> result.</returns>
    private static int R3Not()
    {
        R3BoolWitness observer = new();
        using var subscription = R3.ObservableExtensions.Select(
                R3.Observable.ToObservable(BooleanValues, CancellationToken.None),
                static value => !value)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>R3Return</c> benchmark helper.</summary>
    /// <returns>The <c>R3Return</c> result.</returns>
    private static int R3Return()
    {
        IntR3Witness observer = new();
        using var subscription = R3.Observable.Return(Value).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>R3SelectConstant</c> benchmark helper.</summary>
    /// <returns>The <c>R3SelectConstant</c> result.</returns>
    private static int R3SelectConstant()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Select(R3.Observable.Range(0, Count), static _ => Value).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>R3WhereFalse</c> benchmark helper.</summary>
    /// <returns>The <c>R3WhereFalse</c> result.</returns>
    private static int R3WhereFalse()
    {
        R3BoolWitness observer = new();
        using var subscription = R3.ObservableExtensions.Where(
                R3.Observable.ToObservable(BooleanValues, CancellationToken.None),
                static value => !value)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>R3WhereIsNotNull</c> benchmark helper.</summary>
    /// <returns>The <c>R3WhereIsNotNull</c> result.</returns>
    private static int R3WhereIsNotNull()
    {
        R3CountingWitness<string> observer = new();
        var source = R3.Observable.ToObservable(NullableStrings, CancellationToken.None);
        var filtered = R3.ObservableExtensions.Where(source, static value => value is not null);
        using var subscription = R3.ObservableExtensions.Select(filtered, static value => value!).Subscribe(observer);
        return observer.ItemCount;
    }

    /// <summary>Executes the <c>R3WhereSelect</c> benchmark helper.</summary>
    /// <returns>The <c>R3WhereSelect</c> result.</returns>
    private static int R3WhereSelect()
    {
        IntR3Witness observer = new();
        using var subscription = R3.ObservableExtensions.Select(
                R3.ObservableExtensions.Where(R3.Observable.Range(0, Count), static value => (value & 1) == 0),
                static value => value * ResultMultiplier)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>R3WhereTrue</c> benchmark helper.</summary>
    /// <returns>The <c>R3WhereTrue</c> result.</returns>
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
