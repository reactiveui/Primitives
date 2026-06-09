// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using System.Text.RegularExpressions;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using RxObservable = System.Reactive.Linq.Observable;
using RxUnit = System.Reactive.Unit;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks the complete synchronous ReactiveUI.Primitives.Extensions public helper surface.</summary>
public partial class ReactiveExtensionsComparisonBenchmarks
{
    /// <summary>Executes the <c>ArraySource</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>ArraySource</c> result.</returns>
    private static IObservable<int> ArraySource(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.FromArray(Values)
            : PackageExtensions.FromArray(Values);

    /// <summary>Executes the <c>BoolSource</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>BoolSource</c> result.</returns>
    private static IObservable<bool> BoolSource(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.FromArray(BooleanValues)
            : PackageExtensions.FromArray(BooleanValues);

    /// <summary>Executes the <c>EnsureCompleted</c> benchmark helper.</summary>
    /// <param name="task">The <c>task</c> value.</param>
    private static void EnsureCompleted(Task task)
    {
        while (!task.IsCompleted)
        {
            Thread.Yield();
        }

        if (task.IsCompletedSuccessfully)
        {
            return;
        }

        if (task.IsCanceled)
        {
            throw new TaskCanceledException(task);
        }

        throw new InvalidOperationException("The benchmark task should complete successfully.", task.Exception);
    }

    /// <summary>Executes the <c>EnsureCompleted</c> benchmark helper.</summary>
    /// <param name="task">The <c>task</c> value.</param>
    private static void EnsureCompleted(ValueTask task)
    {
        if (task.IsCompletedSuccessfully)
        {
            return;
        }

        EnsureCompleted(task.AsTask());
    }

    /// <summary>Executes the <c>GetCompletedResult</c> benchmark helper.</summary>
    /// <param name="task">The <c>task</c> value.</param>
    /// <returns>The <c>GetCompletedResult</c> result.</returns>
    private static int GetCompletedResult(Task<int> task)
    {
        EnsureCompleted(task);
        return Value;
    }

    /// <summary>Executes the <c>GetCompletedResult</c> benchmark helper.</summary>
    /// <param name="task">The <c>task</c> value.</param>
    /// <returns>The <c>GetCompletedResult</c> result.</returns>
    private static int GetCompletedResult(ValueTask<int> task)
    {
        if (!task.IsCompletedSuccessfully)
        {
            EnsureCompleted(task.AsTask());
        }

        return Value;
    }

    /// <summary>Executes the <c>BoolSources</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <param name="value">The <c>value</c> value.</param>
    /// <returns>The <c>BoolSources</c> result.</returns>
    private static IEnumerable<IObservable<bool>> BoolSources(ExtensionsLibrary library, bool value)
    {
        yield return library == ExtensionsLibrary.Primitives
            ? PrimitivesObservables.Return(value)
            : PackageObservables.Return(value);
        yield return library == ExtensionsLibrary.Primitives
            ? PrimitivesObservables.Return(value)
            : PackageObservables.Return(value);
    }

    /// <summary>Executes the <c>CompletedTasks</c> benchmark helper.</summary>
    /// <returns>The <c>CompletedTasks</c> result.</returns>
    private static IEnumerable<Task<int>> CompletedTasks()
    {
        for (var i = 0; i < Count; i++)
        {
            yield return Task.FromResult(i);
        }
    }

    /// <summary>Executes the <c>CreateValues</c> benchmark helper.</summary>
    /// <returns>The <c>CreateValues</c> result.</returns>
    private static int[] CreateValues()
    {
        var values = new int[Count];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = i;
        }

        return values;
    }

    /// <summary>Executes the <c>DrainArray</c> benchmark helper.</summary>
    /// <param name="source">The <c>source</c> value.</param>
    /// <returns>The <c>DrainArray</c> result.</returns>
    private static int DrainArray(IObservable<int[]> source)
    {
        var observer = new ArrayWitness();
        using var subscription = source.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>DrainBool</c> benchmark helper.</summary>
    /// <param name="source">The <c>source</c> value.</param>
    /// <returns>The <c>DrainBool</c> result.</returns>
    private static int DrainBool(IObservable<bool> source)
    {
        var observer = new BoolSignalWitness();
        using var subscription = source.Subscribe(observer);
        return observer.Total + observer.NextCount;
    }

    /// <summary>Executes the <c>DrainInt</c> benchmark helper.</summary>
    /// <param name="source">The <c>source</c> value.</param>
    /// <returns>The <c>DrainInt</c> result.</returns>
    private static int DrainInt(IObservable<int> source)
    {
        var observer = new IntSignalWitness();
        using var subscription = source.Subscribe(observer);
        return observer.Total + observer.NextCount;
    }

    /// <summary>Executes the <c>DrainList</c> benchmark helper.</summary>
    /// <param name="source">The <c>source</c> value.</param>
    /// <returns>The <c>DrainList</c> result.</returns>
    private static int DrainList(IObservable<IList<int>> source)
    {
        var observer = new ListWitness();
        using var subscription = source.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>DrainPackageUnit</c> benchmark helper.</summary>
    /// <param name="source">The <c>source</c> value.</param>
    /// <returns>The <c>DrainPackageUnit</c> result.</returns>
    private static int DrainPackageUnit(IObservable<RxUnit> source)
    {
        var observer = new CountingSignalWitness<RxUnit>();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Executes the <c>DrainPrimitiveUnit</c> benchmark helper.</summary>
    /// <param name="source">The <c>source</c> value.</param>
    /// <returns>The <c>DrainPrimitiveUnit</c> result.</returns>
    private static int DrainPrimitiveUnit(IObservable<RxVoid> source)
    {
        var observer = new CountingSignalWitness<RxVoid>();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Executes the <c>DrainString</c> benchmark helper.</summary>
    /// <param name="source">The <c>source</c> value.</param>
    /// <returns>The <c>DrainString</c> result.</returns>
    private static int DrainString(IObservable<string?> source)
    {
        var observer = new NullableStringLengthWitness();
        using var subscription = source.Subscribe(observer);
        return observer.TotalLength + observer.ItemCount;
    }

    /// <summary>Executes the <c>DrainSyncTuple</c> benchmark helper.</summary>
    /// <param name="source">The <c>source</c> value.</param>
    /// <returns>The <c>DrainSyncTuple</c> result.</returns>
    private static int DrainSyncTuple(IObservable<(int Value, IDisposable Sync)> source)
    {
        var observer = new SyncTupleWitness();
        using var subscription = source.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Executes the <c>Range</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>Range</c> result.</returns>
    private static IObservable<int> Range(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? Signal.Sequence(0, Count)
            : RxObservable.Range(0, Count);

    /// <summary>Executes the <c>ThrowInt</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>ThrowInt</c> result.</returns>
    private static IObservable<int> ThrowInt(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? Signal.Fail<int>(Boom)
            : RxObservable.Throw<int>(Boom);

    /// <summary>Executes the <c>ThrowPackageUnit</c> benchmark helper.</summary>
    /// <returns>The <c>ThrowPackageUnit</c> result.</returns>
    private static IObservable<RxUnit> ThrowPackageUnit() => RxObservable.Throw<RxUnit>(Boom);

    /// <summary>Executes the <c>ThrowPrimitiveUnit</c> benchmark helper.</summary>
    /// <returns>The <c>ThrowPrimitiveUnit</c> result.</returns>
    private static IObservable<RxVoid> ThrowPrimitiveUnit() => Signal.Fail<RxVoid>(Boom);

    /// <summary>
    /// Creates the generated even-digit regex.
    /// </summary>
    /// <returns>The generated regex instance.</returns>
    [GeneratedRegex("^[02468]$")]
    private static partial Regex EvenRegex();

    /// <summary>Provides a named benchmark scenario.</summary>
    public sealed class ExtensionScenario
    {
        /// <summary>Stores the scenario name.</summary>
        private readonly string _name;

        /// <summary>Stores the scenario delegate.</summary>
        private readonly Func<int> _run;

        /// <summary>Initializes a new instance of the <see cref="ExtensionScenario"/> class.</summary>
        /// <param name="name">The scenario name.</param>
        /// <param name="run">The delegate that runs the scenario.</param>
        public ExtensionScenario(string name, Func<int> run)
        {
            _name = name;
            _run = run;
        }

        /// <summary>Runs the scenario delegate.</summary>
        /// <returns>The benchmark checksum.</returns>
        public int Run() => _run();

        /// <inheritdoc/>
        public override string ToString() => _name;
    }

    /// <summary>Provides the <c>ArrayWitness</c> benchmark helper type.</summary>
    private sealed class ArrayWitness : IObserver<int[]>
    {
        /// <summary>Gets the <c>Total</c> benchmark helper value.</summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        public void OnNext(int[] value) => Total += value.Length;

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>Provides the <c>DummyResource</c> benchmark helper type.</summary>
    private sealed class DummyResource : IDisposable
    {
        /// <summary>Gets the <c>TouchCount</c> benchmark helper value.</summary>
        public int TouchCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose()
        {
        }

        /// <summary>Executes the <c>Touch</c> benchmark helper.</summary>
        public void Touch() => TouchCount++;
    }

    /// <summary>Provides the <c>ListWitness</c> benchmark helper type.</summary>
    private sealed class ListWitness : IObserver<IList<int>>
    {
        /// <summary>Gets the <c>Total</c> benchmark helper value.</summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        public void OnNext(IList<int> value) => Total += value.Count;

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>Provides the <c>NullableStringLengthWitness</c> benchmark helper type.</summary>
    private sealed class NullableStringLengthWitness : IObserver<string?>
    {
        /// <summary>Gets the <c>ItemCount</c> benchmark helper value.</summary>
        public int ItemCount { get; private set; }

        /// <summary>Gets the <c>TotalLength</c> benchmark helper value.</summary>
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

    /// <summary>Provides the <c>PairWitness</c> benchmark helper type.</summary>
    private sealed class PairWitness : IObserver<(int Previous, int Current)>
    {
        /// <summary>Gets the <c>Total</c> benchmark helper value.</summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        public void OnNext((int Previous, int Current) value) => Total += value.Previous + value.Current;

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>Provides the <c>PropertySource</c> benchmark helper type.</summary>
    private sealed class PropertySource : INotifyPropertyChanged
    {
        /// <summary>Occurs when the <c>PropertyChanged</c> benchmark helper event is raised.</summary>
        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>Gets or sets the <c>CurrentValue</c> benchmark helper value.</summary>
        public int CurrentValue
        {
            get;
            set
            {
                field = value;
                PropertyChanged?.Invoke(this, new(nameof(CurrentValue)));
            }
        }
    }

    /// <summary>Provides the <c>R3BoolWitness</c> benchmark helper type.</summary>
    private sealed class R3BoolWitness : R3.Observer<bool>
    {
        /// <summary>Gets the <c>Total</c> benchmark helper value.</summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        protected override void OnNextCore(bool value)
        {
            if (!value)
            {
                return;
            }

            Total++;
        }

        /// <inheritdoc/>
        protected override void OnErrorResumeCore(Exception error)
        {
        }

        /// <inheritdoc/>
        protected override void OnCompletedCore(R3.Result result)
        {
        }
    }

    /// <summary>Provides the <c>R3CountingWitness</c> benchmark helper type.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class R3CountingWitness<T> : R3.Observer<T>
    {
        /// <summary>Gets the <c>ItemCount</c> benchmark helper value.</summary>
        public int ItemCount { get; private set; }

        /// <inheritdoc/>
        protected override void OnNextCore(T value) => ItemCount++;

        /// <inheritdoc/>
        protected override void OnErrorResumeCore(Exception error)
        {
        }

        /// <inheritdoc/>
        protected override void OnCompletedCore(R3.Result result)
        {
        }
    }

    /// <summary>Provides the <c>SyncTupleWitness</c> benchmark helper type.</summary>
    private sealed class SyncTupleWitness : IObserver<(int Value, IDisposable Sync)>
    {
        /// <summary>Gets the <c>Total</c> benchmark helper value.</summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        public void OnNext((int Value, IDisposable Sync) value)
        {
            Total += value.Value;
            value.Sync.Dispose();
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

    /// <summary>Provides the <c>TupleWitness</c> benchmark helper type.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class TupleWitness<T> : IObserver<(T Value, IDisposable Sync)>
    {
        /// <summary>Gets the <c>ItemCount</c> benchmark helper value.</summary>
        public int ItemCount { get; private set; }

        /// <inheritdoc/>
        public void OnNext((T Value, IDisposable Sync) value)
        {
            ItemCount++;
            value.Sync.Dispose();
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
