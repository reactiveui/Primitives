// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using System.Runtime.CompilerServices;
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
    /// <summary>Creates an int source over the shared array from the selected library.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>An observable over <see cref="Values"/>.</returns>
    private static IObservable<int> ArraySource(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.FromArray(Values)
            : PackageExtensions.FromArray(Values);

    /// <summary>Creates a bool source over the shared boolean array from the selected library.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>An observable over <see cref="BooleanValues"/>.</returns>
    private static IObservable<bool> BoolSource(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.FromArray(BooleanValues)
            : PackageExtensions.FromArray(BooleanValues);

    /// <summary>Spin-waits for the operation, rethrowing cancellation or failure.</summary>
    /// <param name="task">The operation to wait on.</param>
    /// <exception cref="TaskCanceledException"><paramref name="task"/> completed in the canceled state.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="task"/> completed in the faulted state.</exception>
    private static void EnsureCompleted(Task task)
    {
        while (!task.IsCompleted)
        {
            _ = Thread.Yield();
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

    /// <summary>Spin-waits for the operation, rethrowing cancellation or failure.</summary>
    /// <param name="task">The operation to wait on.</param>
    private static void EnsureCompleted(ValueTask task)
    {
        if (task.IsCompletedSuccessfully)
        {
            return;
        }

        EnsureCompleted(task.AsTask());
    }

    /// <summary>Waits for the operation to finish and returns the scenario checksum.</summary>
    /// <param name="task">The operation to wait on.</param>
    /// <returns>The scenario checksum.</returns>
    private static int GetCompletedResult(Task<int> task)
    {
        EnsureCompleted(task);
        return Value;
    }

    /// <summary>Waits for the operation to finish and returns the scenario checksum.</summary>
    /// <param name="task">The operation to wait on.</param>
    /// <returns>The scenario checksum.</returns>
    private static int GetCompletedResult(ValueTask<int> task)
    {
        if (!task.IsCompletedSuccessfully)
        {
            EnsureCompleted(task.AsTask());
        }

        return Value;
    }

    /// <summary>Yields two single-value bool sources from the selected library.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <param name="value">The boolean each source emits.</param>
    /// <returns>Two observables that each emit <paramref name="value"/>.</returns>
    private static IEnumerable<IObservable<bool>> BoolSources(ExtensionsLibrary library, bool value)
    {
        yield return library == ExtensionsLibrary.Primitives
            ? PrimitivesObservables.Return(value)
            : PackageObservables.Return(value);
        yield return library == ExtensionsLibrary.Primitives
            ? PrimitivesObservables.Return(value)
            : PackageObservables.Return(value);
    }

    /// <summary>Yields pre-completed tasks carrying ascending values.</summary>
    /// <returns>A lazy sequence of <see cref="Count"/> completed tasks.</returns>
    private static IEnumerable<Task<int>> CompletedTasks()
    {
        for (var i = 0; i < Count; i++)
        {
            yield return Task.FromResult(i);
        }
    }

    /// <summary>Builds the shared int payload as ascending values from zero.</summary>
    /// <returns>An array of <see cref="Count"/> ascending values.</returns>
    private static int[] CreateValues()
    {
        var values = new int[Count];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = i;
        }

        return values;
    }

    /// <summary>Subscribes to the source and sums the length of every emitted array.</summary>
    /// <param name="source">The sequence to drain.</param>
    /// <returns>The summed array lengths.</returns>
    private static int DrainArray(IObservable<int[]> source)
    {
        ArrayWitness observer = new();
        using var subscription = source.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribes to the source and folds the emitted booleans into a checksum.</summary>
    /// <param name="source">The sequence to drain.</param>
    /// <returns>The true-value count plus the emission count.</returns>
    private static int DrainBool(IObservable<bool> source)
    {
        BoolSignalWitness observer = new();
        using var subscription = source.Subscribe(observer);
        return observer.Total + observer.NextCount;
    }

    /// <summary>Subscribes to the source and folds the emitted integers into a checksum.</summary>
    /// <param name="source">The sequence to drain.</param>
    /// <returns>The value total plus the emission count.</returns>
    private static int DrainInt(IObservable<int> source)
    {
        IntSignalWitness observer = new();
        using var subscription = source.Subscribe(observer);
        return observer.Total + observer.NextCount;
    }

    /// <summary>Subscribes to the source and sums the size of every emitted list.</summary>
    /// <param name="source">The sequence to drain.</param>
    /// <returns>The summed list sizes.</returns>
    private static int DrainList(IObservable<IList<int>> source)
    {
        ListWitness observer = new();
        using var subscription = source.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Subscribes to the source and counts the System.Reactive unit notifications.</summary>
    /// <param name="source">The sequence to drain.</param>
    /// <returns>The emission count plus the completion count.</returns>
    private static int DrainPackageUnit(IObservable<RxUnit> source)
    {
        CountingSignalWitness<RxUnit> observer = new();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Subscribes to the source and counts the primitives void notifications.</summary>
    /// <param name="source">The sequence to drain.</param>
    /// <returns>The emission count plus the completion count.</returns>
    private static int DrainPrimitiveUnit(IObservable<RxVoid> source)
    {
        CountingSignalWitness<RxVoid> observer = new();
        using var subscription = source.Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Subscribes to the source and folds emitted string lengths into a checksum, counting null as zero.</summary>
    /// <param name="source">The sequence to drain.</param>
    /// <returns>The total string length plus the emission count.</returns>
    private static int DrainString(IObservable<string?> source)
    {
        NullableStringLengthWitness observer = new();
        using var subscription = source.Subscribe(observer);
        return observer.TotalLength + observer.ItemCount;
    }

    /// <summary>Subscribes to the source, summing each value and disposing its synchronization handle.</summary>
    /// <param name="source">The sequence to drain.</param>
    /// <returns>The summed values.</returns>
    private static int DrainSyncTuple(IObservable<(int Value, IDisposable Sync)> source)
    {
        SyncTupleWitness observer = new();
        using var subscription = source.Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Creates the shared ascending range from the selected library.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>An observable over <see cref="Count"/> ascending values from zero.</returns>
    private static IObservable<int> Range(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? Signal.Sequence(0, Count)
            : RxObservable.Range(0, Count);

    /// <summary>Creates an immediately failing int source from the selected library.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>An observable that fails with <see cref="Boom"/>.</returns>
    private static IObservable<int> ThrowInt(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? Signal.Fail<int>(Boom)
            : RxObservable.Throw<int>(Boom);

    /// <summary>Creates a System.Reactive unit source that fails immediately.</summary>
    /// <returns>An observable that fails with <see cref="Boom"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<RxUnit> ThrowPackageUnit() => RxObservable.Throw<RxUnit>(Boom);

    /// <summary>Creates a primitives void source that fails immediately.</summary>
    /// <returns>An observable that fails with <see cref="Boom"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<RxVoid> ThrowPrimitiveUnit() => Signal.Fail<RxVoid>(Boom);

    /// <summary>Creates the generated even-digit regex.</summary>
    /// <returns>The generated regex instance.</returns>
    [GeneratedRegex("^[02468]$")]
    private static partial Regex EvenRegex();

    /// <summary>Provides a named benchmark scenario.</summary>
    /// <param name="name">The scenario name.</param>
    /// <param name="run">The delegate that runs the scenario.</param>
    [System.Diagnostics.DebuggerDisplay("ExtensionScenario: Name = {_name}")]
    public sealed class ExtensionScenario(string name, Func<int> run)
    {
        /// <summary>The scenario name reported by BenchmarkDotNet.</summary>
        private readonly string _name = name;

        /// <summary>The delegate invoked for each benchmark operation.</summary>
        private readonly Func<int> _run = run;

        /// <summary>Runs the scenario delegate.</summary>
        /// <returns>The benchmark checksum.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Run() => _run();

        /// <inheritdoc/>
        public override string ToString() => _name;
    }

    /// <summary>Observer that accumulates the length of every emitted array.</summary>
    private sealed class ArrayWitness : IObserver<int[]>
    {
        /// <summary>Gets the summed length of the arrays observed.</summary>
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

    /// <summary>Disposable resource used by the resource-scoping scenarios.</summary>
    private sealed class DummyResource : IDisposable
    {
        /// <summary>Gets or sets the number of times the resource has been touched.</summary>
        private int TouchCount { get; set; }

        /// <inheritdoc/>
        public void Dispose()
        {
        }

        /// <summary>Records one use of the resource.</summary>
        public void Touch() => TouchCount++;
    }

    /// <summary>Observer that accumulates the size of every emitted list.</summary>
    private sealed class ListWitness : IObserver<IList<int>>
    {
        /// <summary>Gets the summed size of the lists observed.</summary>
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

    /// <summary>Observer that counts emissions and accumulates string length, counting null as zero.</summary>
    private sealed class NullableStringLengthWitness : IObserver<string?>
    {
        /// <summary>Gets the number of values observed.</summary>
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

    /// <summary>Observer that sums both halves of every emitted pair.</summary>
    private sealed class PairWitness : IObserver<(int Previous, int Current)>
    {
        /// <summary>Gets the summed pair values.</summary>
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

    /// <summary>Property holder that raises a change notification on every assignment.</summary>
    private sealed class PropertySource : INotifyPropertyChanged
    {
        /// <summary>Occurs when <see cref="CurrentValue"/> is assigned.</summary>
        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>Gets or sets the value whose assignment raises the change notification.</summary>
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

    /// <summary>R3 observer that counts the true values observed.</summary>
    private sealed class R3BoolWitness : R3.Observer<bool>
    {
        /// <summary>Gets the number of true values observed.</summary>
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

    /// <summary>R3 observer that counts the values observed.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class R3CountingWitness<T> : R3.Observer<T>
    {
        /// <summary>Gets the number of values observed.</summary>
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

    /// <summary>Observer that sums each value and disposes its paired synchronization handle.</summary>
    private sealed class SyncTupleWitness : IObserver<(int Value, IDisposable Sync)>
    {
        /// <summary>Gets the summed values.</summary>
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

    /// <summary>Observer that counts each value and disposes its paired synchronization handle.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class TupleWitness<T> : IObserver<(T Value, IDisposable Sync)>
    {
        /// <summary>Gets the number of values observed.</summary>
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
