// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Validates the <c>key=value</c> rows written by <see cref="SmokeBenchmarkRunner"/>. Consecutive
/// rows that normalize to the same scenario form a group, and every library in a group must produce
/// the same value — except for the scheduling differences documented below, which are pinned to
/// their known values so a change in behaviour is caught rather than silently accepted.
/// </summary>
internal static class SmokeParityValidator
{
    /// <summary>The expected Primitives value for the documented SwitchRanges scheduling difference.</summary>
    private const int SwitchRangesPrimitivesValue = 1856;

    /// <summary>The expected System.Reactive value for the documented SwitchRanges scheduling difference.</summary>
    private const int SwitchRangesSystemReactiveValue = 1721;

    /// <summary>The expected R3 value for the documented SwitchRanges scheduling difference.</summary>
    private const int SwitchRangesR3Value = 1856;

    /// <summary>The expected Primitives and R3 value for the documented CombineLatest/WithLatest differences.</summary>
    private const int CombineWithLatestPrimitivesValue = 536;

    /// <summary>The expected System.Reactive value for the documented CombineLatestRanges difference.</summary>
    private const int CombineLatestSystemReactiveValue = 806;

    /// <summary>The expected System.Reactive value for the documented WithLatestRanges difference.</summary>
    private const int WithLatestSystemReactiveValue = 416;

    /// <summary>The benchmark method-name prefix identifying the Primitives library row.</summary>
    private const string PrimitivesPrefix = "Primitives";

    /// <summary>The benchmark method-name prefix identifying the System.Reactive library row.</summary>
    private const string SystemReactivePrefix = "SystemReactive";

    /// <summary>The benchmark method-name prefix identifying the R3 library row.</summary>
    private const string R3Prefix = "R3";

    /// <summary>Maps comparator benchmark method suffixes onto the matching Primitives smoke scenario.</summary>
    private static readonly Dictionary<string, string> SmokeScenarioAliases =
        new(StringComparer.Ordinal)
        {
            ["ToObservableSubscribe"] = "FromEnumerableSubscribe",
            ["RangeSelectWhere"] = "RangeMapKeep",
            ["SelectManyRange"] = "FlatMapRange",
            ["PrependAppendDefaultIfEmpty"] = "StartWithAppendDefaultIfEmpty",
            ["BehaviorSubject32"] = "StateSignal32",
            ["BehaviorSubject1024"] = "StateSignal1024",
            ["ReplaySubscribe"] = "HistorySubscribe",
            ["CompositeDispose"] = "PocketDispose"
        };

    /// <summary>Validates the captured smoke output for parity across the participating libraries.</summary>
    /// <param name="output">The captured smoke benchmark console output.</param>
    public static void Validate(string output)
    {
        var results = output.Split([Environment.NewLine], StringSplitOptions.RemoveEmptyEntries);
        List<string> failures = [];
        var groupCount = 0;
        var index = 0;
        while (index < results.Length)
        {
            var (firstName, firstValue) = ParseSmokeResult(results[index]);
            var scenario = NormalizeSmokeScenarioName(firstName);
            List<(string Name, int Value)> rows = [(firstName, firstValue)];
            var next = index + 1;
            while (next < results.Length)
            {
                var (name, value) = ParseSmokeResult(results[next]);
                if (NormalizeSmokeScenarioName(name) != scenario)
                {
                    break;
                }

                rows.Add((name, value));
                next++;
            }

            var failure = ValidateSmokeGroup(index + 1, rows);
            if (failure is not null)
            {
                failures.Add(failure);
            }

            groupCount++;
            index = next;
        }

        if (failures.Count > 0)
        {
            throw new InvalidOperationException(
                "Benchmark smoke parity validation failed:" + Environment.NewLine +
                string.Join(Environment.NewLine, failures));
        }

        Console.WriteLine($"Smoke parity validation passed for {groupCount} benchmark groups.");
    }

    /// <summary>Validates a group of consecutive smoke rows for one scenario, one row per participating library.</summary>
    /// <param name="firstRowNumber">The one-based row number of the first row in the group.</param>
    /// <param name="rows">The library result rows for the scenario, in emission order.</param>
    /// <returns>A failure description, or <see langword="null"/> when the group is valid.</returns>
    private static string? ValidateSmokeGroup(int firstRowNumber, List<(string Name, int Value)> rows)
    {
        string? primitivesName = null;
        var primitivesValue = 0;
        for (var i = 0; i < rows.Count; i++)
        {
            var name = rows[i].Name;
            if (!HasKnownLibraryPrefix(name))
            {
                return $"Row {firstRowNumber} group contains an unrecognized library prefix: {name}.";
            }

            if (primitivesName is null && name.StartsWith(PrimitivesPrefix, StringComparison.Ordinal))
            {
                primitivesName = name;
                primitivesValue = rows[i].Value;
            }
        }

        if (primitivesName is null)
        {
            return $"Row {firstRowNumber} group has no Primitives result.";
        }

        return IsDocumentedSmokeDifference(primitivesName)
            ? ValidateDocumentedSmokeDifference(rows)
            : ValidateSmokeParity(primitivesName, primitivesValue, rows);
    }

    /// <summary>Determines whether the name carries a recognized library prefix.</summary>
    /// <param name="name">The benchmark result name.</param>
    /// <returns><see langword="true"/> when the name has a known library prefix.</returns>
    private static bool HasKnownLibraryPrefix(string name) =>
        name.StartsWith(PrimitivesPrefix, StringComparison.Ordinal) ||
        name.StartsWith(SystemReactivePrefix, StringComparison.Ordinal) ||
        name.StartsWith(R3Prefix, StringComparison.Ordinal);

    /// <summary>Validates that every library row in the group matches the Primitives value.</summary>
    /// <param name="primitivesName">The Primitives result name.</param>
    /// <param name="primitivesValue">The Primitives result value.</param>
    /// <param name="rows">The library result rows for the scenario.</param>
    /// <returns>A failure description, or <see langword="null"/> when the values match.</returns>
    private static string? ValidateSmokeParity(
        string primitivesName,
        int primitivesValue,
        List<(string Name, int Value)> rows)
    {
        for (var i = 0; i < rows.Count; i++)
        {
            if (rows[i].Value != primitivesValue)
            {
                var parts = new string[rows.Count];
                for (var j = 0; j < rows.Count; j++)
                {
                    parts[j] = $"{rows[j].Name}={rows[j].Value}";
                }

                return $"{primitivesName}: expected parity but got {string.Join(", ", parts)}.";
            }
        }

        return null;
    }

    /// <summary>Normalizes a benchmark result name to its underlying smoke scenario name.</summary>
    /// <param name="name">The benchmark result name including its library prefix.</param>
    /// <returns>The normalized smoke scenario name.</returns>
    private static string NormalizeSmokeScenarioName(string name)
    {
        var scenario = name switch
        {
            _ when name.StartsWith(SystemReactivePrefix, StringComparison.Ordinal) => name[
                SystemReactivePrefix.Length..],
            _ when name.StartsWith(PrimitivesPrefix, StringComparison.Ordinal) => name[PrimitivesPrefix.Length..],
            _ => name[R3Prefix.Length..]
        };

        return SmokeScenarioAliases.TryGetValue(scenario, out var normalized) ? normalized : scenario;
    }

    /// <summary>Determines whether the named scenario has a documented, expected parity difference.</summary>
    /// <param name="primitivesName">The Primitives result name.</param>
    /// <returns><see langword="true"/> when the scenario is a documented difference; otherwise, <see langword="false"/>.</returns>
    private static bool IsDocumentedSmokeDifference(string primitivesName) =>
        primitivesName is "PrimitivesSwitchRanges" or
            "PrimitivesCombineLatestRanges" or
            "PrimitivesWithLatestRanges";

    /// <summary>Splits the scenario rows into the Primitives name and the per-library values.</summary>
    /// <param name="rows">The library result rows for the scenario.</param>
    /// <returns>The Primitives name and the Primitives, System.Reactive, and R3 values.</returns>
    private static (string? PrimitivesName, int PrimitivesValue, int SystemReactiveValue, int R3Value)
        SplitLibraryValues(
            List<(string Name, int Value)> rows)
    {
        string? primitivesName = null;
        var primitivesValue = 0;
        var systemReactiveValue = 0;
        var r3Value = 0;
        for (var i = 0; i < rows.Count; i++)
        {
            var (name, value) = rows[i];
            if (name.StartsWith(SystemReactivePrefix, StringComparison.Ordinal))
            {
                systemReactiveValue = value;
            }
            else if (name.StartsWith(PrimitivesPrefix, StringComparison.Ordinal))
            {
                primitivesName = name;
                primitivesValue = value;
            }
            else
            {
                r3Value = value;
            }
        }

        return (primitivesName, primitivesValue, systemReactiveValue, r3Value);
    }

    /// <summary>Validates a scenario with a documented, expected parity difference against its known values.</summary>
    /// <param name="rows">The library result rows for the scenario.</param>
    /// <returns>A failure description, or <see langword="null"/> when the values match the documented difference.</returns>
    private static string? ValidateDocumentedSmokeDifference(List<(string Name, int Value)> rows)
    {
        var (primitivesName, primitivesValue, systemReactiveValue, r3Value) = SplitLibraryValues(rows);

        var expected = primitivesName switch
        {
            "PrimitivesSwitchRanges" => (Primitives: SwitchRangesPrimitivesValue,
                SystemReactive: SwitchRangesSystemReactiveValue, R3: SwitchRangesR3Value),
            "PrimitivesCombineLatestRanges" => (Primitives: CombineWithLatestPrimitivesValue,
                SystemReactive: CombineLatestSystemReactiveValue, R3: CombineWithLatestPrimitivesValue),
            "PrimitivesWithLatestRanges" => (Primitives: CombineWithLatestPrimitivesValue,
                SystemReactive: WithLatestSystemReactiveValue, R3: CombineWithLatestPrimitivesValue),
            _ => default
        };

        if (expected == default)
        {
            return null;
        }

        return primitivesValue == expected.Primitives &&
               systemReactiveValue == expected.SystemReactive &&
               r3Value == expected.R3
            ? null
            : $"{primitivesName}: documented scheduling difference changed; expected " +
              $"Primitives={expected.Primitives}, System.Reactive={expected.SystemReactive}, R3={expected.R3}, " +
              $"but got Primitives={primitivesValue}, System.Reactive={systemReactiveValue}, R3={r3Value}.";
    }

    /// <summary>Parses a single <c>key=value</c> smoke output row into its name and integer value.</summary>
    /// <param name="line">The smoke output row to parse.</param>
    /// <returns>A tuple containing the result name and its integer value.</returns>
    private static (string Name, int Value) ParseSmokeResult(string line)
    {
        var separator = line.IndexOf('=', StringComparison.Ordinal);
        if (separator <= 0 || separator == line.Length - 1)
        {
            throw new InvalidOperationException($"Smoke output row is not key=value: {line}");
        }

        var value = int.Parse(line[(separator + 1)..], CultureInfo.InvariantCulture);
        return (line[..separator], value);
    }
}
