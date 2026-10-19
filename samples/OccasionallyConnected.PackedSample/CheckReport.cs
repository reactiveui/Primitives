// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace OccasionallyConnected.PackedSample;

/// <summary>Prints expected against actual values and counts failures.</summary>
internal sealed class CheckReport
{
    private int _passed;
    private int _failed;

    /// <summary>Records a check that passes when the actual value equals the expected value.</summary>
    /// <param name="name">The check name.</param>
    /// <param name="expected">The expected value.</param>
    /// <param name="actual">The actual value.</param>
    internal void Check(string name, string expected, string? actual) =>
        Check(name, expected, actual, string.Equals(expected, actual, StringComparison.Ordinal));

    /// <summary>Records a check with an explicit outcome.</summary>
    /// <param name="name">The check name.</param>
    /// <param name="expected">The expected value.</param>
    /// <param name="actual">The actual value.</param>
    /// <param name="passed">Whether the check passed.</param>
    internal void Check(string name, string expected, string? actual, bool passed)
    {
        if (passed)
        {
            _passed++;
        }
        else
        {
            _failed++;
        }

        Console.WriteLine($"{(passed ? "PASS" : "FAIL")} {name}: expected={expected} actual={actual ?? "<none>"}");
    }

    /// <summary>Prints an informational value that is not checked.</summary>
    /// <param name="name">The name.</param>
    /// <param name="value">The value.</param>
    internal static void Info(string name, string value) => Console.WriteLine($"INFO {name}: {value}");

    /// <summary>Prints the summary and returns the process exit code.</summary>
    /// <returns>0 when every check passed; otherwise 1.</returns>
    internal int Summarize()
    {
        Console.WriteLine($"SUMMARY passed={_passed} failed={_failed}");
        return _failed == 0 && _passed > 0 ? 0 : 1;
    }
}
