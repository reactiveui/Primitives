// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace StockLevels;

/// <summary>Carries the barcode read from a stock bin.</summary>
/// <param name="barcode">The text on the label.</param>
[System.Diagnostics.DebuggerDisplay("{Barcode,nq}")]
public sealed class BarcodeScannedEventArgs(string barcode) : EventArgs
{
    /// <summary>Gets the text printed on the label.</summary>
    public string Barcode { get; } = barcode;
}
