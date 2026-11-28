// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace StockLevels.Tests;

/// <summary>Runs the stock display, reorder alert and barcode examples against the selected package family.</summary>
public sealed class StockExampleTests
{
    /// <summary>Checks the stock totals, notification times, adapter lifetime and error recovery.</summary>
    /// <returns>Completion after all receipts have been checked.</returns>
    [Test]
    public async Task StoreProcessesStockChanges() => await Assert.That(Program.Main).ThrowsNothing();
}
