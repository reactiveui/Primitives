// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.Options;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Validates dependency-injection integration options.</summary>
internal sealed class OccasionallyConnectedServiceOptionsValidator :
    IValidateOptions<OccasionallyConnectedServiceOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, OccasionallyConnectedServiceOptions options)
    {
        if (options.Options is null)
        {
            return ValidateOptionsResult.Fail("Options must be supplied.");
        }

        if (options.MaximumNamedStreams <= 0)
        {
            return ValidateOptionsResult.Fail("MaximumNamedStreams must be positive.");
        }

        if (options.MaximumStreamNameLength <= 0)
        {
            return ValidateOptionsResult.Fail("MaximumStreamNameLength must be positive.");
        }

        try
        {
            options.Options.Validate();
            return ValidateOptionsResult.Success;
        }
        catch (Exception exception)
        {
            return ValidateOptionsResult.Fail(exception.Message);
        }
    }
}
