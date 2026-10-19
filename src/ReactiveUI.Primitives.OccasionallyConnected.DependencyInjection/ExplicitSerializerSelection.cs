// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.DependencyInjection;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Stores a validated explicit payload serializer selection.</summary>
/// <param name="ServiceType">The selected payload serializer service type.</param>
/// <param name="Descriptor">The effective singleton descriptor captured during configuration.</param>
internal sealed record ExplicitSerializerSelection(Type ServiceType, ServiceDescriptor Descriptor);
