// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Authored protocol converter and validation helpers for the HTTP protocol DTO allowlist.</summary>
internal sealed partial class HttpProtocolJsonContext
{
    /// <summary>Rejects duplicate JSON object properties after JSON parsing validates syntax.</summary>
    /// <param name="element">The JSON element.</param>
    /// <exception cref="JsonException">The JSON contains a duplicate object member.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RejectDuplicateProperties(JsonElement element) => ValidateDuplicateFreeValue(element);

    /// <summary>Validates the current JSON value for duplicate-free nested objects.</summary>
    /// <param name="element">The JSON element.</param>
    /// <exception cref="JsonException">The JSON contains a duplicate object member.</exception>
    private static void ValidateDuplicateFreeValue(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
            {
                ValidateDuplicateFreeObject(element);
                break;
            }

            case JsonValueKind.Array:
            {
                ValidateDuplicateFreeArray(element);
                break;
            }

            default:
            {
                break;
            }
        }
    }

    /// <summary>Validates the current JSON object for duplicate member names.</summary>
    /// <param name="element">The JSON object.</param>
    /// <exception cref="JsonException">The JSON contains a duplicate object member.</exception>
    private static void ValidateDuplicateFreeObject(JsonElement element)
    {
        HashSet<string> names = [];
        foreach (var property in element.EnumerateObject())
        {
            if (!names.Add(property.Name))
            {
                throw new JsonException(DuplicatePropertyMessage);
            }

            ValidateDuplicateFreeValue(property.Value);
        }
    }

    /// <summary>Validates the current JSON array for duplicate-free nested objects.</summary>
    /// <param name="element">The JSON array.</param>
    /// <exception cref="JsonException">The JSON contains a duplicate object member.</exception>
    private static void ValidateDuplicateFreeArray(JsonElement element)
    {
        foreach (var item in element.EnumerateArray())
        {
            ValidateDuplicateFreeValue(item);
        }
    }

    /// <summary>Converts an HTTP protocol DTO through authored delegates.</summary>
    /// <typeparam name="T">The protocol DTO type.</typeparam>
    /// <param name="read">The JSON element reader.</param>
    /// <param name="write">The JSON writer.</param>
    private sealed class ProtocolJsonConverter<T>(Func<JsonElement, T> read, Action<Utf8JsonWriter, T> write) : JsonConverter<T>
        where T : class
    {
        /// <inheritdoc/>
        public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            RejectDuplicateProperties(document.RootElement);
            try
            {
                return read(document.RootElement);
            }
            catch (FormatException exception)
            {
                throw new JsonException("Invalid protocol JSON value.", exception);
            }
        }

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options) => write(writer, value);
    }
}
