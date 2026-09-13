// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Authored JSON element helpers for the HTTP protocol DTO allowlist.</summary>
internal sealed partial class HttpProtocolJsonContext
{
    /// <summary>Ensures the element is a JSON object.</summary>
    /// <param name="element">The JSON element.</param>
    /// <exception cref="JsonException">The element is not a JSON object.</exception>
    private static void EnsureObject(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            return;
        }

        throw new JsonException(ExpectedObjectMessage);
    }

    /// <summary>Reads a string property or returns the CLR default when absent.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The string value.</returns>
    /// <exception cref="JsonException">The property is not a JSON string.</exception>
    private static string GetString(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return string.Empty;
        }

        if (property.ValueKind != JsonValueKind.String)
        {
            throw new JsonException(ExpectedStringMessage);
        }

        return string.Concat(property.GetString());
    }

    /// <summary>Reads an optional string property.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The string value, if present.</returns>
    /// <exception cref="JsonException">The property is not a JSON string.</exception>
    private static string? GetOptionalString(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property) || property.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (property.ValueKind != JsonValueKind.String)
        {
            throw new JsonException(ExpectedStringMessage);
        }

        return property.GetString();
    }

    /// <summary>Reads a 32-bit integer property or returns the CLR default when absent.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The integer value.</returns>
    /// <exception cref="JsonException">The property is not a JSON number.</exception>
    private static int GetInt32(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return 0;
        }

        if (property.ValueKind != JsonValueKind.Number)
        {
            throw new JsonException(ExpectedNumberMessage);
        }

        return property.GetInt32();
    }

    /// <summary>Reads a 64-bit integer property or returns the CLR default when absent.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The integer value.</returns>
    /// <exception cref="JsonException">The property is not a JSON number.</exception>
    private static long GetInt64(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return 0;
        }

        if (property.ValueKind != JsonValueKind.Number)
        {
            throw new JsonException(ExpectedNumberMessage);
        }

        return property.GetInt64();
    }

    /// <summary>Reads an optional 64-bit integer property.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The integer value, if present.</returns>
    /// <exception cref="JsonException">The property is not a JSON number.</exception>
    private static long? GetOptionalInt64(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property) || property.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (property.ValueKind != JsonValueKind.Number)
        {
            throw new JsonException(ExpectedNumberMessage);
        }

        return property.GetInt64();
    }

    /// <summary>Reads a GUID property or returns the CLR default when absent.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The GUID value.</returns>
    /// <exception cref="JsonException">The property is not a JSON string.</exception>
    private static Guid GetGuid(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return Guid.Empty;
        }

        if (property.ValueKind != JsonValueKind.String)
        {
            throw new JsonException(ExpectedStringMessage);
        }

        return property.GetGuid();
    }

    /// <summary>Reads an optional GUID property.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The GUID value, if present.</returns>
    /// <exception cref="JsonException">The property is not a JSON string.</exception>
    private static Guid? GetOptionalGuid(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property) || property.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (property.ValueKind != JsonValueKind.String)
        {
            throw new JsonException(ExpectedStringMessage);
        }

        return property.GetGuid();
    }

    /// <summary>Reads a timestamp property or returns the CLR default when absent.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The timestamp value.</returns>
    /// <exception cref="JsonException">The property is not a JSON string.</exception>
    private static DateTimeOffset GetDateTimeOffset(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return default;
        }

        if (property.ValueKind != JsonValueKind.String)
        {
            throw new JsonException(ExpectedStringMessage);
        }

        return property.GetDateTimeOffset();
    }

    /// <summary>Reads an object property or creates the CLR default when absent.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="read">The object reader.</param>
    /// <returns>The object value.</returns>
    /// <exception cref="JsonException">The property is not a JSON object.</exception>
    private static T GetObject<T>(JsonElement element, string propertyName, Func<JsonElement, T> read)
        where T : new()
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return new();
        }

        if (property.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException(ExpectedObjectMessage);
        }

        return read(property);
    }

    /// <summary>Reads an optional object property.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="read">The object reader.</param>
    /// <returns>The object value, if present.</returns>
    /// <exception cref="JsonException">The property is not a JSON object.</exception>
    private static T? GetOptionalObject<T>(JsonElement element, string propertyName, Func<JsonElement, T> read)
        where T : class
    {
        if (!element.TryGetProperty(propertyName, out var property) || property.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (property.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException(ExpectedObjectMessage);
        }

        return read(property);
    }

    /// <summary>Reads an array property or returns the CLR default when absent.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="read">The item reader.</param>
    /// <returns>The array value.</returns>
    /// <exception cref="JsonException">The property is not a JSON array.</exception>
    private static T[] GetArray<T>(JsonElement element, string propertyName, Func<JsonElement, T> read)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return [];
        }

        if (property.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException(ExpectedArrayMessage);
        }

        var index = 0;
        var values = new T[property.GetArrayLength()];
        foreach (var item in property.EnumerateArray())
        {
            values[index] = read(item);
            index++;
        }

        return values;
    }

    /// <summary>Reads a 32-bit integer array property or returns the CLR default when absent.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The array value.</returns>
    /// <exception cref="JsonException">The property is not a JSON array.</exception>
    private static int[] GetInt32Array(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return [];
        }

        if (property.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException(ExpectedArrayMessage);
        }

        var index = 0;
        var values = new int[property.GetArrayLength()];
        foreach (var item in property.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Number)
            {
                throw new JsonException(ExpectedNumberMessage);
            }

            values[index] = item.GetInt32();
            index++;
        }

        return values;
    }

    /// <summary>Reads a GUID array property or returns the CLR default when absent.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The array value.</returns>
    /// <exception cref="JsonException">The property is not a JSON array.</exception>
    private static Guid[] GetGuidArray(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return [];
        }

        if (property.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException(ExpectedArrayMessage);
        }

        var index = 0;
        var values = new Guid[property.GetArrayLength()];
        foreach (var item in property.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String)
            {
                throw new JsonException(ExpectedStringMessage);
            }

            values[index] = item.GetGuid();
            index++;
        }

        return values;
    }

    /// <summary>Reads a string dictionary property or returns the CLR default when absent.</summary>
    /// <param name="element">The JSON object.</param>
    /// <param name="propertyName">The property name.</param>
    /// <returns>The dictionary value.</returns>
    /// <exception cref="JsonException">The property is not a JSON object.</exception>
    private static Dictionary<string, string> GetStringDictionary(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return [];
        }

        if (property.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException(ExpectedObjectMessage);
        }

        Dictionary<string, string> values = [with(comparer: StringComparer.Ordinal)];
        foreach (var item in property.EnumerateObject())
        {
            if (item.Value.ValueKind != JsonValueKind.String)
            {
                throw new JsonException(ExpectedStringMessage);
            }

            values.Add(item.Name, string.Concat(item.Value.GetString()));
        }

        return values;
    }

    /// <summary>Writes an optional string property.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="value">The optional value.</param>
    private static void WriteOptionalString(Utf8JsonWriter writer, string propertyName, string? value)
    {
        if (value is null)
        {
            return;
        }

        writer.WriteString(propertyName, value);
    }

    /// <summary>Writes an optional 64-bit integer property.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="value">The optional value.</param>
    private static void WriteOptionalInt64(Utf8JsonWriter writer, string propertyName, long? value)
    {
        if (!value.HasValue)
        {
            return;
        }

        writer.WriteNumber(propertyName, value.Value);
    }

    /// <summary>Writes an optional GUID property.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="value">The optional value.</param>
    private static void WriteOptionalGuid(Utf8JsonWriter writer, string propertyName, Guid? value)
    {
        if (!value.HasValue)
        {
            return;
        }

        writer.WriteString(propertyName, value.Value);
    }

    /// <summary>Writes an optional object property.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="value">The optional value.</param>
    /// <param name="write">The object writer.</param>
    private static void WriteOptionalObject<T>(Utf8JsonWriter writer, string propertyName, T? value, Action<Utf8JsonWriter, T> write)
        where T : class
    {
        if (value is null)
        {
            return;
        }

        writer.WritePropertyName(propertyName);
        write(writer, value);
    }

    /// <summary>Writes an array property.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="values">The values.</param>
    /// <param name="write">The item writer.</param>
    private static void WriteArray<T>(Utf8JsonWriter writer, string propertyName, T[] values, Action<Utf8JsonWriter, T> write)
    {
        writer.WriteStartArray(propertyName);
        foreach (var value in values)
        {
            write(writer, value);
        }

        writer.WriteEndArray();
    }

    /// <summary>Writes a 32-bit integer array property.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="values">The values.</param>
    private static void WriteInt32Array(Utf8JsonWriter writer, string propertyName, int[] values)
    {
        writer.WriteStartArray(propertyName);
        foreach (var value in values)
        {
            writer.WriteNumberValue(value);
        }

        writer.WriteEndArray();
    }

    /// <summary>Writes a GUID array property.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="values">The values.</param>
    private static void WriteGuidArray(Utf8JsonWriter writer, string propertyName, Guid[] values)
    {
        writer.WriteStartArray(propertyName);
        foreach (var value in values)
        {
            writer.WriteStringValue(value);
        }

        writer.WriteEndArray();
    }

    /// <summary>Writes a string dictionary property.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="values">The values.</param>
    private static void WriteStringDictionary(Utf8JsonWriter writer, string propertyName, Dictionary<string, string> values)
    {
        writer.WriteStartObject(propertyName);
        foreach (var pair in values)
        {
            writer.WriteString(pair.Key, pair.Value);
        }

        writer.WriteEndObject();
    }
}
