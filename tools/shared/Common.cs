#nullable enable

using System;
using System.IO;
using System.IO.Compression;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;

namespace OpenSourceLaw.Tools;

public static class ToolDefaults
{
    public const string UserAgent = "opensourcelaw-retsinformation-ingest/0.1";
}

public static class ToolLog
{
    public static void Info(string message)
    {
        Write("info", message);
    }

    public static void Warn(string message)
    {
        Write("warn", message);
    }

    public static void Error(string message)
    {
        Write("error", message);
    }

    private static void Write(string level, string message)
    {
        if (DagsterPipes.TryLog(level, message))
        {
            return;
        }

        Console.Error.WriteLine($"{DateTimeOffset.UtcNow:O} {level}: {message}");
    }
}

public sealed class DagsterPipes : IDisposable
{
    private const string ProtocolVersionField = "__dagster_pipes_version";
    private const string ContextEnvVar = "DAGSTER_PIPES_CONTEXT";
    private const string MessagesEnvVar = "DAGSTER_PIPES_MESSAGES";
    private static DagsterPipes? Current;
    private readonly string? messagesPath;
    private bool closed;

    private DagsterPipes(string? messagesPath)
    {
        this.messagesPath = messagesPath;
    }

    public static bool IsActive => Current?.messagesPath is not null;

    public static DagsterPipes Open()
    {
        var session = new DagsterPipes(LoadMessagePath());
        Current = session;
        session.WriteMessage("opened", WriteEmptyObject);
        return session;
    }

    public static async Task<T?> ReadInputAsync<T>(JsonTypeInfo<T> jsonTypeInfo)
    {
        if (IsActive)
        {
            return GetExtra("payload", jsonTypeInfo);
        }

        var inputJson = await Console.In.ReadToEndAsync();
        return JsonSerializer.Deserialize(inputJson, jsonTypeInfo);
    }

    public static void WriteOutput<T>(T output, JsonTypeInfo<T> jsonTypeInfo)
    {
        if (IsActive)
        {
            ReportCustomMessage(output, jsonTypeInfo);
            return;
        }

        Console.Write(JsonSerializer.Serialize(output, jsonTypeInfo));
    }

    public static void ReportCustomMessage<T>(T payload, JsonTypeInfo<T> jsonTypeInfo)
    {
        Current?.WriteMessage(
            "report_custom_message",
            writer =>
            {
                writer.WriteStartObject();
                writer.WritePropertyName("payload");
                JsonSerializer.Serialize(writer, payload, jsonTypeInfo);
                writer.WriteEndObject();
            }
        );
    }

    public static bool TryLog(string level, string message)
    {
        if (!IsActive)
        {
            return false;
        }

        Current?.WriteMessage(
            "log",
            writer =>
            {
                writer.WriteStartObject();
                writer.WriteString("message", message);
                writer.WriteString(
                    "level",
                    level switch
                    {
                        "error" => "ERROR",
                        "warn" => "WARNING",
                        _ => "INFO",
                    }
                );
                writer.WriteEndObject();
            }
        );
        return true;
    }

    public void Dispose()
    {
        if (!closed)
        {
            WriteMessage("closed", WriteEmptyObject);
            closed = true;
        }

        if (ReferenceEquals(Current, this))
        {
            Current = null;
        }
    }

    private static T? GetExtra<T>(string name, JsonTypeInfo<T> jsonTypeInfo)
    {
        using var contextData = LoadContextData()
            ?? throw new InvalidOperationException("Dagster Pipes context is not available.");

        if (
            !contextData.RootElement.TryGetProperty("extras", out var extras)
            || !extras.TryGetProperty(name, out var value)
        )
        {
            throw new InvalidOperationException($"Dagster Pipes extra '{name}' is missing.");
        }

        return value.Deserialize(jsonTypeInfo);
    }

    private static JsonDocument? LoadContextData()
    {
        using var contextParams = DecodeEnvJson(ContextEnvVar);

        if (contextParams is null)
        {
            return null;
        }

        var root = contextParams.RootElement;

        if (root.TryGetProperty("path", out var path))
        {
            return JsonDocument.Parse(File.ReadAllText(path.GetString()!));
        }

        if (root.TryGetProperty("data", out var data))
        {
            return JsonDocument.Parse(data.GetRawText());
        }

        throw new InvalidOperationException("Dagster Pipes context params are missing path/data.");
    }

    private static string? LoadMessagePath()
    {
        using var messagesParams = DecodeEnvJson(MessagesEnvVar);

        if (messagesParams is null)
        {
            return null;
        }

        if (!messagesParams.RootElement.TryGetProperty("path", out var path))
        {
            throw new InvalidOperationException("Dagster Pipes message params are missing path.");
        }

        return path.GetString();
    }

    private static JsonDocument? DecodeEnvJson(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);

        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var compressed = Convert.FromBase64String(value);
        using var input = new MemoryStream(compressed);
        using var zlib = new ZLibStream(input, CompressionMode.Decompress);
        return JsonDocument.Parse(zlib);
    }

    private static void WriteEmptyObject(Utf8JsonWriter writer)
    {
        writer.WriteStartObject();
        writer.WriteEndObject();
    }

    private void WriteMessage(string method, Action<Utf8JsonWriter> writeParameters)
    {
        if (messagesPath is null)
        {
            return;
        }

        using var stream = new FileStream(
            messagesPath,
            FileMode.Append,
            FileAccess.Write,
            FileShare.ReadWrite
        );
        using var writer = new Utf8JsonWriter(stream);
        writer.WriteStartObject();
        writer.WriteString(ProtocolVersionField, "0.1");
        writer.WriteString("method", method);
        writer.WritePropertyName("params");
        writeParameters(writer);
        writer.WriteEndObject();
        writer.Flush();
        stream.WriteByte((byte)'\n');
    }
}

public sealed record SitemapEntry(
    [property: JsonPropertyName("url")] string Url,
    [property: JsonPropertyName("lastmod")] string Lastmod,
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("year")] string Year,
    [property: JsonPropertyName("type")] string Type
);
