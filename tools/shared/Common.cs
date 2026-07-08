using System;
using System.Text.Json.Serialization;

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
        Console.Error.WriteLine($"{DateTimeOffset.UtcNow:O} {level}: {message}");
    }
}

public sealed record SitemapEntry(
    [property: JsonPropertyName("url")] string Url,
    [property: JsonPropertyName("lastmod")] string Lastmod,
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("year")] string Year,
    [property: JsonPropertyName("type")] string Type
);
