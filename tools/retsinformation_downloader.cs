using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

return await RetsinformationDownloaderTool.RunAsync();

internal static class RetsinformationDownloaderTool
{
    public static async Task<int> RunAsync()
    {
        try
        {
            var inputJson = await Console.In.ReadToEndAsync();
            var input = JsonSerializer.Deserialize(
                inputJson,
                RetsinformationDownloaderJsonContext.Default.ToolInput
            );

            Validate(input);

            var output = Run(input!);
            Console.Write(
                JsonSerializer.Serialize(
                    output,
                    RetsinformationDownloaderJsonContext.Default.DocumentRefSetOutput
                )
            );

            return 0;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine(error.Message);
            return 1;
        }
    }

    private static DocumentRefSetOutput Run(ToolInput input)
    {
        // TODO: replace this placeholder with the downloader/ref-building logic.
        return new DocumentRefSetOutput(input.DocumentType!, input.Year!, new List<SitemapEntry>());
    }

    private static void Validate(ToolInput? input)
    {
        if (input is null)
        {
            throw new InvalidOperationException("Input JSON is required.");
        }

        if (string.IsNullOrWhiteSpace(input.DocumentType))
        {
            throw new InvalidOperationException("Input must contain documentType.");
        }

        if (string.IsNullOrWhiteSpace(input.Year))
        {
            throw new InvalidOperationException("Input must contain year.");
        }

        if (input.RetsinfoSitemapPage is null)
        {
            throw new InvalidOperationException("Input must contain retsinfoSitemapPage.");
        }
    }
}

internal sealed record ToolInput(
    [property: JsonPropertyName("documentType")] string? DocumentType,
    [property: JsonPropertyName("year")] string? Year,
    [property: JsonPropertyName("retsinfoSitemapPage")] List<SitemapEntry>? RetsinfoSitemapPage
);

internal sealed record SitemapEntry(
    [property: JsonPropertyName("url")] string Url,
    [property: JsonPropertyName("lastmod")] string Lastmod,
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("year")] string Year,
    [property: JsonPropertyName("type")] string Type
);

internal sealed record DocumentRefSetOutput(
    [property: JsonPropertyName("documentType")] string DocumentType,
    [property: JsonPropertyName("year")] string Year,
    [property: JsonPropertyName("entries")] List<SitemapEntry> Entries
);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(ToolInput))]
[JsonSerializable(typeof(DocumentRefSetOutput))]
internal partial class RetsinformationDownloaderJsonContext : JsonSerializerContext;
