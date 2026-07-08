using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

return await RetsinformationDownloaderTool.RunAsync();

internal static class RetsinformationDownloaderTool
{
    private const string DefaultUserAgent = "opensourcelaw-retsinformation-ingest/0.1";
    private const string XmlEndpointSource = "xml_endpoint";
    private const string ApiDocumentSource = "api_document";

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

            var output = await RunAsync(input!);
            Console.Write(
                JsonSerializer.Serialize(
                    output,
                    RetsinformationDownloaderJsonContext.Default.ToolOutput
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

    private static async Task<ToolOutput> RunAsync(ToolInput input)
    {
        var timeoutSeconds = input.TimeoutSeconds ?? 30.0;

        if (timeoutSeconds <= 0)
        {
            throw new InvalidOperationException("timeoutSeconds must be greater than zero.");
        }

        var outputDir = Path.GetFullPath(input.OutputDir!);
        var documentsPath = Path.Combine(outputDir, "documents.jsonl");
        var failuresPath = Path.Combine(outputDir, "failures.jsonl");
        var manifestPath = Path.Combine(outputDir, "manifest.json");
        var documentRefs = input.RetsinfoSitemapPage!
            .Where(entry => entry.Type == input.DocumentType && entry.Year == input.Year)
            .ToList();
        var sourceCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        var fetchedCount = 0;
        var failedCount = 0;
        var notFoundCount = 0;
        long bytesDownloaded = 0;

        Directory.CreateDirectory(outputDir);

        using var handler = new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.All,
        };
        using var httpClient = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(timeoutSeconds),
        };
        httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(
            string.IsNullOrWhiteSpace(input.UserAgent) ? DefaultUserAgent : input.UserAgent
        );

        var tempDocumentsPath = documentsPath + ".tmp";
        var tempFailuresPath = failuresPath + ".tmp";

        await using (var documents = new StreamWriter(tempDocumentsPath, false, new UTF8Encoding(false)))
        await using (var failures = new StreamWriter(tempFailuresPath, false, new UTF8Encoding(false)))
        {
            foreach (var entry in documentRefs)
            {
                ValidateEntry(entry);

                var (page, failure) = await FetchDocumentAsync(httpClient, entry);

                if (page is not null)
                {
                    await documents.WriteLineAsync(
                        JsonSerializer.Serialize(
                            page,
                            RetsinformationDownloaderJsonContext.Default.DocumentPageOutput
                        )
                    );
                    fetchedCount += 1;
                    bytesDownloaded += page.BytesDownloaded;
                    sourceCounts[page.Source] = sourceCounts.GetValueOrDefault(page.Source) + 1;
                    continue;
                }

                if (failure is not null)
                {
                    await failures.WriteLineAsync(
                        JsonSerializer.Serialize(
                            failure,
                            RetsinformationDownloaderJsonContext.Default.DocumentFetchFailureOutput
                        )
                    );
                    failedCount += 1;

                    if (failure.StatusCode == 404)
                    {
                        notFoundCount += 1;
                    }
                }
            }
        }

        File.Move(tempDocumentsPath, documentsPath, true);
        File.Move(tempFailuresPath, failuresPath, true);

        var firstEntry = documentRefs.FirstOrDefault();
        var output = new ToolOutput(
            input.DocumentType!,
            input.Year!,
            outputDir,
            documentsPath,
            failuresPath,
            manifestPath,
            documentRefs.Count,
            fetchedCount,
            failedCount,
            notFoundCount,
            bytesDownloaded,
            sourceCounts,
            firstEntry is null ? null : DocumentXmlUrl(firstEntry.Url),
            firstEntry is null ? null : DocumentApiUrl(firstEntry.Url)
        );

        await WriteJsonFileAsync(manifestPath, output);
        return output;
    }

    private static async Task<(DocumentPageOutput? Page, DocumentFetchFailureOutput? Failure)> FetchDocumentAsync(
        HttpClient httpClient,
        SitemapEntry entry
    )
    {
        var xmlUrl = DocumentXmlUrl(entry.Url);
        using var xmlResponse = await httpClient.GetAsync(
            xmlUrl,
            HttpCompletionOption.ResponseContentRead
        );
        var xmlBytes = await xmlResponse.Content.ReadAsByteArrayAsync();

        if (xmlResponse.StatusCode == HttpStatusCode.NotFound)
        {
            return await FetchApiDocumentAsync(httpClient, entry);
        }

        if (!xmlResponse.IsSuccessStatusCode)
        {
            throw new HttpRequestException(
                $"XML endpoint {xmlUrl} returned HTTP {(int)xmlResponse.StatusCode} "
                + xmlResponse.ReasonPhrase
            );
        }

        return (
            new DocumentPageOutput(
                entry,
                xmlUrl,
                XmlEndpointSource,
                (int)xmlResponse.StatusCode,
                ContentType(xmlResponse),
                DecodeBody(xmlBytes, xmlResponse),
                xmlBytes.Length
            ),
            null
        );
    }

    private static async Task<(DocumentPageOutput? Page, DocumentFetchFailureOutput? Failure)> FetchApiDocumentAsync(
        HttpClient httpClient,
        SitemapEntry entry
    )
    {
        var apiUrl = DocumentApiUrl(entry.Url);
        using var request = new HttpRequestMessage(HttpMethod.Post, apiUrl)
        {
            Content = new StringContent("{\"isRawHtml\":false}", Encoding.UTF8, "application/json"),
        };
        using var response = await httpClient.SendAsync(
            request,
            HttpCompletionOption.ResponseContentRead
        );
        var bytes = await response.Content.ReadAsByteArrayAsync();
        var body = DecodeBody(bytes, response);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return (
                null,
                new DocumentFetchFailureOutput(
                    entry,
                    apiUrl,
                    ApiDocumentSource,
                    (int)response.StatusCode,
                    response.ReasonPhrase ?? "not found"
                )
            );
        }

        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException(
                $"API fallback {apiUrl} returned HTTP {(int)response.StatusCode} "
                + response.ReasonPhrase
            );
        }

        if (!ApiDocumentResponseHasDocument(body))
        {
            return (
                null,
                new DocumentFetchFailureOutput(
                    entry,
                    apiUrl,
                    ApiDocumentSource,
                    (int)response.StatusCode,
                    "API fallback returned no document"
                )
            );
        }

        return (
            new DocumentPageOutput(
                entry,
                apiUrl,
                ApiDocumentSource,
                (int)response.StatusCode,
                ContentType(response),
                body,
                bytes.Length
            ),
            null
        );
    }

    private static string DocumentXmlUrl(string url)
    {
        var baseUrl = url.TrimEnd('/');
        return baseUrl.EndsWith("/xml", StringComparison.Ordinal) ? baseUrl : baseUrl + "/xml";
    }

    private static string DocumentApiUrl(string url)
    {
        var builder = new UriBuilder(url.TrimEnd('/'));
        var path = builder.Path;

        if (path.EndsWith("/xml", StringComparison.Ordinal))
        {
            path = path[..^"/xml".Length];
        }

        builder.Path = "/api/document" + path;
        builder.Query = string.Empty;
        builder.Fragment = string.Empty;

        return builder.Uri.ToString();
    }

    private static bool ApiDocumentResponseHasDocument(string body)
    {
        try
        {
            using var documents = JsonDocument.Parse(body);

            if (
                documents.RootElement.ValueKind != JsonValueKind.Array
                || documents.RootElement.GetArrayLength() == 0
            )
            {
                return false;
            }

            var firstDocument = documents.RootElement[0];

            if (firstDocument.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            return !firstDocument.TryGetProperty("id", out var id)
                || id.ValueKind != JsonValueKind.Number
                || !id.TryGetInt32(out var idValue)
                || idValue != -1;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static string ContentType(HttpResponseMessage response)
    {
        return response.Content.Headers.ContentType?.ToString() ?? string.Empty;
    }

    private static string DecodeBody(byte[] body, HttpResponseMessage response)
    {
        var charset = response.Content.Headers.ContentType?.CharSet;

        try
        {
            return Encoding.GetEncoding(string.IsNullOrWhiteSpace(charset) ? "utf-8" : charset)
                .GetString(body);
        }
        catch (ArgumentException)
        {
            return Encoding.UTF8.GetString(body);
        }
    }

    private static async Task WriteJsonFileAsync(string path, ToolOutput output)
    {
        var tempPath = path + ".tmp";
        await File.WriteAllTextAsync(
            tempPath,
            JsonSerializer.Serialize(
                output,
                RetsinformationDownloaderJsonContext.Default.ToolOutput
            ),
            new UTF8Encoding(false)
        );
        File.Move(tempPath, path, true);
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

        if (string.IsNullOrWhiteSpace(input.OutputDir))
        {
            throw new InvalidOperationException("Input must contain outputDir.");
        }

        if (input.RetsinfoSitemapPage is null)
        {
            throw new InvalidOperationException("Input must contain retsinfoSitemapPage.");
        }
    }

    private static void ValidateEntry(SitemapEntry entry)
    {
        if (string.IsNullOrWhiteSpace(entry.Url))
        {
            throw new InvalidOperationException("Sitemap entry is missing url.");
        }
    }
}

internal sealed record ToolInput(
    [property: JsonPropertyName("documentType")] string? DocumentType,
    [property: JsonPropertyName("year")] string? Year,
    [property: JsonPropertyName("outputDir")] string? OutputDir,
    [property: JsonPropertyName("userAgent")] string? UserAgent,
    [property: JsonPropertyName("timeoutSeconds")] double? TimeoutSeconds,
    [property: JsonPropertyName("retsinfoSitemapPage")] List<SitemapEntry>? RetsinfoSitemapPage
);

internal sealed record SitemapEntry(
    [property: JsonPropertyName("url")] string Url,
    [property: JsonPropertyName("lastmod")] string Lastmod,
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("year")] string Year,
    [property: JsonPropertyName("type")] string Type
);

internal sealed record DocumentPageOutput(
    [property: JsonPropertyName("entry")] SitemapEntry Entry,
    [property: JsonPropertyName("sourceUrl")] string SourceUrl,
    [property: JsonPropertyName("source")] string Source,
    [property: JsonPropertyName("statusCode")] int StatusCode,
    [property: JsonPropertyName("contentType")] string ContentType,
    [property: JsonPropertyName("body")] string Body,
    [property: JsonPropertyName("bytesDownloaded")] long BytesDownloaded
);

internal sealed record DocumentFetchFailureOutput(
    [property: JsonPropertyName("entry")] SitemapEntry Entry,
    [property: JsonPropertyName("sourceUrl")] string SourceUrl,
    [property: JsonPropertyName("source")] string Source,
    [property: JsonPropertyName("statusCode")] int StatusCode,
    [property: JsonPropertyName("reason")] string Reason
);

internal sealed record ToolOutput(
    [property: JsonPropertyName("documentType")] string DocumentType,
    [property: JsonPropertyName("year")] string Year,
    [property: JsonPropertyName("outputDir")] string OutputDir,
    [property: JsonPropertyName("documentsPath")] string DocumentsPath,
    [property: JsonPropertyName("failuresPath")] string FailuresPath,
    [property: JsonPropertyName("manifestPath")] string ManifestPath,
    [property: JsonPropertyName("availableRefCount")] int AvailableRefCount,
    [property: JsonPropertyName("fetchedCount")] int FetchedCount,
    [property: JsonPropertyName("failedCount")] int FailedCount,
    [property: JsonPropertyName("notFoundCount")] int NotFoundCount,
    [property: JsonPropertyName("bytesDownloaded")] long BytesDownloaded,
    [property: JsonPropertyName("sourceCounts")] Dictionary<string, int> SourceCounts,
    [property: JsonPropertyName("firstXmlUrl")] string? FirstXmlUrl,
    [property: JsonPropertyName("firstApiUrl")] string? FirstApiUrl
);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(ToolInput))]
[JsonSerializable(typeof(ToolOutput))]
[JsonSerializable(typeof(DocumentPageOutput))]
[JsonSerializable(typeof(DocumentFetchFailureOutput))]
internal partial class RetsinformationDownloaderJsonContext : JsonSerializerContext;
