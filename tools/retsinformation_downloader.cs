#:project ./shared/OpenSourceLaw.Tools.csproj

using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using OpenSourceLaw.Tools;

return await RetsinformationDownloaderTool.RunAsync();

internal static class RetsinformationDownloaderTool
{
    public static async Task<int> RunAsync()
    {
        using var pipes = DagsterPipes.Open();

        try
        {
            var input = await DagsterPipes.ReadInputAsync(
                RetsinformationDownloaderJsonContext.Default.ToolInput
            );

            Validate(input);

            var output = await RunAsync(input!);
            DagsterPipes.WriteOutput(output, RetsinformationDownloaderJsonContext.Default.ToolOutput);

            return 0;
        }
        catch (Exception error)
        {
            ToolLog.Error(error.Message);
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
        var xmlDirectoryPath = Path.Combine(outputDir, "xml");
        var failuresPath = Path.Combine(outputDir, "failures.jsonl");
        var manifestPath = Path.Combine(outputDir, "manifest.json");
        var tempXmlDirectoryPath = xmlDirectoryPath + ".tmp";
        var tempFailuresPath = failuresPath + ".tmp";
        var documentRefs = input.RetsinfoSitemapPage!
            .Where(entry => entry.Type == input.DocumentType && entry.Year == input.Year)
            .ToList();
        var downloadedCount = 0;
        var failedCount = 0;
        var notFoundCount = 0;
        long bytesDownloaded = 0;

        Directory.CreateDirectory(outputDir);

        if (Directory.Exists(tempXmlDirectoryPath))
        {
            Directory.Delete(tempXmlDirectoryPath, true);
        }

        Directory.CreateDirectory(tempXmlDirectoryPath);

        using var handler = new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.All,
        };
        using var httpClient = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(timeoutSeconds),
        };
        httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(
            string.IsNullOrWhiteSpace(input.UserAgent)
                ? ToolDefaults.UserAgent
                : input.UserAgent
        );

        ToolLog.Info($"Downloading {documentRefs.Count} XML documents to {xmlDirectoryPath}");

        await using (var failures = new StreamWriter(tempFailuresPath, false, new UTF8Encoding(false)))
        {
            for (var index = 0; index < documentRefs.Count; index += 1)
            {
                var entry = documentRefs[index];
                ValidateEntry(entry);

                var xmlUrl = DocumentXmlUrl(entry.Url);
                using var response = await httpClient.GetAsync(
                    xmlUrl,
                    HttpCompletionOption.ResponseContentRead
                );
                var bytes = await response.Content.ReadAsByteArrayAsync();

                if (response.IsSuccessStatusCode)
                {
                    await File.WriteAllBytesAsync(
                        Path.Combine(tempXmlDirectoryPath, DocumentFileName(entry, index + 1)),
                        bytes
                    );
                    downloadedCount += 1;
                    bytesDownloaded += bytes.Length;
                    continue;
                }

                failedCount += 1;

                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    notFoundCount += 1;
                }

                ToolLog.Warn(
                    $"XML endpoint {xmlUrl} returned HTTP {(int)response.StatusCode} {response.ReasonPhrase}"
                );
                await failures.WriteLineAsync(
                    JsonSerializer.Serialize(
                        new DocumentFetchFailureOutput(
                            entry,
                            xmlUrl,
                            (int)response.StatusCode,
                            response.ReasonPhrase ?? "request failed"
                        ),
                        RetsinformationDownloaderJsonContext.Default.DocumentFetchFailureOutput
                    )
                );
            }
        }

        if (Directory.Exists(xmlDirectoryPath))
        {
            Directory.Delete(xmlDirectoryPath, true);
        }

        Directory.Move(tempXmlDirectoryPath, xmlDirectoryPath);
        File.Move(tempFailuresPath, failuresPath, true);

        var firstEntry = documentRefs.FirstOrDefault();
        var output = new ToolOutput(
            input.DocumentType!,
            input.Year!,
            outputDir,
            xmlDirectoryPath,
            failuresPath,
            manifestPath,
            documentRefs.Count,
            downloadedCount,
            failedCount,
            notFoundCount,
            bytesDownloaded,
            firstEntry is null ? null : DocumentXmlUrl(firstEntry.Url)
        );

        await WriteJsonFileAsync(manifestPath, output);
        ToolLog.Info(
            $"Downloaded {downloadedCount}/{documentRefs.Count} XML documents ({failedCount} failed)"
        );
        return output;
    }

    private static string DocumentXmlUrl(string url)
    {
        var baseUrl = url.TrimEnd('/');
        return baseUrl.EndsWith("/xml", StringComparison.Ordinal) ? baseUrl : baseUrl + "/xml";
    }

    private static string DocumentFileName(SitemapEntry entry, int index)
    {
        var id = string.IsNullOrWhiteSpace(entry.Id)
            ? "document"
            : string.Concat(entry.Id.Select(ch => char.IsLetterOrDigit(ch) ? ch : '_'));
        return $"{index:000000}_{id}.xml";
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

internal sealed record DocumentFetchFailureOutput(
    [property: JsonPropertyName("entry")] SitemapEntry Entry,
    [property: JsonPropertyName("sourceUrl")] string SourceUrl,
    [property: JsonPropertyName("statusCode")] int StatusCode,
    [property: JsonPropertyName("reason")] string Reason
);

internal sealed record ToolOutput(
    [property: JsonPropertyName("documentType")] string DocumentType,
    [property: JsonPropertyName("year")] string Year,
    [property: JsonPropertyName("outputDir")] string OutputDir,
    [property: JsonPropertyName("xmlDirectoryPath")] string XmlDirectoryPath,
    [property: JsonPropertyName("failuresPath")] string FailuresPath,
    [property: JsonPropertyName("manifestPath")] string ManifestPath,
    [property: JsonPropertyName("availableRefCount")] int AvailableRefCount,
    [property: JsonPropertyName("downloadedCount")] int DownloadedCount,
    [property: JsonPropertyName("failedCount")] int FailedCount,
    [property: JsonPropertyName("notFoundCount")] int NotFoundCount,
    [property: JsonPropertyName("bytesDownloaded")] long BytesDownloaded,
    [property: JsonPropertyName("firstXmlUrl")] string? FirstXmlUrl
);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(ToolInput))]
[JsonSerializable(typeof(ToolOutput))]
[JsonSerializable(typeof(DocumentFetchFailureOutput))]
internal partial class RetsinformationDownloaderJsonContext : JsonSerializerContext;
