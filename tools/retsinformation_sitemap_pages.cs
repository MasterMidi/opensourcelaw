using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

return await SitemapPagesTool.RunAsync();

internal static class SitemapPagesTool
{
    private const string DefaultUserAgent = "opensourcelaw-retsinformation-ingest/0.1";
    private const string SitemapNamespace = "http://www.sitemaps.org/schemas/sitemap/0.9";
    private static readonly HashSet<string> DocumentTypes = new(StringComparer.Ordinal)
    {
        "fc",
        "fob",
        "ilt",
        "lta",
        "ltb",
        "ltc",
        "mt",
        "retsinfo",
    };

    public static async Task<int> RunAsync()
    {
        try
        {
            var inputJson = await Console.In.ReadToEndAsync();
            var input = JsonSerializer.Deserialize(
                inputJson,
                SitemapPagesJsonContext.Default.ToolInput
            );

            if (input?.Pages is null)
            {
                throw new InvalidOperationException("Input must contain a pages array.");
            }

            var timeoutSeconds = input.TimeoutSeconds ?? 30.0;

            if (timeoutSeconds <= 0)
            {
                throw new InvalidOperationException("timeoutSeconds must be greater than zero.");
            }

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
                    ? DefaultUserAgent
                    : input.UserAgent
            );

            var entries = new List<SitemapEntryOutput>();
            var pages = new List<PageOutput>();
            var skippedCount = 0;
            var totalFetchSeconds = 0.0;
            var totalParseSeconds = 0.0;

            foreach (var page in input.Pages.OrderBy(PageNumber))
            {
                ValidatePage(page);

                var fetchTimer = Stopwatch.StartNew();
                using var response = await httpClient.GetAsync(
                    page.Url,
                    HttpCompletionOption.ResponseContentRead
                );
                var body = await response.Content.ReadAsByteArrayAsync();
                fetchTimer.Stop();

                if (!response.IsSuccessStatusCode)
                {
                    throw new HttpRequestException(
                        $"Sitemap page {page.Page} returned HTTP {(int)response.StatusCode} "
                        + response.ReasonPhrase
                    );
                }

                var parseTimer = Stopwatch.StartNew();
                var parsedPage = ParseSitemapPage(body);
                parseTimer.Stop();

                var fetchSeconds = fetchTimer.Elapsed.TotalSeconds;
                var parseSeconds = parseTimer.Elapsed.TotalSeconds;
                entries.AddRange(parsedPage.Entries);
                skippedCount += parsedPage.SkippedCount;
                totalFetchSeconds += fetchSeconds;
                totalParseSeconds += parseSeconds;

                pages.Add(
                    new PageOutput(
                        page.Page!,
                        page.Url!,
                        parsedPage.Entries.Count,
                        parsedPage.SkippedCount,
                        entries.Count,
                        fetchSeconds,
                        parseSeconds
                    )
                );
            }

            var typeCounts = entries
                .GroupBy(entry => entry.Type)
                .OrderBy(group => group.Key, StringComparer.Ordinal)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);
            var output = new ToolOutput(
                input.Pages.Count,
                entries.Count,
                skippedCount,
                typeCounts,
                entries.Select(entry => entry.Year).Distinct(StringComparer.Ordinal).Count(),
                totalFetchSeconds,
                totalParseSeconds,
                pages,
                entries
            );

            Console.Write(JsonSerializer.Serialize(output, SitemapPagesJsonContext.Default.ToolOutput));
            return 0;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine(error.Message);
            return 1;
        }
    }

    private static int PageNumber(PageInput page)
    {
        if (!int.TryParse(page.Page, NumberStyles.None, CultureInfo.InvariantCulture, out var number))
        {
            throw new InvalidOperationException($"Invalid sitemap page number: {page.Page}");
        }

        return number;
    }

    private static void ValidatePage(PageInput page)
    {
        if (string.IsNullOrWhiteSpace(page.Page))
        {
            throw new InvalidOperationException("Sitemap page is missing page.");
        }

        if (string.IsNullOrWhiteSpace(page.Url))
        {
            throw new InvalidOperationException($"Sitemap page {page.Page} is missing url.");
        }
    }

    private static ParsedSitemapPage ParseSitemapPage(byte[] xmlContent)
    {
        var entries = new List<SitemapEntryOutput>();
        var skippedCount = 0;
        var settings = new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
        };

        using var stream = new MemoryStream(xmlContent);
        using var reader = XmlReader.Create(stream, settings);

        while (reader.Read())
        {
            if (
                reader.NodeType != XmlNodeType.Element
                || reader.LocalName != "url"
                || reader.NamespaceURI != SitemapNamespace
            )
            {
                continue;
            }

            var (locText, lastmodText) = ReadUrlElement(reader);

            if (string.IsNullOrWhiteSpace(locText) || string.IsNullOrWhiteSpace(lastmodText))
            {
                skippedCount += 1;
                continue;
            }

            var url = locText.Trim();
            var urlParts = ParseEliDocumentUrl(url);

            if (urlParts is null)
            {
                skippedCount += 1;
                continue;
            }

            entries.Add(
                new SitemapEntryOutput(
                    url,
                    lastmodText.Trim(),
                    urlParts.Id,
                    urlParts.Year,
                    urlParts.Type
                )
            );
        }

        return new ParsedSitemapPage(entries, skippedCount);
    }

    private static (string? Loc, string? Lastmod) ReadUrlElement(XmlReader reader)
    {
        using var subtree = reader.ReadSubtree();
        var urlElement = XElement.Load(subtree);
        var loc = urlElement.Elements().FirstOrDefault(element => element.Name.LocalName == "loc");
        var lastmod = urlElement.Elements()
            .FirstOrDefault(element => element.Name.LocalName == "lastmod");

        return (loc?.Value, lastmod?.Value);
    }

    private static EliDocumentUrlParts? ParseEliDocumentUrl(string url)
    {
        const string separator = "/eli/";
        var separatorIndex = url.IndexOf(separator, StringComparison.Ordinal);

        if (separatorIndex < 0)
        {
            return null;
        }

        var path = url[(separatorIndex + separator.Length)..];
        var queryIndex = path.IndexOfAny(new[] { '?', '#' });

        if (queryIndex >= 0)
        {
            path = path[..queryIndex];
        }

        var parts = path.Trim('/').Split('/');

        if (parts.Length is not 2 and not 3 || !DocumentTypes.Contains(parts[0]))
        {
            return null;
        }

        var documentType = parts[0];
        string documentId;
        string year;

        if (parts.Length == 2)
        {
            documentId = parts[1];

            if (documentType != "fc")
            {
                return null;
            }

            year = documentId.Length >= 4 ? documentId[..4] : documentId;
        }
        else
        {
            year = parts[1];
            documentId = parts[2];
        }

        if (year.Length != 4 || !year.All(char.IsDigit))
        {
            return null;
        }

        return new EliDocumentUrlParts(documentId, year, documentType);
    }
}

internal sealed record ToolInput(
    [property: JsonPropertyName("userAgent")] string? UserAgent,
    [property: JsonPropertyName("timeoutSeconds")] double? TimeoutSeconds,
    [property: JsonPropertyName("pages")] List<PageInput> Pages
);

internal sealed record PageInput(
    [property: JsonPropertyName("page")] string? Page,
    [property: JsonPropertyName("url")] string? Url
);

internal sealed record ParsedSitemapPage(
    List<SitemapEntryOutput> Entries,
    int SkippedCount
);

internal sealed record EliDocumentUrlParts(string Id, string Year, string Type);

internal sealed record SitemapEntryOutput(
    [property: JsonPropertyName("url")] string Url,
    [property: JsonPropertyName("lastmod")] string Lastmod,
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("year")] string Year,
    [property: JsonPropertyName("type")] string Type
);

internal sealed record PageOutput(
    [property: JsonPropertyName("page")] string Page,
    [property: JsonPropertyName("url")] string Url,
    [property: JsonPropertyName("entryCount")] int EntryCount,
    [property: JsonPropertyName("skippedCount")] int SkippedCount,
    [property: JsonPropertyName("totalEntryCount")] int TotalEntryCount,
    [property: JsonPropertyName("fetchSeconds")] double FetchSeconds,
    [property: JsonPropertyName("parseSeconds")] double ParseSeconds
);

internal sealed record ToolOutput(
    [property: JsonPropertyName("sitemapPageCount")] int SitemapPageCount,
    [property: JsonPropertyName("entryCount")] int EntryCount,
    [property: JsonPropertyName("skippedCount")] int SkippedCount,
    [property: JsonPropertyName("typeCounts")] Dictionary<string, int> TypeCounts,
    [property: JsonPropertyName("yearCount")] int YearCount,
    [property: JsonPropertyName("fetchSeconds")] double FetchSeconds,
    [property: JsonPropertyName("parseSeconds")] double ParseSeconds,
    [property: JsonPropertyName("pages")] List<PageOutput> Pages,
    [property: JsonPropertyName("entries")] List<SitemapEntryOutput> Entries
);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(ToolInput))]
[JsonSerializable(typeof(ToolOutput))]
internal partial class SitemapPagesJsonContext : JsonSerializerContext;
