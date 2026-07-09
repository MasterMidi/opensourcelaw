#:project ./shared/OpenSourceLaw.Tools.csproj

using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.Logging;
using OpenSourceLaw.Tools;

return await RetsinformationDownloaderTool.RunAsync();

internal static partial class RetsinformationDownloaderTool
{
    // ponytail: local constants are enough until tools share release metadata.
    private const string ToolName = "retsinformation_downloader";
    private const string ToolVersion = "0.1";
    private static readonly ILogger Logger = ToolLog.Logger;

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
            LogToolFailed(Logger, error, error.Message);
            return 1;
        }
    }

    private static Task<ToolOutput> RunAsync(ToolInput input) =>
        input.S3 is null ? RunLocalAsync(input) : RunS3Async(input);

    private static async Task<ToolOutput> RunLocalAsync(ToolInput input)
    {
        var timeoutSeconds = input.TimeoutSeconds ?? 30.0;

        if (timeoutSeconds <= 0)
        {
            throw new InvalidOperationException("timeoutSeconds must be greater than zero.");
        }

        var outputDir = Path.GetFullPath(input.OutputDir!);
        var xmlDirectoryPath = Path.Combine(outputDir, "xml");
        var jsonLdDirectoryPath = Path.Combine(outputDir, "jsonld");
        var metadataDirectoryPath = Path.Combine(outputDir, "metadata");
        var failuresPath = Path.Combine(outputDir, "failures.jsonl");
        var manifestPath = Path.Combine(outputDir, "manifest.json");
        var tempXmlDirectoryPath = xmlDirectoryPath + ".tmp";
        var tempJsonLdDirectoryPath = jsonLdDirectoryPath + ".tmp";
        var tempMetadataDirectoryPath = metadataDirectoryPath + ".tmp";
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

        if (Directory.Exists(tempMetadataDirectoryPath))
        {
            Directory.Delete(tempMetadataDirectoryPath, true);
        }

        if (Directory.Exists(tempJsonLdDirectoryPath))
        {
            Directory.Delete(tempJsonLdDirectoryPath, true);
        }

        Directory.CreateDirectory(tempXmlDirectoryPath);
        Directory.CreateDirectory(tempJsonLdDirectoryPath);
        Directory.CreateDirectory(tempMetadataDirectoryPath);

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

        LogDownloadStarted(Logger, documentRefs.Count, input.DocumentType!, input.Year!, xmlDirectoryPath);

        await using (var failures = new StreamWriter(tempFailuresPath, false, new UTF8Encoding(false)))
        {
            async Task RecordFailureAsync(SitemapEntry entry, string url, HttpResponseMessage response)
            {
                failedCount += 1;

                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    notFoundCount += 1;
                }

                LogEndpointFailed(
                    Logger,
                    url,
                    (int)response.StatusCode,
                    response.ReasonPhrase ?? "request failed"
                );
                await failures.WriteLineAsync(
                    JsonSerializer.Serialize(
                        new DocumentFetchFailureOutput(
                            entry,
                            url,
                            (int)response.StatusCode,
                            response.ReasonPhrase ?? "request failed"
                        ),
                        RetsinformationDownloaderJsonContext.Default.DocumentFetchFailureOutput
                    )
                );
            }

            for (var index = 0; index < documentRefs.Count; index += 1)
            {
                var entry = documentRefs[index];
                ValidateEntry(entry);
                LogFetchingDocument(Logger, index + 1, documentRefs.Count, entry.Type, entry.Year, entry.Id);

                var xmlUrl = DocumentXmlUrl(entry.Url);
                var jsonLdUrl = DocumentJsonLdUrl(entry.Url);
                using var response = await httpClient.GetAsync(
                    xmlUrl,
                    HttpCompletionOption.ResponseContentRead
                );
                var bytes = await response.Content.ReadAsByteArrayAsync();

                if (response.IsSuccessStatusCode)
                {
                    using var jsonLdResponse = await httpClient.GetAsync(
                        jsonLdUrl,
                        HttpCompletionOption.ResponseContentRead
                    );
                    var jsonLdBytes = await jsonLdResponse.Content.ReadAsByteArrayAsync();

                    if (!jsonLdResponse.IsSuccessStatusCode)
                    {
                        await RecordFailureAsync(entry, jsonLdUrl, jsonLdResponse);
                        continue;
                    }

                    var fileName = DocumentFileName(entry, index + 1);
                    await File.WriteAllBytesAsync(Path.Combine(tempXmlDirectoryPath, fileName), bytes);
                    await File.WriteAllBytesAsync(
                        Path.Combine(tempJsonLdDirectoryPath, Path.ChangeExtension(fileName, ".json")),
                        jsonLdBytes
                    );
                    await WriteJsonFileAsync(
                        Path.Combine(tempMetadataDirectoryPath, Path.ChangeExtension(fileName, ".json")),
                        DocumentMetadata(entry, xmlUrl, bytes),
                        RetsinformationDownloaderJsonContext.Default.DocumentMetadataOutput
                    );
                    LogSavedDocument(Logger, fileName, bytes.Length, jsonLdBytes.Length);
                    downloadedCount += 1;
                    bytesDownloaded += bytes.Length;
                    continue;
                }

                await RecordFailureAsync(entry, xmlUrl, response);
            }
        }

        if (Directory.Exists(xmlDirectoryPath))
        {
            Directory.Delete(xmlDirectoryPath, true);
        }

        if (Directory.Exists(metadataDirectoryPath))
        {
            Directory.Delete(metadataDirectoryPath, true);
        }

        if (Directory.Exists(jsonLdDirectoryPath))
        {
            Directory.Delete(jsonLdDirectoryPath, true);
        }

        Directory.Move(tempXmlDirectoryPath, xmlDirectoryPath);
        Directory.Move(tempJsonLdDirectoryPath, jsonLdDirectoryPath);
        Directory.Move(tempMetadataDirectoryPath, metadataDirectoryPath);
        File.Move(tempFailuresPath, failuresPath, true);

        var firstEntry = documentRefs.FirstOrDefault();
        var output = new ToolOutput(
            input.DocumentType!,
            input.Year!,
            outputDir,
            xmlDirectoryPath,
            jsonLdDirectoryPath,
            metadataDirectoryPath,
            failuresPath,
            manifestPath,
            documentRefs.Count,
            downloadedCount,
            failedCount,
            notFoundCount,
            bytesDownloaded,
            firstEntry is null ? null : DocumentXmlUrl(firstEntry.Url)
        );

        await WriteJsonFileAsync(
            manifestPath,
            output,
            RetsinformationDownloaderJsonContext.Default.ToolOutput
        );
        LogDownloadFinished(Logger, downloadedCount, documentRefs.Count, failedCount, notFoundCount);
        return output;
    }

    private static async Task<ToolOutput> RunS3Async(ToolInput input)
    {
        var timeoutSeconds = input.TimeoutSeconds ?? 30.0;

        if (timeoutSeconds <= 0)
        {
            throw new InvalidOperationException("timeoutSeconds must be greater than zero.");
        }

        var s3Input = input.S3!;
        var prefix = NormalizeS3Prefix(s3Input.Prefix!);
        var manifestKey = $"{prefix}/manifest.json";
        var latestKey = $"{prefix}/latest.json";
        var failuresKey = $"{prefix}/failures.jsonl";
        var documentRefs = input.RetsinfoSitemapPage!
            .Where(entry => entry.Type == input.DocumentType && entry.Year == input.Year)
            .ToList();
        var completed = new List<DocumentCheckpointOutput>();
        var failures = new List<DocumentFetchFailureOutput>();
        var downloadedCount = 0;
        var skippedCount = 0;
        var failedCount = 0;
        var notFoundCount = 0;
        long bytesDownloaded = 0;

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
        using var s3 = new S3UploadClient(s3Input);

        LogDownloadStarted(Logger, documentRefs.Count, input.DocumentType!, input.Year!, prefix);

        async Task RecordFailureAsync(SitemapEntry entry, string url, HttpResponseMessage response)
        {
            failedCount += 1;

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                notFoundCount += 1;
            }

            LogEndpointFailed(
                Logger,
                url,
                (int)response.StatusCode,
                response.ReasonPhrase ?? "request failed"
            );
            failures.Add(
                new DocumentFetchFailureOutput(
                    entry,
                    url,
                    (int)response.StatusCode,
                    response.ReasonPhrase ?? "request failed"
                )
            );
        }

        for (var index = 0; index < documentRefs.Count; index += 1)
        {
            var entry = documentRefs[index];
            ValidateEntry(entry);
            LogFetchingDocument(Logger, index + 1, documentRefs.Count, entry.Type, entry.Year, entry.Id);

            var stem = DocumentStableStem(entry);
            var checkpointKey = $"{prefix}/checkpoints/{stem}.json";
            var checkpoint = await s3.GetJsonOrNullAsync(
                checkpointKey,
                RetsinformationDownloaderJsonContext.Default.DocumentCheckpointOutput
            );

            if (CheckpointMatches(checkpoint, entry))
            {
                completed.Add(checkpoint!);
                skippedCount += 1;
                continue;
            }

            var xmlUrl = DocumentXmlUrl(entry.Url);
            var jsonLdUrl = DocumentJsonLdUrl(entry.Url);
            using var response = await httpClient.GetAsync(
                xmlUrl,
                HttpCompletionOption.ResponseContentRead
            );

            if (!response.IsSuccessStatusCode)
            {
                await RecordFailureAsync(entry, xmlUrl, response);
                continue;
            }

            var bytes = await response.Content.ReadAsByteArrayAsync();
            using var jsonLdResponse = await httpClient.GetAsync(
                jsonLdUrl,
                HttpCompletionOption.ResponseContentRead
            );

            if (!jsonLdResponse.IsSuccessStatusCode)
            {
                await RecordFailureAsync(entry, jsonLdUrl, jsonLdResponse);
                continue;
            }

            var jsonLdBytes = await jsonLdResponse.Content.ReadAsByteArrayAsync();
            var xmlPath = $"xml/{stem}.xml";
            var jsonLdPath = $"jsonld/{stem}.json";
            var metadataPath = $"metadata/{stem}.json";
            var xmlKey = $"{prefix}/{xmlPath}";
            var jsonLdKey = $"{prefix}/{jsonLdPath}";
            var metadataKey = $"{prefix}/{metadataPath}";
            var metadata = DocumentMetadata(entry, xmlUrl, bytes);
            var nextCheckpoint = new DocumentCheckpointOutput(
                entry,
                xmlKey,
                jsonLdKey,
                metadataKey,
                xmlPath,
                jsonLdPath,
                metadataPath,
                Hex(SHA256.HashData(bytes)),
                Hex(SHA256.HashData(jsonLdBytes)),
                bytes.Length,
                jsonLdBytes.Length
            );

            await s3.PutBytesAsync(xmlKey, bytes, "application/xml");
            await s3.PutBytesAsync(jsonLdKey, jsonLdBytes, "application/json");
            await s3.PutJsonAsync(
                metadataKey,
                metadata,
                RetsinformationDownloaderJsonContext.Default.DocumentMetadataOutput
            );
            await s3.PutJsonAsync(
                checkpointKey,
                nextCheckpoint,
                RetsinformationDownloaderJsonContext.Default.DocumentCheckpointOutput
            );

            completed.Add(nextCheckpoint);
            downloadedCount += 1;
            bytesDownloaded += bytes.Length;
            LogSavedDocument(Logger, stem, bytes.Length, jsonLdBytes.Length);
        }

        var failuresBytes = FailureJsonLines(failures);
        await s3.PutBytesAsync(failuresKey, failuresBytes, "application/x-ndjson");

        var payloadHashes = completed
            .SelectMany(
                checkpoint => new[]
                {
                    new RawPayloadHash(checkpoint.XmlPath, checkpoint.XmlSha256),
                    new RawPayloadHash(checkpoint.JsonLdPath, checkpoint.JsonLdSha256),
                }
            )
            .Append(new RawPayloadHash("failures.jsonl", Hex(SHA256.HashData(failuresBytes))))
            .ToList();
        var dataVersion = RawDocumentDataVersion(payloadHashes);
        var objects = completed.SelectMany(RawObjects).ToList();
        objects.Add(new RawObjectOutput(failuresKey, "failures.jsonl"));
        objects.Add(new RawObjectOutput(manifestKey, "manifest.json"));
        objects.Sort((left, right) => string.CompareOrdinal(left.Path, right.Path));

        var firstEntry = documentRefs.FirstOrDefault();
        var output = new ToolOutput(
            input.DocumentType!,
            input.Year!,
            null,
            null,
            null,
            null,
            null,
            null,
            documentRefs.Count,
            downloadedCount,
            failedCount,
            notFoundCount,
            bytesDownloaded,
            firstEntry is null ? null : DocumentXmlUrl(firstEntry.Url),
            completed.Count,
            skippedCount,
            s3Input.Bucket!,
            prefix,
            latestKey,
            manifestKey,
            dataVersion,
            objects
        );

        await s3.PutBytesAsync(
            manifestKey,
            JsonSerializer.SerializeToUtf8Bytes(
                output,
                RetsinformationDownloaderJsonContext.Default.ToolOutput
            ),
            "application/json"
        );
        LogDownloadFinished(Logger, completed.Count, documentRefs.Count, failedCount, notFoundCount);
        return output;
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "Downloading {DocumentCount} {DocumentType}/{Year} XML documents to {XmlDirectoryPath}."
    )]
    private static partial void LogDownloadStarted(
        ILogger logger,
        int documentCount,
        string documentType,
        string year,
        string xmlDirectoryPath
    );

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Fetching document {Index}/{DocumentCount}: {DocumentType}/{Year}/{DocumentId}."
    )]
    private static partial void LogFetchingDocument(
        ILogger logger,
        int index,
        int documentCount,
        string documentType,
        string year,
        string documentId
    );

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Debug,
        Message = "Saved {FileName}: XML {XmlBytes} bytes, JSON-LD {JsonLdBytes} bytes."
    )]
    private static partial void LogSavedDocument(
        ILogger logger,
        string fileName,
        int xmlBytes,
        int jsonLdBytes
    );

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Warning,
        Message = "Endpoint {Url} returned HTTP {StatusCode} {Reason}."
    )]
    private static partial void LogEndpointFailed(
        ILogger logger,
        string url,
        int statusCode,
        string reason
    );

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Information,
        Message = "Downloaded {DownloadedCount}/{DocumentCount} XML documents ({FailedCount} failed, {NotFoundCount} not found)."
    )]
    private static partial void LogDownloadFinished(
        ILogger logger,
        int downloadedCount,
        int documentCount,
        int failedCount,
        int notFoundCount
    );

    [LoggerMessage(
        EventId = 6,
        Level = LogLevel.Error,
        Message = "Downloader failed: {ErrorMessage}."
    )]
    private static partial void LogToolFailed(
        ILogger logger,
        Exception exception,
        string errorMessage
    );

    private static string DocumentXmlUrl(string url)
    {
        var baseUrl = url.TrimEnd('/');
        return baseUrl.EndsWith("/xml", StringComparison.Ordinal) ? baseUrl : baseUrl + "/xml";
    }

    private static string DocumentJsonLdUrl(string url) => url.TrimEnd('/') + ".json";

    private static string DocumentFileName(SitemapEntry entry, int index)
    {
        var id = string.IsNullOrWhiteSpace(entry.Id)
            ? "document"
            : string.Concat(entry.Id.Select(ch => char.IsLetterOrDigit(ch) ? ch : '_'));
        return $"{index:000000}_{id}.xml";
    }

    private static DocumentMetadataOutput DocumentMetadata(
        SitemapEntry entry,
        string xmlUrl,
        byte[] bytes
    ) => new(
        new DocumentMetadataTool(ToolName, ToolVersion),
        new DocumentMetadataSource(
            entry.Type,
            entry.Year,
            entry.Id,
            $"/eli/{entry.Type}/{entry.Year}/{entry.Id}",
            entry.Url,
            xmlUrl
        ),
        DateTimeOffset.UtcNow,
        "application/xml",
        bytes.Length,
        Hex(SHA256.HashData(bytes))
    );

    private static string NormalizeS3Prefix(string prefix) => prefix.Trim('/');

    private static string DocumentStableStem(SitemapEntry entry) =>
        Hex(SHA256.HashData(Encoding.UTF8.GetBytes(entry.Url)))[..32];

    private static bool CheckpointMatches(DocumentCheckpointOutput? checkpoint, SitemapEntry entry) =>
        checkpoint is not null
        && checkpoint.Entry.Url == entry.Url
        && checkpoint.Entry.Lastmod == entry.Lastmod
        && checkpoint.Entry.Id == entry.Id
        && checkpoint.Entry.Year == entry.Year
        && checkpoint.Entry.Type == entry.Type;

    private static IEnumerable<RawObjectOutput> RawObjects(DocumentCheckpointOutput checkpoint)
    {
        yield return new RawObjectOutput(checkpoint.XmlKey, checkpoint.XmlPath);
        yield return new RawObjectOutput(checkpoint.JsonLdKey, checkpoint.JsonLdPath);
        yield return new RawObjectOutput(checkpoint.MetadataKey, checkpoint.MetadataPath);
    }

    private static byte[] FailureJsonLines(List<DocumentFetchFailureOutput> failures)
    {
        var builder = new StringBuilder();

        foreach (var failure in failures)
        {
            builder.Append(
                JsonSerializer.Serialize(
                    failure,
                    RetsinformationDownloaderJsonContext.Default.DocumentFetchFailureOutput
                )
            );
            builder.Append('\n');
        }

        return Encoding.UTF8.GetBytes(builder.ToString());
    }

    private static string RawDocumentDataVersion(List<RawPayloadHash> payloadHashes)
    {
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        foreach (var payload in payloadHashes.OrderBy(payload => payload.Path, StringComparer.Ordinal))
        {
            hasher.AppendData(Encoding.UTF8.GetBytes(payload.Path));
            hasher.AppendData(new byte[] { 0 });
            hasher.AppendData(Encoding.ASCII.GetBytes(payload.Sha256));
            hasher.AppendData(new byte[] { 0 });
        }

        return Hex(hasher.GetHashAndReset());
    }

    private static string Hex(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();

    private static async Task WriteJsonFileAsync<T>(
        string path,
        T output,
        JsonTypeInfo<T> jsonTypeInfo
    )
    {
        var tempPath = path + ".tmp";
        await File.WriteAllTextAsync(
            tempPath,
            JsonSerializer.Serialize(output, jsonTypeInfo),
            new UTF8Encoding(false)
        );
        File.Move(tempPath, path, true);
    }

    private sealed class S3UploadClient : IDisposable
    {
        private readonly S3UploadInput input;
        private readonly HttpClient httpClient = new();

        public S3UploadClient(S3UploadInput input)
        {
            this.input = input;
        }

        public async Task<T?> GetJsonOrNullAsync<T>(string key, JsonTypeInfo<T> jsonTypeInfo)
        {
            var bytes = await RequestAsync("GET", key, Array.Empty<byte>(), null, true);

            if (bytes is null)
            {
                return default;
            }

            try
            {
                return JsonSerializer.Deserialize(bytes, jsonTypeInfo);
            }
            catch (JsonException)
            {
                return default;
            }
        }

        public Task PutBytesAsync(string key, byte[] bytes, string contentType) =>
            RequestAsync("PUT", key, bytes, contentType, false);

        public Task PutJsonAsync<T>(string key, T value, JsonTypeInfo<T> jsonTypeInfo) =>
            PutBytesAsync(
                key,
                JsonSerializer.SerializeToUtf8Bytes(value, jsonTypeInfo),
                "application/json"
            );

        public void Dispose()
        {
            httpClient.Dispose();
        }

        private async Task<byte[]?> RequestAsync(
            string method,
            string key,
            byte[] body,
            string? contentType,
            bool nullOnNotFound
        )
        {
            var attempts = Math.Max(1, input.MaxAttempts ?? 3);

            for (var attempt = 1; attempt <= attempts; attempt += 1)
            {
                using var request = SignedRequest(method, key, body, contentType);
                using var response = await httpClient.SendAsync(request);

                if (response.StatusCode == HttpStatusCode.NotFound && nullOnNotFound)
                {
                    return null;
                }

                if (response.IsSuccessStatusCode)
                {
                    return await response.Content.ReadAsByteArrayAsync();
                }

                if ((int)response.StatusCode >= 500 && attempt < attempts)
                {
                    await Task.Delay(TimeSpan.FromSeconds(attempt));
                    continue;
                }

                var detail = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException(
                    $"S3 {method} s3://{input.Bucket}/{key} failed HTTP {(int)response.StatusCode}: {detail}"
                );
            }

            throw new InvalidOperationException("unreachable");
        }

        private HttpRequestMessage SignedRequest(
            string method,
            string key,
            byte[] body,
            string? contentType
        )
        {
            var now = DateTimeOffset.UtcNow;
            var amzDate = now.ToString("yyyyMMdd'T'HHmmss'Z'");
            var dateStamp = now.ToString("yyyyMMdd");
            var payloadHash = Hex(SHA256.HashData(body));
            var (url, canonicalUri, host) = S3Url(key);
            var signedHeadersMap = new SortedDictionary<string, string>(StringComparer.Ordinal)
            {
                ["host"] = host,
                ["x-amz-content-sha256"] = payloadHash,
                ["x-amz-date"] = amzDate,
            };

            if (contentType is not null)
            {
                signedHeadersMap["content-type"] = contentType;
            }

            var signedHeaders = string.Join(";", signedHeadersMap.Keys);
            var canonicalHeaders = string.Concat(
                signedHeadersMap.Select(pair => $"{pair.Key}:{pair.Value.Trim()}\n")
            );
            var canonicalRequest = string.Join(
                "\n",
                new[]
                {
                    method,
                    canonicalUri,
                    "",
                    canonicalHeaders,
                    signedHeaders,
                    payloadHash,
                }
            );
            var scope = $"{dateStamp}/{input.Region}/s3/aws4_request";
            var stringToSign = string.Join(
                "\n",
                new[]
                {
                    "AWS4-HMAC-SHA256",
                    amzDate,
                    scope,
                    Hex(SHA256.HashData(Encoding.UTF8.GetBytes(canonicalRequest))),
                }
            );
            var signature = Hex(
                HmacSha256(S3SigningKey(input.SecretAccessKey!, dateStamp, input.Region!), stringToSign)
            );
            var request = new HttpRequestMessage(new HttpMethod(method), url);
            request.Headers.Host = host;
            request.Headers.TryAddWithoutValidation("x-amz-content-sha256", payloadHash);
            request.Headers.TryAddWithoutValidation("x-amz-date", amzDate);
            request.Headers.TryAddWithoutValidation(
                "authorization",
                $"AWS4-HMAC-SHA256 Credential={input.AccessKeyId}/{scope}, SignedHeaders={signedHeaders}, Signature={signature}"
            );

            if (method == "PUT")
            {
                request.Content = new ByteArrayContent(body);
                request.Content.Headers.TryAddWithoutValidation("content-type", contentType);
            }

            return request;
        }

        private (Uri Url, string CanonicalUri, string Host) S3Url(string key)
        {
            var endpoint = new Uri(input.EndpointUrl!.TrimEnd('/'));
            var pathParts = endpoint.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries).ToList();
            pathParts.Add(input.Bucket!);
            pathParts.AddRange(key.Split('/', StringSplitOptions.RemoveEmptyEntries));

            var canonicalUri = "/" + string.Join("/", pathParts.Select(Uri.EscapeDataString));
            var host = endpoint.IsDefaultPort ? endpoint.Host : $"{endpoint.Host}:{endpoint.Port}";
            return (new Uri(endpoint.GetLeftPart(UriPartial.Authority) + canonicalUri), canonicalUri, host);
        }

        private static byte[] S3SigningKey(string secretKey, string dateStamp, string region)
        {
            var dateKey = HmacSha256(Encoding.UTF8.GetBytes("AWS4" + secretKey), dateStamp);
            var regionKey = HmacSha256(dateKey, region);
            var serviceKey = HmacSha256(regionKey, "s3");
            return HmacSha256(serviceKey, "aws4_request");
        }

        private static byte[] HmacSha256(byte[] key, string value) =>
            HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(value));
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

        if (input.S3 is null && string.IsNullOrWhiteSpace(input.OutputDir))
        {
            throw new InvalidOperationException("Input must contain outputDir.");
        }

        if (input.S3 is not null)
        {
            ValidateS3(input.S3);
        }

        if (input.RetsinfoSitemapPage is null)
        {
            throw new InvalidOperationException("Input must contain retsinfoSitemapPage.");
        }
    }

    private static void ValidateS3(S3UploadInput s3)
    {
        if (string.IsNullOrWhiteSpace(s3.Bucket))
        {
            throw new InvalidOperationException("Input s3 must contain bucket.");
        }

        if (string.IsNullOrWhiteSpace(s3.EndpointUrl))
        {
            throw new InvalidOperationException("Input s3 must contain endpointUrl.");
        }

        if (string.IsNullOrWhiteSpace(s3.Region))
        {
            throw new InvalidOperationException("Input s3 must contain region.");
        }

        if (string.IsNullOrWhiteSpace(s3.AccessKeyId))
        {
            throw new InvalidOperationException("Input s3 must contain accessKeyId.");
        }

        if (string.IsNullOrWhiteSpace(s3.SecretAccessKey))
        {
            throw new InvalidOperationException("Input s3 must contain secretAccessKey.");
        }

        if (string.IsNullOrWhiteSpace(s3.Prefix))
        {
            throw new InvalidOperationException("Input s3 must contain prefix.");
        }

        if (s3.MaxAttempts is <= 0)
        {
            throw new InvalidOperationException("Input s3 maxAttempts must be greater than zero.");
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
    [property: JsonPropertyName("retsinfoSitemapPage")] List<SitemapEntry>? RetsinfoSitemapPage,
    [property: JsonPropertyName("s3")] S3UploadInput? S3
);

internal sealed record S3UploadInput(
    [property: JsonPropertyName("bucket")] string? Bucket,
    [property: JsonPropertyName("endpointUrl")] string? EndpointUrl,
    [property: JsonPropertyName("region")] string? Region,
    [property: JsonPropertyName("accessKeyId")] string? AccessKeyId,
    [property: JsonPropertyName("secretAccessKey")] string? SecretAccessKey,
    [property: JsonPropertyName("prefix")] string? Prefix,
    [property: JsonPropertyName("maxAttempts")] int? MaxAttempts
);

internal sealed record DocumentFetchFailureOutput(
    [property: JsonPropertyName("entry")] SitemapEntry Entry,
    [property: JsonPropertyName("sourceUrl")] string SourceUrl,
    [property: JsonPropertyName("statusCode")] int StatusCode,
    [property: JsonPropertyName("reason")] string Reason
);

internal sealed record DocumentMetadataTool(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("version")] string Version
);

internal sealed record DocumentMetadataSource(
    [property: JsonPropertyName("eli_type")] string EliType,
    [property: JsonPropertyName("year")] string Year,
    [property: JsonPropertyName("number")] string Number,
    [property: JsonPropertyName("eli_uri")] string EliUri,
    [property: JsonPropertyName("source_url")] string SourceUrl,
    [property: JsonPropertyName("xml_url")] string XmlUrl
);

internal sealed record DocumentMetadataOutput(
    [property: JsonPropertyName("tool")] DocumentMetadataTool Tool,
    [property: JsonPropertyName("source")] DocumentMetadataSource Source,
    [property: JsonPropertyName("fetched_at")] DateTimeOffset FetchedAt,
    [property: JsonPropertyName("content_type")] string ContentType,
    [property: JsonPropertyName("content_bytes")] int ContentBytes,
    [property: JsonPropertyName("sha256")] string Sha256
);

internal sealed record DocumentCheckpointOutput(
    [property: JsonPropertyName("entry")] SitemapEntry Entry,
    [property: JsonPropertyName("xmlKey")] string XmlKey,
    [property: JsonPropertyName("jsonLdKey")] string JsonLdKey,
    [property: JsonPropertyName("metadataKey")] string MetadataKey,
    [property: JsonPropertyName("xmlPath")] string XmlPath,
    [property: JsonPropertyName("jsonLdPath")] string JsonLdPath,
    [property: JsonPropertyName("metadataPath")] string MetadataPath,
    [property: JsonPropertyName("xmlSha256")] string XmlSha256,
    [property: JsonPropertyName("jsonLdSha256")] string JsonLdSha256,
    [property: JsonPropertyName("xmlBytes")] int XmlBytes,
    [property: JsonPropertyName("jsonLdBytes")] int JsonLdBytes
);

internal sealed record RawObjectOutput(
    [property: JsonPropertyName("key")] string Key,
    [property: JsonPropertyName("path")] string Path
);

internal sealed record RawPayloadHash(string Path, string Sha256);

internal sealed record ToolOutput(
    [property: JsonPropertyName("documentType")] string DocumentType,
    [property: JsonPropertyName("year")] string Year,
    [property: JsonPropertyName("outputDir")] string? OutputDir,
    [property: JsonPropertyName("xmlDirectoryPath")] string? XmlDirectoryPath,
    [property: JsonPropertyName("jsonLdDirectoryPath")] string? JsonLdDirectoryPath,
    [property: JsonPropertyName("metadataDirectoryPath")] string? MetadataDirectoryPath,
    [property: JsonPropertyName("failuresPath")] string? FailuresPath,
    [property: JsonPropertyName("manifestPath")] string? ManifestPath,
    [property: JsonPropertyName("availableRefCount")] int AvailableRefCount,
    [property: JsonPropertyName("downloadedCount")] int DownloadedCount,
    [property: JsonPropertyName("failedCount")] int FailedCount,
    [property: JsonPropertyName("notFoundCount")] int NotFoundCount,
    [property: JsonPropertyName("bytesDownloaded")] long BytesDownloaded,
    [property: JsonPropertyName("firstXmlUrl")] string? FirstXmlUrl,
    [property: JsonPropertyName("completedCount")] int? CompletedCount = null,
    [property: JsonPropertyName("skippedCount")] int? SkippedCount = null,
    [property: JsonPropertyName("rawBucket")] string? RawBucket = null,
    [property: JsonPropertyName("rawPrefix")] string? RawPrefix = null,
    [property: JsonPropertyName("rawLatestKey")] string? RawLatestKey = null,
    [property: JsonPropertyName("rawManifestKey")] string? RawManifestKey = null,
    [property: JsonPropertyName("dataVersion")] string? DataVersion = null,
    [property: JsonPropertyName("objects")] List<RawObjectOutput>? Objects = null
);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(ToolInput))]
[JsonSerializable(typeof(ToolOutput))]
[JsonSerializable(typeof(DocumentFetchFailureOutput))]
[JsonSerializable(typeof(DocumentMetadataOutput))]
[JsonSerializable(typeof(DocumentCheckpointOutput))]
internal partial class RetsinformationDownloaderJsonContext : JsonSerializerContext;
