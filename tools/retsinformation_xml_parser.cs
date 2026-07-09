#:project ./shared/OpenSourceLaw.Tools.csproj

using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;
using Microsoft.Extensions.Logging;
using OpenSourceLaw.Tools;

return await RetsinformationXmlParserTool.RunAsync();

internal static partial class RetsinformationXmlParserTool
{
    private const string RetsinformationBaseUrl = "https://www.retsinformation.dk";
    private static readonly ILogger Logger = ToolLog.Logger;

    private static readonly HashSet<string> LawPubMedia = new(StringComparer.Ordinal)
    {
        "lta",
        "ltb",
        "ltc",
        "mt",
    };

    private static readonly HashSet<string> ContentRootTags = new(StringComparer.Ordinal)
    {
        "DokumentIndhold",
        "LovHoveddel",
        "BekHoveddel",
    };

    private static readonly HashSet<string> ContainerTags = new(StringComparer.Ordinal)
    {
        "ParagrafGruppe",
        "Bog",
        "Afsnit",
        "Bilag",
        "BilagIndhold",
        "DokumentIndhold",
        "LovHoveddel",
        "BekHoveddel",
    };

    private static readonly HashSet<string> TextGroupTags = new(StringComparer.Ordinal)
    {
        "TekstGruppe",
        "Indledning",
        "TraktatTabel",
        "Afslutning",
        "Hymne",
        "SkriftligFremsaettelse",
        "Fremsaetter",
        "FremsaettelsesIndledning",
        "Fremsaettelse",
        "ForslagsTitel",
        "ForslagsNummer",
        "FremsaettelsesTekst",
        "Praeambel",
        "BetaenkningsTekst",
        "BeretningsTekst",
        "UnderskriverTekst",
        "AendringsForslagBemaerkninger",
        "BemaerkningerTilAendringsNummer",
    };

    private static readonly HashSet<string> ParagraphTags = new(StringComparer.Ordinal)
    {
        "Paragraf",
        "TilParagraf",
        "AendringCentreretParagraf",
        "IkraftCentreretParagraf",
    };

    public static async Task<int> RunAsync()
    {
        using var pipes = DagsterPipes.Open();

        try
        {
            var input = await DagsterPipes.ReadInputAsync(
                RetsinformationXmlParserJsonContext.Default.ToolInput
            );
            Validate(input);

            var output = await RunAsync(input!);
            DagsterPipes.WriteOutput(
                output,
                RetsinformationXmlParserJsonContext.Default.ToolOutput
            );

            return 0;
        }
        catch (Exception error)
        {
            LogToolFailed(Logger, error, error.Message);
            return 1;
        }
    }

    private static async Task<ToolOutput> RunAsync(ToolInput input)
    {
        var inputDir = Path.GetFullPath(input.InputDir!);
        var outputDir = Path.GetFullPath(input.OutputDir!);
        var xmlDir = Path.Combine(inputDir, "xml");
        var jsonLdDir = Path.Combine(inputDir, "jsonld");
        var metadataDir = Path.Combine(inputDir, "metadata");

        if (!Directory.Exists(xmlDir))
        {
            throw new DirectoryNotFoundException($"XML directory does not exist: {xmlDir}");
        }

        var tempOutputDir = outputDir + ".tmp";
        if (Directory.Exists(tempOutputDir))
        {
            Directory.Delete(tempOutputDir, true);
        }

        Directory.CreateDirectory(tempOutputDir);

        var documentsPath = Path.Combine(tempOutputDir, "documents.jsonl");
        var unitsPath = Path.Combine(tempOutputDir, "units.jsonl");
        var sourcePositionsPath = Path.Combine(tempOutputDir, "source_positions.jsonl");
        var referencesPath = Path.Combine(tempOutputDir, "references.jsonl");
        var textsPath = Path.Combine(tempOutputDir, "texts.jsonl");
        var failuresPath = Path.Combine(tempOutputDir, "failures.jsonl");
        var manifestPath = Path.Combine(tempOutputDir, "manifest.json");
        var xmlFiles = Directory.EnumerateFiles(xmlDir, "*.xml")
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToList();

        LogParseStarted(Logger, input.DocumentType!, input.Year!, xmlFiles.Count, xmlDir);

        var parsedCount = 0;
        var failedCount = 0;
        var totalUnits = 0;
        var totalTextUnits = 0;
        var totalReferences = 0;
        var totalSourcePositions = 0;
        var typeCounts = new Dictionary<string, int>(StringComparer.Ordinal);

        await using (var documents = CreateWriter(documentsPath))
        await using (var units = CreateWriter(unitsPath))
        await using (var sourcePositions = CreateWriter(sourcePositionsPath))
        await using (var references = CreateWriter(referencesPath))
        await using (var texts = CreateWriter(textsPath))
        await using (var failures = CreateWriter(failuresPath))
        {
            for (var index = 0; index < xmlFiles.Count; index += 1)
            {
                var xmlPath = xmlFiles[index];
                var fileName = Path.GetFileName(xmlPath);
                var stem = Path.GetFileNameWithoutExtension(xmlPath);

                LogParsingDocument(Logger, index + 1, xmlFiles.Count, fileName);

                try
                {
                    var metadata = await ReadMetadataAsync(metadataDir, stem);
                    var eliMetadata = await ReadJsonElementOrNullAsync(jsonLdDir, stem);
                    var xmlBytes = await File.ReadAllBytesAsync(xmlPath);
                    var parsed = ParseXml(
                        xmlBytes,
                        input.DocumentType!,
                        input.Year!,
                        fileName,
                        metadata,
                        eliMetadata
                    );

                    await WriteJsonLineAsync(
                        documents,
                        parsed.Document,
                        RetsinformationXmlParserJsonContext.Default.DocumentRecord
                    );
                    await WriteJsonLineAsync(
                        texts,
                        parsed.Text,
                        RetsinformationXmlParserJsonContext.Default.TextRecord
                    );

                    foreach (var unit in parsed.Units)
                    {
                        await WriteJsonLineAsync(
                            units,
                            unit,
                            RetsinformationXmlParserJsonContext.Default.UnitRecord
                        );
                        typeCounts[unit.ProvisionType] = typeCounts.GetValueOrDefault(unit.ProvisionType) + 1;
                    }

                    foreach (var position in parsed.SourcePositions)
                    {
                        await WriteJsonLineAsync(
                            sourcePositions,
                            position,
                            RetsinformationXmlParserJsonContext.Default.SourcePositionRecord
                        );
                    }

                    foreach (var reference in parsed.References)
                    {
                        await WriteJsonLineAsync(
                            references,
                            reference,
                            RetsinformationXmlParserJsonContext.Default.SourceReferenceRecord
                        );
                    }

                    parsedCount += 1;
                    totalUnits += parsed.Units.Count;
                    totalTextUnits += parsed.Units.Count(unit => !string.IsNullOrWhiteSpace(unit.Text));
                    totalReferences += parsed.References.Count;
                    totalSourcePositions += parsed.SourcePositions.Count;
                }
                catch (Exception error)
                {
                    failedCount += 1;
                    LogDocumentFailed(Logger, fileName, error.Message);
                    await WriteJsonLineAsync(
                        failures,
                        new FailureRecord(fileName, xmlPath, error.Message),
                        RetsinformationXmlParserJsonContext.Default.FailureRecord
                    );
                }
            }
        }

        var output = new ToolOutput(
            input.DocumentType!,
            input.Year!,
            inputDir,
            outputDir,
            xmlDir,
            xmlFiles.Count,
            parsedCount,
            failedCount,
            totalUnits,
            totalTextUnits,
            totalSourcePositions,
            totalReferences,
            typeCounts,
            documentsPath,
            unitsPath,
            sourcePositionsPath,
            referencesPath,
            textsPath,
            failuresPath,
            manifestPath
        );

        await File.WriteAllTextAsync(
            manifestPath,
            JsonSerializer.Serialize(
                output,
                RetsinformationXmlParserJsonContext.Default.ToolOutput
            ),
            new UTF8Encoding(false)
        );

        if (Directory.Exists(outputDir))
        {
            Directory.Delete(outputDir, true);
        }

        Directory.Move(tempOutputDir, outputDir);

        output = output with
        {
            DocumentsPath = Path.Combine(outputDir, "documents.jsonl"),
            UnitsPath = Path.Combine(outputDir, "units.jsonl"),
            SourcePositionsPath = Path.Combine(outputDir, "source_positions.jsonl"),
            ReferencesPath = Path.Combine(outputDir, "references.jsonl"),
            TextsPath = Path.Combine(outputDir, "texts.jsonl"),
            FailuresPath = Path.Combine(outputDir, "failures.jsonl"),
            ManifestPath = Path.Combine(outputDir, "manifest.json"),
        };

        await File.WriteAllTextAsync(
            output.ManifestPath,
            JsonSerializer.Serialize(
                output,
                RetsinformationXmlParserJsonContext.Default.ToolOutput
            ),
            new UTF8Encoding(false)
        );

        LogParseFinished(Logger, parsedCount, xmlFiles.Count, failedCount, totalUnits);
        return output;
    }

    private static ParsedXml ParseXml(
        byte[] xmlBytes,
        string partitionDocumentType,
        string partitionYear,
        string fileName,
        RawDocumentMetadata? metadata,
        JsonElement? eliMetadata
    )
    {
        var xml = LoadXml(xmlBytes);
        var root = xml.Root ?? throw new InvalidOperationException("XML document has no root element.");
        var document = BuildDocument(root, partitionDocumentType, partitionYear, fileName, metadata);
        var specialist = SelectSpecialist(document.DocumentType, document.PubMedia);
        var provisions = BuildProvisions(root, document, specialist);

        if (provisions.Count == 0)
        {
            var text = Text(root);
            if (!string.IsNullOrWhiteSpace(text))
            {
                provisions.Add(
                    new Provision(
                        "indhold",
                        null,
                        "indhold",
                        null,
                        "Indhold",
                        document.Title,
                        text,
                        "/indhold",
                        "/content",
                        null,
                        1,
                        1,
                        specialist.Key,
                        new List<string> { "indhold" }
                    )
                );
            }
        }

        var extractedText = ExtractedText(provisions);
        if (string.IsNullOrWhiteSpace(extractedText))
        {
            extractedText = document.Title ?? string.Empty;
        }

        var textSha256 = Sha256Text(extractedText);
        var sourcePositions = SourcePositions(document.PayloadId, provisions, extractedText);
        var positionByUnitId = sourcePositions.ToDictionary(
            position => position.UnitId,
            position => position.PositionId,
            StringComparer.Ordinal
        );
        var units = provisions.Select(provision => ToUnit(document.PayloadId, provision, specialist, positionByUnitId))
            .ToList();
        var references = SourceReferenceEvidence(root, document.PayloadId)
            .Concat(LegalReferenceEvidence(extractedText, document.PayloadId))
            .ToList();
        var byType = units.GroupBy(unit => unit.ProvisionType, StringComparer.Ordinal)
            .OrderBy(group => group.Key, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);
        var stats = new ParseStats(
            units.Count,
            units.Count(unit => !string.IsNullOrWhiteSpace(unit.Text)),
            units.Count == 1 && units.All(unit => string.IsNullOrWhiteSpace(unit.Text)),
            byType
        );

        return new ParsedXml(
            document with
            {
                ParserSpecialistKey = specialist.Key,
                ParserSpecialistFamily = specialist.Family,
                ParserChunkingProfile = specialist.ChunkingProfile,
                TextSha256 = textSha256,
                Stats = stats,
                EliMetadata = eliMetadata,
            },
            units,
            sourcePositions,
            references,
            new TextRecord(document.PayloadId, document.EliUri, extractedText, textSha256)
        );
    }

    private static List<Provision> BuildProvisions(
        XElement root,
        DocumentRecord document,
        Specialist specialist
    )
    {
        var provisions = new List<Provision>();
        var usedIds = new HashSet<string>(StringComparer.Ordinal);
        var order = 0;

        Provision AddProvision(
            Provision? parent,
            string provisionType,
            string? number,
            string? label,
            string? heading,
            string? text,
            string? eliFragment,
            List<string>? tokens = null,
            string? canonicalPath = null
        )
        {
            order += 1;
            tokens ??= MakeTokens(parent, provisionType, number ?? label ?? order.ToString(), usedIds);
            var id = string.Join("-", tokens);
            usedIds.Add(id);
            var path = "/" + string.Join("/", tokens);
            var provision = new Provision(
                id,
                parent?.ProvisionId,
                provisionType,
                number,
                label,
                heading,
                text,
                path,
                canonicalPath ?? path,
                eliFragment,
                order,
                tokens.Count,
                specialist.Key,
                tokens
            );
            provisions.Add(provision);
            return provision;
        }

        var documentRoot = AddProvision(
            null,
            "dokument",
            null,
            "Dokument",
            document.Title,
            null,
            null,
            new List<string> { "dokument" },
            "/"
        );

        void Walk(XElement parent, Provision currentParent)
        {
            foreach (var child in parent.Elements())
            {
                var tag = child.Name.LocalName;

                if (tag == "Kapitel")
                {
                    var number = NumberFromExplicitOrLocal(child)
                        ?? (provisions.Count(item => item.ProvisionType == "kapitel") + 1).ToString();
                    var chapter = AddProvision(
                        currentParent,
                        "kapitel",
                        number,
                        $"Kapitel {number}",
                        Text(FindDirect(child, "Rubrica")),
                        null,
                        null
                    );
                    Walk(child, chapter);
                }
                else if (ParagraphTags.Contains(tag))
                {
                    AddParagraph(child, currentParent);
                }
                else if (TextGroupTags.Contains(tag))
                {
                    AddTextGroup(child, currentParent);
                }
                else if (ContainerTags.Contains(tag))
                {
                    var container = AddContainer(child, currentParent);
                    Walk(child, container ?? currentParent);
                }
                else if (child.HasElements)
                {
                    Walk(child, currentParent);
                }
            }
        }

        Provision? AddContainer(XElement element, Provision parent)
        {
            var tag = element.Name.LocalName;
            var provisionType = tag switch
            {
                "ParagrafGruppe" => "paragrafgruppe",
                "Afsnit" => "afsnit",
                "Bog" => "bog",
                "Bilag" => "bilag",
                "BilagIndhold" => "bilagindhold",
                _ => "container",
            };
            var heading = Text(FindDirect(element, "Rubrica", "Overskrift", "Titel"));
            var explicitText = Text(FindDirect(element, "Explicatus"));
            var localId = OptionalText(AttributeValue(element, "localId"));
            var directText = DirectTextBlockText(element);

            if (string.IsNullOrWhiteSpace(heading)
                && string.IsNullOrWhiteSpace(explicitText)
                && string.IsNullOrWhiteSpace(localId)
                && string.IsNullOrWhiteSpace(directText))
            {
                return null;
            }

            var number = localId
                ?? explicitText
                ?? (provisions.Count(item => item.ProvisionType == provisionType) + 1).ToString();
            return AddProvision(
                parent,
                provisionType,
                number,
                heading ?? explicitText ?? $"{tag} {number}",
                heading ?? explicitText,
                directText,
                null
            );
        }

        void AddTextGroup(XElement element, Provision parent)
        {
            var number = OptionalText(AttributeValue(element, "localId"))
                ?? (provisions.Count(item => item.ProvisionType == "tekstgruppe") + 1).ToString();
            var heading = Text(FindDirect(element, "Rubrica", "Overskrift"));
            var group = AddProvision(
                parent,
                "tekstgruppe",
                number,
                heading ?? $"Tekstgruppe {number}",
                heading,
                DirectTextBlockText(element),
                null
            );

            foreach (var child in element.Elements())
            {
                var tag = child.Name.LocalName;
                if (TextGroupTags.Contains(tag))
                {
                    AddTextGroup(child, group);
                }
                else if (ParagraphTags.Contains(tag))
                {
                    AddParagraph(child, group);
                }
                else if (child.HasElements && tag != "Exitus")
                {
                    Walk(child, group);
                }
            }
        }

        void AddParagraph(XElement element, Provision parent)
        {
            var number = ParagraphNumber(element);
            if (string.IsNullOrWhiteSpace(number))
            {
                return;
            }

            var paragraph = AddProvision(
                parent,
                "paragraf",
                number,
                $"§ {DisplayParagraphNumber(number)}",
                Text(FindDirect(element, "Rubrica")),
                null,
                $"/par/{number}"
            );

            var subsectionIndex = 0;
            var directText = DirectExitusText(element);
            if (!string.IsNullOrWhiteSpace(directText))
            {
                subsectionIndex += 1;
                AddSubsection(paragraph, number, subsectionIndex.ToString(), directText);
            }

            foreach (var child in element.Elements())
            {
                var tag = child.Name.LocalName;
                if (tag == "Stk")
                {
                    subsectionIndex += 1;
                    var subsectionNumber = StkNumber(child) ?? subsectionIndex.ToString();
                    var subsection = AddSubsection(
                        paragraph,
                        number,
                        subsectionNumber,
                        DirectExitusText(child) ?? string.Empty
                    );
                    foreach (var indentatio in child.Elements()
                        .Where(element => element.Name.LocalName == "Exitus")
                        .Elements()
                        .Where(element => element.Name.LocalName == "Index")
                        .Elements()
                        .Where(element => element.Name.LocalName == "Indentatio"))
                    {
                        AddIndentatio(indentatio, subsection, number, subsectionNumber, null);
                    }
                }
                else if (tag == "AendringsNummer")
                {
                    AddAmendmentNumber(child, paragraph, number);
                }
                else if (tag == "AendringsForslag")
                {
                    foreach (var amendmentNumber in child.Elements()
                        .Where(element => element.Name.LocalName == "AendringsNummer"))
                    {
                        AddAmendmentNumber(amendmentNumber, paragraph, number);
                    }
                }
            }
        }

        Provision AddSubsection(
            Provision paragraph,
            string paragraphNumber,
            string subsectionNumber,
            string text
        ) => AddProvision(
            paragraph,
            "stykke",
            subsectionNumber,
            $"Stk. {subsectionNumber}.",
            null,
            text,
            $"/par/{paragraphNumber}/stk/{subsectionNumber}"
        );

        void AddAmendmentNumber(XElement element, Provision parent, string? paragraphNumber)
        {
            var fallback = (provisions.Count(item => item.ParentProvisionId == parent.ProvisionId
                && item.ProvisionType == "nummer") + 1).ToString();
            var number = NumberFromExplicitOrLocal(element) ?? fallback;
            AddProvision(
                parent,
                "nummer",
                number,
                $"{number}.",
                null,
                Text(element),
                paragraphNumber is null ? null : $"/par/{paragraphNumber}/nr/{number}"
            );
        }

        void AddIndentatio(
            XElement element,
            Provision parent,
            string paragraphNumber,
            string subsectionNumber,
            string? currentNumber
        )
        {
            var number = IndentatioValue(element);
            if (string.IsNullOrWhiteSpace(number))
            {
                return;
            }

            var forma = AttributeValue(element, "formaInd") ?? string.Empty;
            var isLitra = forma == "Litra" || (forma.Length == 0 && LitraRegex().IsMatch(number));
            var provisionType = isLitra ? "litra" : "nummer";
            var eliFragment = isLitra
                ? LitraEliFragment(paragraphNumber, subsectionNumber, currentNumber, number)
                : $"/par/{paragraphNumber}/stk/{subsectionNumber}/nr/{number}";
            var provision = AddProvision(
                parent,
                provisionType,
                number,
                $"{number})",
                null,
                DirectExitusText(element),
                eliFragment
            );

            foreach (var child in element.Elements()
                .Where(element => element.Name.LocalName == "Index")
                .Elements()
                .Where(element => element.Name.LocalName == "Indentatio"))
            {
                AddIndentatio(
                    child,
                    provision,
                    paragraphNumber,
                    subsectionNumber,
                    isLitra ? currentNumber : number
                );
            }
        }

        var contentRoots = ContentRoots(root).ToList();
        foreach (var contentRoot in contentRoots)
        {
            Walk(contentRoot, documentRoot);
        }

        if (provisions.Count == 1 && contentRoots.All(IsMetadataOnlyContent))
        {
            return provisions;
        }

        if (provisions.Count == 1)
        {
            var text = string.Join("\n\n", contentRoots.Select(Text).Where(value => !string.IsNullOrWhiteSpace(value)));
            if (!string.IsNullOrWhiteSpace(text))
            {
                AddProvision(
                    documentRoot,
                    "indhold",
                    null,
                    "Indhold",
                    document.Title,
                    text,
                    null,
                    null,
                    "/content"
                );
            }
        }

        return provisions;
    }

    private static DocumentRecord BuildDocument(
        XElement root,
        string partitionDocumentType,
        string partitionYear,
        string fileName,
        RawDocumentMetadata? metadata
    )
    {
        var source = metadata?.Source;
        var documentType = Text(Find(root, "DocumentType", "DokumentType")) ?? string.Empty;
        var title = Text(Find(root, "DocumentTitle", "LovTitel", "BekTitel")) ?? string.Empty;
        var popularTitle = Text(Find(root, "PopularTitle", "PopulaerTitel"));
        var accessionNumber = Text(Find(root, "AccessionNumber")) ?? string.Empty;
        var year = ParseInt(partitionYear) ?? ParseInt(Text(Find(root, "Year", "Aar")));
        var number = ParseInt(source?.Number) ?? ParseInt(Text(Find(root, "Number", "Nummer")));
        var pubMedia = source?.EliType ?? partitionDocumentType;
        var eliUri = source?.EliUri
            ?? (year is null || number is null ? string.Empty : $"/eli/{pubMedia}/{year}/{number}");
        var sourceUrl = source?.SourceUrl
            ?? (eliUri.StartsWith("/", StringComparison.Ordinal) ? RetsinformationBaseUrl + eliUri : eliUri);
        var xmlUrl = source?.XmlUrl
            ?? (string.IsNullOrWhiteSpace(sourceUrl) ? fileName : sourceUrl.TrimEnd('/') + "/xml");
        var payloadId = metadata?.Sha256 ?? Path.GetFileNameWithoutExtension(fileName);

        return new DocumentRecord(
            payloadId,
            fileName,
            eliUri,
            sourceUrl,
            xmlUrl,
            accessionNumber,
            documentType,
            title,
            popularTitle ?? title,
            popularTitle,
            year,
            number,
            pubMedia,
            Text(Find(root, "DocumentId", "DocumentID")),
            Text(Find(root, "UniqueDocumentId", "UniqueDocumentID")),
            Text(Find(root, "AdministrativeAuthority", "AdministrativMyndighed", "Myndighed")),
            Text(Find(root, "Ministry", "Ressort", "Ministerium")),
            Text(Find(root, "AnnouncedIn", "KundgoerelsesPublikation")),
            Text(Find(root, "DiesSigni", "DatoUnderskrevet")),
            Text(Find(root, "StartDate", "DatoIkraft")),
            Text(Find(root, "EndDate", "DatoOphoer")),
            Text(Find(root, "DiesEdicti", "DatoKundgoerelse")),
            Text(Find(root, "Status")),
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    private static Specialist SelectSpecialist(string? documentType, string? pubMedia)
    {
        var normalized = NormalizeDocumentType(documentType);
        var media = pubMedia ?? string.Empty;

        if (media == "ft" || StartsWithAny(normalized, "FT", "LFS", "BESL", "FREMS"))
        {
            return new Specialist(
                "retsinformation_preparatory_material",
                "preparatory_work",
                "text_group_hierarchy"
            );
        }

        if (media == "fob" || StartsWithAny(normalized, "FOU", "AFG", "KEN", "UDT"))
        {
            return new Specialist(
                "retsinformation_administrative_decision",
                "administrative_decision",
                "decision_text_hierarchy"
            );
        }

        if (media == "retsinfo" || StartsWithAny(normalized, "CIR", "CIS", "VEJ", "SKR"))
        {
            return new Specialist(
                "retsinformation_guidance",
                "guidance",
                "guidance_text_hierarchy"
            );
        }

        if (LawPubMedia.Contains(media) || StartsWithAny(normalized, "LOV", "LBK", "BEK"))
        {
            return new Specialist(
                "retsinformation_law_structured",
                "legislation",
                "section_hierarchy"
            );
        }

        return new Specialist("retsinformation_generic", "other", "fallback_text_hierarchy");
    }

    private static UnitRecord ToUnit(
        string payloadId,
        Provision provision,
        Specialist specialist,
        Dictionary<string, string> positionByUnitId
    ) => new(
        payloadId,
        provision.ProvisionId,
        provision.ParentProvisionId,
        provision.ProvisionType,
        UnitTypeFor(provision, specialist),
        provision.Label,
        provision.Number,
        provision.Heading,
        provision.Text,
        provision.SourcePath,
        provision.CanonicalPath,
        provision.EliFragment,
        provision.SortOrder,
        provision.Depth,
        specialist.Key,
        positionByUnitId.GetValueOrDefault(provision.ProvisionId)
    );

    private static string UnitTypeFor(Provision provision, Specialist specialist)
    {
        if (provision.ProvisionType == "dokument")
        {
            return specialist.Family == "legislation" ? "act" : "work";
        }

        if (provision.ProvisionType == "tekstgruppe")
        {
            return specialist.Family switch
            {
                "guidance" => "guidance_section",
                "legislation" or "preparatory_work" => "paragraph",
                _ => "paragraph",
            };
        }

        return provision.ProvisionType switch
        {
            "bog" => "book",
            "afsnit" => "section_group",
            "paragrafgruppe" => "section_group",
            "bilag" => "annex",
            "bilagindhold" => "section_group",
            "kapitel" => "chapter",
            "paragraf" => "section",
            "stykke" => "subsection",
            "nummer" => "item",
            "litra" => "litra",
            "tekstafsnit" => "paragraph",
            _ => "other",
        };
    }

    private static List<SourcePositionRecord> SourcePositions(
        string payloadId,
        List<Provision> provisions,
        string extractedText
    )
    {
        var positions = new List<SourcePositionRecord>();
        var offset = 0;

        foreach (var provision in provisions)
        {
            if (string.IsNullOrWhiteSpace(provision.Text))
            {
                continue;
            }

            var start = extractedText.IndexOf(provision.Text, offset, StringComparison.Ordinal);
            if (start < 0)
            {
                start = offset;
            }

            var end = start + provision.Text.Length;
            positions.Add(
                new SourcePositionRecord(
                    $"position-{provision.ProvisionId}",
                    payloadId,
                    $"xml:{provision.SourcePath}",
                    start,
                    end,
                    provision.SourcePath,
                    provision.ProvisionId
                )
            );
            offset = end;
        }

        return positions;
    }

    private static List<SourceReferenceRecord> SourceReferenceEvidence(XElement root, string payloadId)
    {
        var references = new List<SourceReferenceRecord>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var element in root.DescendantsAndSelf())
        {
            var tag = element.Name.LocalName;
            var accession = OptionalText(
                AttributeValue(element, "accn")
                ?? AttributeValue(element, "AccessionNumber")
                ?? AttributeValue(element, "accession")
            );

            if ((tag == "Ref_Accn" || tag == "RefAccn") && accession is null)
            {
                accession = Text(element);
            }

            if ((tag == "Ref_Accn" || tag == "RefAccn") && accession is not null)
            {
                var key = accession + "|";
                if (seen.Add(key))
                {
                    references.Add(
                        new SourceReferenceRecord(
                            payloadId,
                            Text(element) ?? accession,
                            accession,
                            "identifier",
                            "retsinformation_accession_number",
                            accession,
                            null,
                            0.95
                        )
                    );
                }
            }
        }

        foreach (var sourceText in root.DescendantsAndSelf()
            .SelectMany(element => element.Attributes().Select(attribute => attribute.Value).Append(Text(element) ?? string.Empty)))
        {
            foreach (Match match in LegacyAccessionRegex().Matches(sourceText))
            {
                var accession = match.Groups["accession"].Value;
                var anchor = match.Groups["anchor"].Success ? match.Groups["anchor"].Value : null;
                var key = accession + "|" + anchor;
                if (!seen.Add(key))
                {
                    continue;
                }

                references.Add(
                    new SourceReferenceRecord(
                        payloadId,
                        match.Value,
                        accession,
                        "source_uri",
                        "retsinformation_accession_number",
                        accession,
                        anchor,
                        0.95
                    )
                );
            }
        }

        return references;
    }

    private static List<SourceReferenceRecord> LegalReferenceEvidence(string text, string payloadId)
    {
        var references = new List<SourceReferenceRecord>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (Match match in LegalSectionReferenceRegex().Matches(text))
        {
            var rawText = WhitespaceRegex().Replace(match.Value, " ").Trim();
            if (!seen.Add(rawText))
            {
                continue;
            }

            references.Add(
                new SourceReferenceRecord(
                    payloadId,
                    rawText,
                    rawText,
                    "legal_citation",
                    "danish_legal_section_reference",
                    rawText,
                    null,
                    0.75
                )
            );
        }

        return references;
    }

    private static IEnumerable<XElement> ContentRoots(XElement root)
    {
        var roots = root.Elements().Where(element => ContentRootTags.Contains(element.Name.LocalName)).ToList();
        if (roots.Count == 0)
        {
            var found = root.Descendants().FirstOrDefault(element => ContentRootTags.Contains(element.Name.LocalName));
            roots = found is null ? new List<XElement> { root } : new List<XElement> { found };
        }

        return roots;
    }

    private static List<string> MakeTokens(
        Provision? parent,
        string provisionType,
        string value,
        HashSet<string> usedIds
    )
    {
        var prefix = provisionType switch
        {
            "paragraf" => "paragraf",
            "stykke" => "stk",
            "nummer" => "nummer",
            "litra" => "litra",
            _ => StableSlug(provisionType),
        };
        var token = $"{prefix}-{StableSlug(value)}";
        var tokens = parent is null ? new List<string> { token } : new List<string>(parent.PathTokens) { token };
        var id = string.Join("-", tokens);
        if (!usedIds.Contains(id))
        {
            return tokens;
        }

        for (var suffix = 2; ; suffix += 1)
        {
            var deduped = parent is null
                ? new List<string> { $"{token}-{suffix}" }
                : new List<string>(parent.PathTokens) { $"{token}-{suffix}" };
            if (!usedIds.Contains(string.Join("-", deduped)))
            {
                return deduped;
            }
        }
    }

    private static string ExtractedText(List<Provision> provisions) => string.Join(
        "\n\n",
        provisions.Select(provision => provision.Text).Where(value => !string.IsNullOrWhiteSpace(value))
    );

    private static XElement? Find(XElement root, params string[] names)
    {
        var wanted = names.ToHashSet(StringComparer.Ordinal);
        return root.DescendantsAndSelf().FirstOrDefault(element => wanted.Contains(element.Name.LocalName));
    }

    private static XElement? FindDirect(XElement root, params string[] names)
    {
        var wanted = names.ToHashSet(StringComparer.Ordinal);
        return root.Elements().FirstOrDefault(element => wanted.Contains(element.Name.LocalName));
    }

    private static string? Text(XElement? element)
    {
        if (element is null)
        {
            return null;
        }

        return OptionalText(WhitespaceRegex().Replace(element.Value, " "));
    }

    private static string? DirectExitusText(XElement element)
    {
        var text = string.Join(
            " ",
            element.Elements()
                .Where(child => child.Name.LocalName == "Exitus")
                .Select(TextExcludingIndex)
                .Where(value => !string.IsNullOrWhiteSpace(value))
        );
        return OptionalText(text);
    }

    private static string? DirectTextBlockText(XElement element)
    {
        var text = string.Join(
            " ",
            element.Elements()
                .Where(child => child.Name.LocalName is "Exitus" or "Punktum" or "Linea")
                .Select(Text)
                .Where(value => !string.IsNullOrWhiteSpace(value))
        );
        return OptionalText(text);
    }

    private static string? TextExcludingIndex(XElement element)
    {
        var parts = new List<string>();

        void Collect(XNode node)
        {
            if (node is XText text)
            {
                parts.Add(text.Value);
                return;
            }

            if (node is XElement child)
            {
                if (child.Name.LocalName == "Index")
                {
                    return;
                }

                foreach (var childNode in child.Nodes())
                {
                    Collect(childNode);
                }
            }
        }

        foreach (var node in element.Nodes())
        {
            Collect(node);
        }

        return OptionalText(WhitespaceRegex().Replace(string.Join(" ", parts), " "));
    }

    private static string? ParagraphNumber(XElement element)
    {
        var localId = OptionalText(AttributeValue(element, "localId"));
        if (localId is not null && ParagraphNumberRegex().IsMatch(localId))
        {
            return NormalizeParagraphNumber(localId);
        }

        var explicitText = Text(FindDirect(element, "Explicatus"));
        if (explicitText is not null)
        {
            var match = ExplicitParagraphNumberRegex().Match(explicitText);
            if (match.Success)
            {
                return NormalizeParagraphNumber(match.Groups["number"].Value);
            }
        }

        return localId;
    }

    private static string? StkNumber(XElement element)
    {
        var explicitText = Text(FindDirect(element, "Explicatus"));
        if (explicitText is not null)
        {
            var match = StkNumberRegex().Match(explicitText);
            if (match.Success)
            {
                return match.Groups["number"].Value;
            }
        }

        return OptionalText(AttributeValue(element, "localId"));
    }

    private static string? NumberFromExplicitOrLocal(XElement element)
    {
        var explicitText = Text(FindDirect(element, "Explicatus"));
        if (explicitText is not null)
        {
            var match = LooseNumberRegex().Match(explicitText);
            if (match.Success)
            {
                return NormalizeParagraphNumber(match.Value);
            }
        }

        var localId = OptionalText(AttributeValue(element, "localId"));
        return localId is not null && ParagraphNumberRegex().IsMatch(localId)
            ? NormalizeParagraphNumber(localId)
            : localId;
    }

    private static string? IndentatioValue(XElement element)
    {
        var explicitText = Text(FindDirect(element, "Explicatus"));
        if (explicitText is not null)
        {
            var text = LeadingNumberMarkerRegex().Replace(explicitText.Trim(), "");
            var match = IndentatioValueRegex().Match(text);
            if (match.Success)
            {
                return NormalizeIndentatioValue(match.Value);
            }
        }

        var localId = OptionalText(AttributeValue(element, "localId"));
        return localId is null ? null : NormalizeIndentatioValue(localId);
    }

    private static string NormalizeParagraphNumber(string value) => WhitespaceRegex()
        .Replace(value.Trim().TrimEnd('.'), string.Empty)
        .ToUpperInvariant();

    private static string DisplayParagraphNumber(string value) => DisplayParagraphNumberRegex()
        .Replace(value.Trim(), "${digit} ${letter}");

    private static string NormalizeIndentatioValue(string value) => WhitespaceRegex()
        .Replace(value.Trim().TrimEnd('.'), string.Empty);

    private static string LitraEliFragment(
        string paragraphNumber,
        string subsectionNumber,
        string? nummer,
        string litra
    )
    {
        var value = $"/par/{paragraphNumber}/stk/{subsectionNumber}";
        if (!string.IsNullOrWhiteSpace(nummer))
        {
            value += $"/nr/{nummer}";
        }

        return value + $"/litra/{litra}";
    }

    private static bool IsMetadataOnlyContent(XElement element) => element.HasElements
        && element.Elements().All(child => child.Name.LocalName == "Meta");

    private static string? AttributeValue(XElement element, string name) => element.Attributes()
        .FirstOrDefault(attribute => attribute.Name.LocalName == name)
        ?.Value;

    private static string? OptionalText(string? value)
    {
        if (value is null)
        {
            return null;
        }

        var text = value.Trim();
        return text.Length == 0 ? null : text;
    }

    private static int? ParseInt(string? value) => int.TryParse(value, out var number) ? number : null;

    private static string NormalizeDocumentType(string? value) => (value ?? string.Empty)
        .ToUpperInvariant()
        .Replace('Æ', 'A')
        .Replace('Ø', 'O')
        .Replace('Å', 'A');

    private static bool StartsWithAny(string value, params string[] prefixes) => prefixes.Any(prefix => value.StartsWith(prefix, StringComparison.Ordinal));

    private static string StableSlug(string value)
    {
        var slug = SlugTrimRegex().Replace(
            SlugUnderscoreRegex().Replace(SlugSeparatorRegex().Replace(value, "-"), "-"),
            string.Empty
        ).ToLowerInvariant();
        return slug.Length == 0 ? "item" : slug;
    }

    private static string Sha256Text(string text) => Convert.ToHexString(
        SHA256.HashData(Encoding.UTF8.GetBytes(text))
    ).ToLowerInvariant();

    private static XDocument LoadXml(byte[] xmlBytes)
    {
        var settings = new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
        };
        using var stream = new MemoryStream(xmlBytes);
        using var reader = XmlReader.Create(stream, settings);
        return XDocument.Load(reader);
    }

    private static async Task<RawDocumentMetadata?> ReadMetadataAsync(string metadataDir, string stem)
    {
        var path = Path.Combine(metadataDir, stem + ".json");
        if (!File.Exists(path))
        {
            return null;
        }

        await using var stream = File.OpenRead(path);
        return await JsonSerializer.DeserializeAsync(
            stream,
            RetsinformationXmlParserJsonContext.Default.RawDocumentMetadata
        );
    }

    private static async Task<JsonElement?> ReadJsonElementOrNullAsync(string jsonLdDir, string stem)
    {
        var path = Path.Combine(jsonLdDir, stem + ".json");
        if (!File.Exists(path))
        {
            return null;
        }

        await using var stream = File.OpenRead(path);
        using var document = await JsonDocument.ParseAsync(stream);
        return document.RootElement.Clone();
    }

    private static StreamWriter CreateWriter(string path) => new(
        new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.Read),
        new UTF8Encoding(false)
    );

    private static async Task WriteJsonLineAsync<T>(
        StreamWriter writer,
        T value,
        JsonTypeInfo<T> jsonTypeInfo
    )
    {
        await writer.WriteLineAsync(JsonSerializer.Serialize(value, jsonTypeInfo));
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

        if (string.IsNullOrWhiteSpace(input.InputDir))
        {
            throw new InvalidOperationException("Input must contain inputDir.");
        }

        if (string.IsNullOrWhiteSpace(input.OutputDir))
        {
            throw new InvalidOperationException("Input must contain outputDir.");
        }
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "Parsing {DocumentCount} {DocumentType}/{Year} XML documents from {XmlDirectoryPath}."
    )]
    private static partial void LogParseStarted(
        ILogger logger,
        string documentType,
        string year,
        int documentCount,
        string xmlDirectoryPath
    );

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Parsing document {Index}/{DocumentCount}: {FileName}."
    )]
    private static partial void LogParsingDocument(
        ILogger logger,
        int index,
        int documentCount,
        string fileName
    );

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Warning,
        Message = "Failed to parse {FileName}: {ErrorMessage}."
    )]
    private static partial void LogDocumentFailed(
        ILogger logger,
        string fileName,
        string errorMessage
    );

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Information,
        Message = "Parsed {ParsedCount}/{DocumentCount} XML documents ({FailedCount} failed, {UnitCount} units)."
    )]
    private static partial void LogParseFinished(
        ILogger logger,
        int parsedCount,
        int documentCount,
        int failedCount,
        int unitCount
    );

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Error,
        Message = "XML parser failed: {ErrorMessage}."
    )]
    private static partial void LogToolFailed(
        ILogger logger,
        Exception exception,
        string errorMessage
    );

    [GeneratedRegex(@"\s+")]
    private static partial Regex WhitespaceRegex();

    [GeneratedRegex(@"[^\p{L}\p{Nd}_]+")]
    private static partial Regex SlugSeparatorRegex();

    [GeneratedRegex(@"_+")]
    private static partial Regex SlugUnderscoreRegex();

    [GeneratedRegex(@"^-+|-+$")]
    private static partial Regex SlugTrimRegex();

    [GeneratedRegex(@"^[0-9]+[A-Za-zÆØÅæøå]?(?:-[0-9]+[A-Za-zÆØÅæøå]?)?$")]
    private static partial Regex ParagraphNumberRegex();

    [GeneratedRegex(@"§+\s*(?<number>[0-9]+\s*[A-Za-zÆØÅæøå]?(?:\s*-\s*[0-9]+\s*[A-Za-zÆØÅæøå]?)?)")]
    private static partial Regex ExplicitParagraphNumberRegex();

    [GeneratedRegex(@"(?<digit>\d)(?<letter>[A-Za-zÆØÅæøå])$")]
    private static partial Regex DisplayParagraphNumberRegex();

    [GeneratedRegex(@"Stk\.\s*(?<number>[0-9]+[A-Za-zÆØÅæøå]?)", RegexOptions.IgnoreCase)]
    private static partial Regex StkNumberRegex();

    [GeneratedRegex(@"[0-9]+\s*[A-Za-zÆØÅæøå]?(?:\s*-\s*[0-9]+\s*[A-Za-zÆØÅæøå]?)?")]
    private static partial Regex LooseNumberRegex();

    [GeneratedRegex(@"^nr\.?\s*", RegexOptions.IgnoreCase)]
    private static partial Regex LeadingNumberMarkerRegex();

    [GeneratedRegex(@"[0-9]+\s*[A-Za-zÆØÅæøå]?|[A-Za-zÆØÅæøå]")]
    private static partial Regex IndentatioValueRegex();

    [GeneratedRegex(@"^[a-zæøå]$", RegexOptions.IgnoreCase)]
    private static partial Regex LitraRegex();

    [GeneratedRegex(@"_GETDOC_/ACCN/(?<accession>[A-Za-z0-9]+?)(?:_(?<anchor>P[0-9]+))?\b", RegexOptions.IgnoreCase)]
    private static partial Regex LegacyAccessionRegex();

    [GeneratedRegex(@"§{1,2}\s*(?<section>[0-9]+(?:\s*[A-Za-zÆØÅæøå])?(?:\s*-\s*[0-9]+(?:\s*[A-Za-zÆØÅæøå])?)?)(?:\s*,\s*stk\.\s*(?<subsection>[0-9]+[A-Za-zÆØÅæøå]?))?(?:\s*,\s*nr\.\s*(?<number>[0-9]+[A-Za-zÆØÅæøå]?))?", RegexOptions.IgnoreCase)]
    private static partial Regex LegalSectionReferenceRegex();
}

internal sealed record ToolInput(
    [property: JsonPropertyName("documentType")] string? DocumentType,
    [property: JsonPropertyName("year")] string? Year,
    [property: JsonPropertyName("inputDir")] string? InputDir,
    [property: JsonPropertyName("outputDir")] string? OutputDir
);

internal sealed record ToolOutput(
    string DocumentType,
    string Year,
    string InputDir,
    string OutputDir,
    string XmlDirectoryPath,
    int DocumentCount,
    int ParsedCount,
    int FailedCount,
    int UnitCount,
    int TextUnitCount,
    int SourcePositionCount,
    int ReferenceCount,
    Dictionary<string, int> TypeCounts,
    string DocumentsPath,
    string UnitsPath,
    string SourcePositionsPath,
    string ReferencesPath,
    string TextsPath,
    string FailuresPath,
    string ManifestPath
);

internal sealed record RawDocumentMetadata(
    RawDocumentSource? Source,
    string? Sha256
);

internal sealed record RawDocumentSource(
    string? EliType,
    string? Year,
    string? Number,
    string? EliUri,
    string? SourceUrl,
    string? XmlUrl
);

internal sealed record DocumentRecord(
    string PayloadId,
    string FileName,
    string EliUri,
    string SourceUrl,
    string XmlUrl,
    string AccessionNumber,
    string DocumentType,
    string Title,
    string ShortTitle,
    string? PopularTitle,
    int? Year,
    int? Number,
    string? PubMedia,
    string? DocumentId,
    string? UniqueDocumentId,
    string? AdministrativeAuthority,
    string? Ressort,
    string? AnnouncedIn,
    string? Signed,
    string? Effective,
    string? EndDate,
    string? Published,
    string? Status,
    string? ParserSpecialistKey,
    string? ParserSpecialistFamily,
    string? ParserChunkingProfile,
    string? TextSha256,
    ParseStats? Stats,
    JsonElement? EliMetadata
);

internal sealed record ParseStats(
    int TotalUnits,
    int TextUnits,
    bool MetadataOnlyXml,
    Dictionary<string, int> ByType
);

internal sealed record UnitRecord(
    string PayloadId,
    string UnitId,
    string? ParentUnitId,
    string ProvisionType,
    string UnitType,
    string? Label,
    string? Number,
    string? Heading,
    string? Text,
    string SourcePath,
    string CanonicalPath,
    string? EliFragment,
    int SortOrder,
    int Depth,
    string SpecialistKey,
    string? SourcePositionId
);

internal sealed record SourcePositionRecord(
    string PositionId,
    string PayloadId,
    string SourceAnchor,
    int CharStart,
    int CharEnd,
    string SectionPath,
    string UnitId
);

internal sealed record SourceReferenceRecord(
    string PayloadId,
    string RawText,
    string? NormalizedText,
    string ReferenceKind,
    string? TargetIdentifierScheme,
    string? TargetIdentifierValue,
    string? TargetUnitAnchor,
    double Confidence
);

internal sealed record TextRecord(
    string PayloadId,
    string EliUri,
    string ExtractedText,
    string TextSha256
);

internal sealed record FailureRecord(
    string FileName,
    string XmlPath,
    string ErrorMessage
);

internal sealed record ParsedXml(
    DocumentRecord Document,
    List<UnitRecord> Units,
    List<SourcePositionRecord> SourcePositions,
    List<SourceReferenceRecord> References,
    TextRecord Text
);

internal sealed record Specialist(string Key, string Family, string ChunkingProfile);

internal sealed record Provision(
    string ProvisionId,
    string? ParentProvisionId,
    string ProvisionType,
    string? Number,
    string? Label,
    string? Heading,
    string? Text,
    string SourcePath,
    string CanonicalPath,
    string? EliFragment,
    int SortOrder,
    int Depth,
    string SpecialistKey,
    List<string> PathTokens
);

[JsonSourceGenerationOptions(
    JsonSerializerDefaults.Web,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower
)]
[JsonSerializable(typeof(ToolInput))]
[JsonSerializable(typeof(ToolOutput))]
[JsonSerializable(typeof(RawDocumentMetadata))]
[JsonSerializable(typeof(DocumentRecord))]
[JsonSerializable(typeof(UnitRecord))]
[JsonSerializable(typeof(SourcePositionRecord))]
[JsonSerializable(typeof(SourceReferenceRecord))]
[JsonSerializable(typeof(TextRecord))]
[JsonSerializable(typeof(FailureRecord))]
internal partial class RetsinformationXmlParserJsonContext : JsonSerializerContext;
