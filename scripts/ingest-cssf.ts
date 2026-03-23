#!/usr/bin/env tsx
/**
 * CSSF (Commission de Surveillance du Secteur Financier) ingestion crawler.
 *
 * Three-phase pipeline:
 *   Phase 1 (Discovery):   Crawl cssf.lu document listing pages to build an index
 *                           of circulaires, reglements, FAQs, and communiques.
 *   Phase 2 (Content):     Fetch each document page, extract metadata and inline
 *                           text or download + parse linked PDFs.
 *   Phase 3 (Enforcement): Crawl administrative sanction pages for enforcement data.
 *
 * The crawler writes directly to the SQLite database used by the MCP server.
 *
 * Usage:
 *   npx tsx scripts/ingest-cssf.ts                        # full crawl
 *   npx tsx scripts/ingest-cssf.ts --dry-run              # crawl without writing to DB
 *   npx tsx scripts/ingest-cssf.ts --resume               # skip already-ingested references
 *   npx tsx scripts/ingest-cssf.ts --force                # drop DB and re-ingest everything
 *   npx tsx scripts/ingest-cssf.ts --limit 50             # stop after 50 documents
 *   npx tsx scripts/ingest-cssf.ts --type circulaire      # only crawl circulaires
 *   npx tsx scripts/ingest-cssf.ts --lang en              # prefer English content (default: fr)
 *   npx tsx scripts/ingest-cssf.ts --max-pages 10         # limit listing pages to crawl
 *   npx tsx scripts/ingest-cssf.ts --resume --type faq    # resume FAQs only
 *
 * Environment:
 *   CSSF_DB_PATH    — SQLite database path (default: data/cssf.db)
 *   CSSF_RATE_MS    — Rate limit between requests in ms (default: 1500)
 *
 * Requirements: better-sqlite3, cheerio (install: npm i cheerio)
 */

import Database from "better-sqlite3";
import * as cheerio from "cheerio";
import type { Element } from "domhandler";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ─── Constants ──────────────────────────────────────────────────────────────

const BASE_URL = "https://www.cssf.lu";
const EN_DOC_LIST = `${BASE_URL}/en/Document/page/`;
const FR_DOC_LIST = `${BASE_URL}/fr/Document/page/`;
const EN_REG_FRAMEWORK = `${BASE_URL}/en/regulatory-framework/page/`;
const FR_REG_FRAMEWORK = `${BASE_URL}/fr/cadre-reglementaire/page/`;

const RATE_MS = parseInt(process.env["CSSF_RATE_MS"] ?? "1500", 10);
const DB_PATH = process.env["CSSF_DB_PATH"] ?? "data/cssf.db";

const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 3000;
const REQUEST_TIMEOUT_MS = 30_000;

const USER_AGENT =
  "Ansvar-CSSF-Crawler/1.0 (+https://ansvar.eu; compliance research)";

// ─── CLI argument parsing ───────────────────────────────────────────────────

interface CliOptions {
  dryRun: boolean;
  resume: boolean;
  force: boolean;
  limit: number;
  maxPages: number;
  type: DocumentType | "all";
  lang: "fr" | "en";
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  const opts: CliOptions = {
    dryRun: false,
    resume: false,
    force: false,
    limit: 0,
    maxPages: 0,
    type: "all",
    lang: "fr",
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case "--dry-run":
        opts.dryRun = true;
        break;
      case "--resume":
        opts.resume = true;
        break;
      case "--force":
        opts.force = true;
        break;
      case "--limit":
        opts.limit = parseInt(args[++i] ?? "0", 10);
        break;
      case "--max-pages":
        opts.maxPages = parseInt(args[++i] ?? "0", 10);
        break;
      case "--type":
        opts.type = (args[++i] ?? "all") as DocumentType | "all";
        break;
      case "--lang":
        opts.lang = (args[++i] ?? "fr") as "fr" | "en";
        break;
      default:
        console.error(`Unknown argument: ${arg}`);
        process.exit(1);
    }
  }

  return opts;
}

// ─── Types ──────────────────────────────────────────────────────────────────

type DocumentType =
  | "circulaire"
  | "reglement"
  | "faq"
  | "communique"
  | "regulation"
  | "directive"
  | "guideline"
  | "other";

interface DiscoveredDocument {
  url: string;
  title: string;
  date: string;
  docType: DocumentType;
  topics: string[];
  entities: string[];
}

interface ParsedProvision {
  reference: string;
  title: string;
  text: string;
  type: string;
  chapter: string | null;
  section: string | null;
  effectiveDate: string | null;
}

interface ParsedEnforcement {
  firmName: string;
  referenceNumber: string;
  actionType: string;
  amount: number;
  date: string;
  summary: string;
  sourcebookReferences: string;
}

// ─── HTTP helpers ───────────────────────────────────────────────────────────

let requestCount = 0;
let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<Response | null> {
  // Enforce rate limit
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_MS) {
    await sleep(RATE_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      requestCount++;

      const controller = new AbortController();
      const timeout = setTimeout(
        () => controller.abort(),
        REQUEST_TIMEOUT_MS,
      );

      const res = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        },
        signal: controller.signal,
      });

      clearTimeout(timeout);

      if (res.ok) {
        return res;
      }

      if (res.status === 404) {
        return null;
      }

      // Rate limited or server error — retry with backoff
      if (res.status === 429 || res.status >= 500) {
        const backoff = RETRY_BACKOFF_MS * attempt;
        console.warn(
          `  WARN: HTTP ${res.status} for ${url} — retry ${attempt}/${MAX_RETRIES} in ${backoff}ms`,
        );
        await sleep(backoff);
        continue;
      }

      console.warn(`  WARN: HTTP ${res.status} for ${url} — skipping`);
      return null;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (attempt < MAX_RETRIES) {
        const backoff = RETRY_BACKOFF_MS * attempt;
        console.warn(
          `  WARN: ${msg} for ${url} — retry ${attempt}/${MAX_RETRIES} in ${backoff}ms`,
        );
        await sleep(backoff);
      } else {
        console.error(`  ERROR: ${msg} for ${url} — giving up after ${MAX_RETRIES} attempts`);
        return null;
      }
    }
  }

  return null;
}

async function fetchPage(url: string): Promise<string | null> {
  const res = await rateLimitedFetch(url);
  if (!res) return null;
  return res.text();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── Document type classification ───────────────────────────────────────────

function classifyDocType(label: string): DocumentType {
  const lower = label.toLowerCase().trim();

  if (
    lower.includes("circular") ||
    lower.includes("circulaire")
  ) {
    return "circulaire";
  }
  if (
    lower.includes("cssf regulation") ||
    lower.includes("règlement cssf") ||
    lower.includes("reglement cssf")
  ) {
    return "reglement";
  }
  if (lower.includes("faq")) {
    return "faq";
  }
  if (
    lower.includes("communiqué") ||
    lower.includes("communique") ||
    lower.includes("press release") ||
    lower.includes("sanction") ||
    lower.includes("warning")
  ) {
    return "communique";
  }
  if (lower.includes("eu regulation") || lower.includes("règlement ue")) {
    return "regulation";
  }
  if (lower.includes("eu directive") || lower.includes("directive ue")) {
    return "directive";
  }
  if (
    lower.includes("guideline") ||
    lower.includes("orientation") ||
    lower.includes("recommendation")
  ) {
    return "guideline";
  }

  return "other";
}

function docTypeToSourcebook(dt: DocumentType): string {
  switch (dt) {
    case "circulaire":
      return "CSSF_CIRCULAIRES";
    case "reglement":
      return "CSSF_REGLEMENTS";
    case "faq":
      return "CSSF_FAQ";
    case "communique":
      return "CSSF_COMMUNIQUES";
    case "regulation":
      return "CSSF_EU_REGULATIONS";
    case "directive":
      return "CSSF_EU_DIRECTIVES";
    case "guideline":
      return "CSSF_GUIDELINES";
    default:
      return "CSSF_OTHER";
  }
}

function shouldCrawlType(dt: DocumentType, filter: DocumentType | "all"): boolean {
  if (filter === "all") return true;
  return dt === filter;
}

// ─── Date parsing ───────────────────────────────────────────────────────────

/**
 * Parse dates from CSSF pages. Handles formats:
 *   "Published on 11.12.2012"    (English listing)
 *   "Publié le 11.12.2012"       (French listing)
 *   "11 December 2012"           (English detail)
 *   "11 décembre 2012"           (French detail)
 *   "2024-01-17"                 (ISO)
 */
function parseDate(raw: string): string | null {
  if (!raw) return null;

  const cleaned = raw
    .replace(/published\s+on\s*/i, "")
    .replace(/publié\s+le\s*/i, "")
    .replace(/document\s+date:\s*/i, "")
    .replace(/last\s+updated?\s*:?\s*/i, "")
    .replace(/mise\s+à\s+jour\s*:?\s*/i, "")
    .trim();

  // DD.MM.YYYY
  const dotMatch = cleaned.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (dotMatch) {
    const [, d, m, y] = dotMatch;
    return `${y}-${m!.padStart(2, "0")}-${d!.padStart(2, "0")}`;
  }

  // ISO format
  const isoMatch = cleaned.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    return cleaned;
  }

  // DD Month YYYY (English/French)
  const MONTHS: Record<string, string> = {
    january: "01", janvier: "01",
    february: "02", février: "02", fevrier: "02",
    march: "03", mars: "03",
    april: "04", avril: "04",
    may: "05", mai: "05",
    june: "06", juin: "06",
    july: "07", juillet: "07",
    august: "08", août: "08", aout: "08",
    september: "09", septembre: "09",
    october: "10", octobre: "10",
    november: "11", novembre: "11",
    december: "12", décembre: "12", decembre: "12",
  };

  const textMatch = cleaned.match(/^(\d{1,2})\s+(\w+)\s+(\d{4})$/i);
  if (textMatch) {
    const [, d, monthName, y] = textMatch;
    const m = MONTHS[monthName!.toLowerCase()];
    if (m) {
      return `${y}-${m}-${d!.padStart(2, "0")}`;
    }
  }

  // DD/MM/YYYY
  const slashMatch = cleaned.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (slashMatch) {
    const [, d, m, y] = slashMatch;
    return `${y}-${m!.padStart(2, "0")}-${d!.padStart(2, "0")}`;
  }

  return null;
}

// ─── Phase 1: Discovery ────────────────────────────────────────────────────

async function discoverDocuments(
  opts: CliOptions,
): Promise<DiscoveredDocument[]> {
  console.log("\n=== Phase 1: Discovery ===\n");

  const discovered: DiscoveredDocument[] = [];
  const seenUrls = new Set<string>();

  // Crawl both the /en/Document/ listing and the /en/regulatory-framework/
  // listing. The Document listing has broader coverage; the regulatory
  // framework listing has better categorisation. We merge and deduplicate.
  const listingSources = opts.lang === "fr"
    ? [
        { base: FR_DOC_LIST, label: "Documents (FR)" },
        { base: FR_REG_FRAMEWORK, label: "Cadre réglementaire (FR)" },
      ]
    : [
        { base: EN_DOC_LIST, label: "Documents (EN)" },
        { base: EN_REG_FRAMEWORK, label: "Regulatory Framework (EN)" },
      ];

  for (const source of listingSources) {
    console.log(`  Crawling ${source.label}...`);

    let page = 1;
    let emptyStreak = 0;

    while (true) {
      if (opts.maxPages > 0 && page > opts.maxPages) {
        console.log(`    Reached --max-pages limit (${opts.maxPages})`);
        break;
      }

      const url = page === 1
        ? source.base.replace(/page\/$/, "")
        : `${source.base}${page}/`;

      const html = await fetchPage(url);
      if (!html) {
        emptyStreak++;
        if (emptyStreak >= 2) {
          console.log(`    No more pages after page ${page - 1}`);
          break;
        }
        page++;
        continue;
      }

      const $ = cheerio.load(html);
      let pageCount = 0;

      // Parse document entries. CSSF uses WordPress with articles or divs
      // containing the document metadata. We look for headings with links
      // to /en/Document/ or /fr/Document/ detail pages.
      $("article, .post, .document-entry, .type-document, .type-post").each(
        (_i, el) => {
          const $el = $(el);
          const link = $el.find("h2 a, h3 a, .entry-title a").first();
          let href = link.attr("href") ?? "";
          const title = link.text().trim();

          if (!href || !title) return;

          // Normalise to absolute URL
          if (href.startsWith("/")) {
            href = `${BASE_URL}${href}`;
          }

          // Only keep cssf.lu document pages
          if (
            !href.includes("/Document/") &&
            !href.includes("/document/")
          ) {
            return;
          }

          if (seenUrls.has(href)) return;
          seenUrls.add(href);

          // Extract metadata from the listing entry
          const typeLabel =
            $el.find(".document-type, .doc-type, .entry-category, .cat-links").first().text().trim() ||
            $el.find(".post-categories a, .entry-meta .category").first().text().trim() ||
            "";
          const dateText =
            $el.find(".document-date, .pub-date, .entry-date, .posted-on").first().text().trim() ||
            $el.find("time").first().attr("datetime") ||
            "";
          const topicsText =
            $el.find(".main-topics, .topics").first().text().trim();
          const entitiesText =
            $el.find(".relevant-for").first().text().trim();

          const docType = classifyDocType(typeLabel || title);
          const date = parseDate(dateText);

          const topics = topicsText
            ? topicsText
                .replace(/^(main\s+)?topics?\s*:\s*/i, "")
                .split(/[,;]/)
                .map((t) => t.trim())
                .filter(Boolean)
            : [];

          const entities = entitiesText
            ? entitiesText
                .replace(/^relevant\s+for\s*:\s*/i, "")
                .split(/[,;]/)
                .map((e) => e.trim())
                .filter(Boolean)
            : [];

          discovered.push({
            url: href,
            title,
            date: date ?? "",
            docType,
            topics,
            entities,
          });

          pageCount++;
        },
      );

      // Fallback: if no structured entries found, try generic link extraction
      if (pageCount === 0) {
        $("a[href*='/Document/']").each((_i, el) => {
          const href = $(el).attr("href") ?? "";
          const title = $(el).text().trim();
          if (!href || !title || title.length < 5) return;

          const fullUrl = href.startsWith("/") ? `${BASE_URL}${href}` : href;

          // Skip listing/category pages
          if (fullUrl.endsWith("/Document/") || fullUrl.includes("/page/")) return;

          if (seenUrls.has(fullUrl)) return;
          seenUrls.add(fullUrl);

          discovered.push({
            url: fullUrl,
            title,
            date: "",
            docType: classifyDocType(title),
            topics: [],
            entities: [],
          });
          pageCount++;
        });
      }

      if (pageCount === 0) {
        emptyStreak++;
        if (emptyStreak >= 2) {
          console.log(`    No more entries after page ${page}`);
          break;
        }
      } else {
        emptyStreak = 0;
      }

      if (page % 10 === 0 || page === 1) {
        console.log(
          `    Page ${page}: ${pageCount} documents (total: ${discovered.length})`,
        );
      }

      page++;
    }
  }

  // Filter by type if requested
  const filtered =
    opts.type === "all"
      ? discovered
      : discovered.filter((d) => d.docType === opts.type);

  console.log(
    `\n  Discovery complete: ${filtered.length} documents` +
      (opts.type !== "all" ? ` (filtered to ${opts.type})` : "") +
      ` from ${discovered.length} total`,
  );

  // Print type breakdown
  const byType = new Map<string, number>();
  for (const d of discovered) {
    byType.set(d.docType, (byType.get(d.docType) ?? 0) + 1);
  }
  for (const [type, count] of [...byType.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`    ${type}: ${count}`);
  }

  return filtered;
}

// ─── Phase 2: Content extraction ────────────────────────────────────────────

function extractReference(title: string, url: string): string {
  // Try to extract a structured reference from the title.
  //   "Circular CSSF 12/552 (as amended by ...)" → "Circulaire 12/552"
  //   "Circulaire CSSF 20/750"                    → "Circulaire 20/750"
  //   "CSSF Regulation No 12-02"                  → "Reglement CSSF N 12-02"
  //   "FAQ regarding Circular CSSF 02/77"         → "FAQ Circulaire 02/77"

  // Circular pattern
  const circMatch = title.match(
    /(?:circular|circulaire)\s+(?:cssf\s+)?(\d{2,4}[/-]\d{2,4})/i,
  );
  if (circMatch) {
    return `Circulaire ${circMatch[1]!.replace("-", "/")}`;
  }

  // Regulation pattern
  const regMatch = title.match(
    /(?:regulation|règlement|reglement)\s+(?:cssf\s+)?(?:n[°o.]?\s*)?(\d[\d-]+)/i,
  );
  if (regMatch) {
    return `Reglement CSSF N ${regMatch[1]}`;
  }

  // FAQ pattern
  const faqMatch = title.match(/faq\s+(?:regarding\s+)?(?:circular\s+cssf\s+)?(.+)/i);
  if (faqMatch) {
    const ref = faqMatch[1]!.trim().substring(0, 60);
    return `FAQ ${ref}`;
  }

  // Sanction pattern
  const sanctionMatch = title.match(
    /(?:administrative\s+)?sanction[s]?\s+(?:of\s+)?(.+)/i,
  );
  if (sanctionMatch) {
    return `Sanction ${sanctionMatch[1]!.trim().substring(0, 40)}`;
  }

  // Fallback: extract slug from URL
  const slug = url.split("/").filter(Boolean).pop() ?? title;
  return slug.replace(/-/g, " ").substring(0, 80);
}

function extractChapterSection(
  title: string,
  text: string,
): { chapter: string | null; section: string | null } {
  // Try to find chapter/section references
  const chapterMatch =
    title.match(/chapitre\s+(\d+|[IVXLC]+)/i) ??
    text.match(/chapitre\s+(\d+|[IVXLC]+)/i) ??
    title.match(/chapter\s+(\d+|[IVXLC]+)/i) ??
    text.match(/chapter\s+(\d+|[IVXLC]+)/i);

  const sectionMatch =
    title.match(/section\s+(\d+[\d.]*)/i) ??
    text.match(/section\s+(\d+[\d.]*)/i) ??
    title.match(/art(?:icle)?\.?\s+(\d+[\d.]*)/i) ??
    text.match(/art(?:icle)?\.?\s+(\d+[\d.]*)/i);

  return {
    chapter: chapterMatch ? chapterMatch[1]! : null,
    section: sectionMatch ? sectionMatch[1]! : null,
  };
}

async function fetchDocumentContent(
  doc: DiscoveredDocument,
): Promise<ParsedProvision[]> {
  const html = await fetchPage(doc.url);
  if (!html) return [];

  const $ = cheerio.load(html);
  const provisions: ParsedProvision[] = [];

  // Extract the main content area
  const contentArea = $(
    ".entry-content, .post-content, .document-content, article .content, .single-content, main article",
  ).first();

  if (contentArea.length === 0) {
    return [];
  }

  // Strategy 1: Extract inline text content (some documents have full text).
  // The CSSF site often provides content as inline HTML paragraphs plus
  // links to PDF versions.
  const inlineText = extractInlineText($, contentArea);

  // Strategy 2: Extract metadata even if the main text is in a PDF.
  // We still record the document with its metadata and any summary text
  // found on the HTML page.
  const metaTitle =
    $("h1.entry-title, h1.post-title, .document-title h1, h1").first().text().trim() ||
    doc.title;

  // Extract publication and update dates from the detail page
  let effectiveDate = doc.date;
  $("p, div, span, .meta-value, .document-date, .pub-date").each((_i, el) => {
    const text = $(el).text().trim();
    const parsed = parseDate(text);
    if (parsed && !effectiveDate) {
      effectiveDate = parsed;
    }
  });

  // Extract keywords/topics from the detail page
  const keywords: string[] = [];
  $(".keywords a, .tags a, .post-tags a, .entry-tags a").each((_i, el) => {
    const kw = $(el).text().trim();
    if (kw) keywords.push(kw);
  });

  // Look for PDF download links
  const pdfLinks: { url: string; label: string }[] = [];
  $("a[href$='.pdf'], a[href*='.pdf']").each((_i, el) => {
    const href = $(el).attr("href") ?? "";
    const label = $(el).text().trim();
    if (href && href.includes(".pdf")) {
      const fullUrl = href.startsWith("/") ? `${BASE_URL}${href}` : href;
      pdfLinks.push({ url: fullUrl, label });
    }
  });

  // Build provisions from inline content
  if (inlineText.length > 100) {
    // Split long texts into section-based provisions
    const sections = splitIntoSections(inlineText, metaTitle);

    for (const sec of sections) {
      const { chapter, section } = extractChapterSection(
        sec.heading,
        sec.text,
      );
      provisions.push({
        reference: extractReference(metaTitle, doc.url),
        title: sec.heading || metaTitle,
        text: sec.text,
        type: doc.docType,
        chapter,
        section,
        effectiveDate: effectiveDate || null,
      });
    }
  }

  // If no inline text was extracted (PDF-only), create a summary provision
  // from the page metadata so the document is at least discoverable.
  if (provisions.length === 0) {
    const summaryParts: string[] = [];

    // Grab any description/summary text on the page
    const descText = $(
      ".entry-summary, .document-description, .post-excerpt, .entry-content > p:first-of-type",
    )
      .first()
      .text()
      .trim();

    if (descText && descText.length > 20) {
      summaryParts.push(descText);
    }

    // Add keyword context
    const allKeywords = [...doc.topics, ...keywords];
    if (allKeywords.length > 0) {
      summaryParts.push(`Topics: ${allKeywords.join(", ")}`);
    }

    // Add entity scope
    if (doc.entities.length > 0) {
      summaryParts.push(`Applicable to: ${doc.entities.join(", ")}`);
    }

    // Note PDF availability
    if (pdfLinks.length > 0) {
      const pdfNote = pdfLinks
        .map((p) => `${p.label}: ${p.url}`)
        .join("; ");
      summaryParts.push(`PDF: ${pdfNote}`);
    }

    const summaryText =
      summaryParts.length > 0
        ? summaryParts.join("\n\n")
        : `${metaTitle}. Document available on cssf.lu.`;

    const { chapter, section } = extractChapterSection(
      metaTitle,
      summaryText,
    );

    provisions.push({
      reference: extractReference(metaTitle, doc.url),
      title: metaTitle,
      text: summaryText,
      type: doc.docType,
      chapter,
      section,
      effectiveDate: effectiveDate || null,
    });
  }

  return provisions;
}

function extractInlineText(
  $: cheerio.CheerioAPI,
  contentArea: cheerio.Cheerio<Element>,
): string {
  // Remove navigation, download links, and non-content elements
  const clone = contentArea.clone();
  clone.find("nav, .navigation, .download, .sharedaddy, script, style, .cookie-notice").remove();

  // Collect text from paragraphs, lists, headings, and tables
  const parts: string[] = [];

  clone.find("h2, h3, h4, h5, h6, p, li, td, blockquote").each((_i, el) => {
    const text = $(el).text().trim();
    if (text && text.length > 5) {
      parts.push(text);
    }
  });

  return parts.join("\n\n");
}

interface TextSection {
  heading: string;
  text: string;
}

function splitIntoSections(
  fullText: string,
  fallbackTitle: string,
): TextSection[] {
  // Split on heading-like patterns:
  //   "Chapter 1 — Title"
  //   "Chapitre 2. Title"
  //   "Section 3: Title"
  //   "Article 4"
  //   Numbered items like "1." or "(a)"
  const lines = fullText.split("\n\n");

  // If text is short enough, keep as single provision
  if (lines.length <= 3 || fullText.length < 2000) {
    return [{ heading: fallbackTitle, text: fullText }];
  }

  const sections: TextSection[] = [];
  let currentHeading = fallbackTitle;
  let currentParts: string[] = [];

  const headingPattern =
    /^(?:chapitre|chapter|section|art(?:icle)?\.?|titre|title|partie|part)\s+\d/i;

  for (const line of lines) {
    if (headingPattern.test(line) && currentParts.length > 0) {
      // Flush previous section
      sections.push({
        heading: currentHeading,
        text: currentParts.join("\n\n"),
      });
      currentHeading = line.substring(0, 120);
      currentParts = [line];
    } else {
      currentParts.push(line);
    }
  }

  // Flush final section
  if (currentParts.length > 0) {
    sections.push({
      heading: currentHeading,
      text: currentParts.join("\n\n"),
    });
  }

  // If splitting produced only one section, try splitting on size
  if (sections.length === 1 && fullText.length > 5000) {
    return splitBySizeLimit(fullText, fallbackTitle, 4000);
  }

  return sections;
}

function splitBySizeLimit(
  text: string,
  baseTitle: string,
  maxLen: number,
): TextSection[] {
  const paragraphs = text.split("\n\n");
  const result: TextSection[] = [];
  let current: string[] = [];
  let currentLen = 0;
  let partNum = 1;

  for (const p of paragraphs) {
    if (currentLen + p.length > maxLen && current.length > 0) {
      result.push({
        heading: `${baseTitle} (Part ${partNum})`,
        text: current.join("\n\n"),
      });
      partNum++;
      current = [p];
      currentLen = p.length;
    } else {
      current.push(p);
      currentLen += p.length;
    }
  }

  if (current.length > 0) {
    result.push({
      heading:
        result.length > 0 ? `${baseTitle} (Part ${partNum})` : baseTitle,
      text: current.join("\n\n"),
    });
  }

  return result;
}

// ─── Phase 3: Enforcement actions ───────────────────────────────────────────

async function crawlEnforcementActions(
  docs: DiscoveredDocument[],
  opts: CliOptions,
): Promise<ParsedEnforcement[]> {
  console.log("\n=== Phase 3: Enforcement Actions ===\n");

  const enforcementDocs = docs.filter(
    (d) =>
      d.title.toLowerCase().includes("sanction") ||
      d.title.toLowerCase().includes("enforcement") ||
      d.title.toLowerCase().includes("fine") ||
      d.title.toLowerCase().includes("amende"),
  );

  console.log(`  Found ${enforcementDocs.length} enforcement-related documents`);

  const actions: ParsedEnforcement[] = [];

  for (const doc of enforcementDocs) {
    const html = await fetchPage(doc.url);
    if (!html) continue;

    const $ = cheerio.load(html);
    const parsed = parseEnforcementPage($, doc);
    actions.push(...parsed);
  }

  console.log(`  Extracted ${actions.length} enforcement actions`);
  return actions;
}

function parseEnforcementPage(
  $: cheerio.CheerioAPI,
  doc: DiscoveredDocument,
): ParsedEnforcement[] {
  const actions: ParsedEnforcement[] = [];

  const content = $(
    ".entry-content, .post-content, .document-content, article .content",
  )
    .first()
    .text()
    .trim();

  if (!content || content.length < 20) {
    return [];
  }

  // Extract amount if mentioned
  const amountMatch = content.match(
    /(?:EUR|€)\s*([\d.,]+(?:\s*(?:million|mio))?\s*)/i,
  );
  let amount = 0;
  if (amountMatch) {
    const raw = amountMatch[1]!
      .replace(/\s/g, "")
      .replace(/\./g, "")
      .replace(",", ".");
    amount = parseFloat(raw) || 0;
    if (content.toLowerCase().includes("million") || content.toLowerCase().includes("mio")) {
      amount *= 1_000_000;
    }
  }

  // Determine action type
  let actionType = "unknown";
  const contentLower = content.toLowerCase();
  if (contentLower.includes("amende") || contentLower.includes("fine")) {
    actionType = "fine";
  } else if (
    contentLower.includes("avertissement") ||
    contentLower.includes("warning")
  ) {
    actionType = "warning";
  } else if (
    contentLower.includes("restriction") ||
    contentLower.includes("suspension")
  ) {
    actionType = "restriction";
  } else if (
    contentLower.includes("injonction") ||
    contentLower.includes("injunction")
  ) {
    actionType = "injunction";
  } else if (
    contentLower.includes("retrait") ||
    contentLower.includes("withdrawal")
  ) {
    actionType = "withdrawal";
  }

  // Try to extract firm name from content
  let firmName = "Entity (see document)";
  // Look for patterns like "imposed on [firm name]" or "infligée à [firm name]"
  const firmMatchEN = content.match(
    /(?:imposed\s+on|against|sanctioned)\s+([A-Z][A-Za-z\s&.,]+(?:S\.A\.|S\.à\s*r\.l\.|Ltd|GmbH|AG|SE))/,
  );
  const firmMatchFR = content.match(
    /(?:infligée?\s+à|prononcée?\s+à\s+l'encontre\s+de)\s+([A-Z][A-Za-zÀ-ÿ\s&.,]+(?:S\.A\.|S\.à\s*r\.l\.|Ltd|GmbH|AG|SE))/,
  );
  if (firmMatchEN) {
    firmName = firmMatchEN[1]!.trim();
  } else if (firmMatchFR) {
    firmName = firmMatchFR[1]!.trim();
  }

  // Extract regulation references
  const refMatches = content.match(
    /(?:circulaire|circular|règlement|regulation)\s+(?:cssf\s+)?(?:n[°o.]?\s*)?\d[\d/.-]*/gi,
  );
  const sourcebookRefs = refMatches ? [...new Set(refMatches)].join(", ") : "";

  // Generate reference number from URL or title
  const refNumber =
    doc.url.split("/").filter(Boolean).pop()?.replace(/-/g, " ") ??
    doc.title.substring(0, 40);

  actions.push({
    firmName,
    referenceNumber: `CSSF-${refNumber}`,
    actionType,
    amount,
    date: doc.date,
    summary: content.substring(0, 2000),
    sourcebookReferences: sourcebookRefs,
  });

  return actions;
}

// ─── Database operations ────────────────────────────────────────────────────

function initDb(opts: CliOptions): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  if (opts.force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`  Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  // Add progress tracking table for --resume support
  db.exec(`
    CREATE TABLE IF NOT EXISTS _ingest_progress (
      url  TEXT PRIMARY KEY,
      status TEXT DEFAULT 'done',
      crawled_at TEXT DEFAULT (datetime('now'))
    );
  `);

  return db;
}

function ensureSourcebooks(db: Database.Database): void {
  const insert = db.prepare(
    "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
  );

  const sourcebooks: Array<[string, string, string]> = [
    [
      "CSSF_CIRCULAIRES",
      "Circulaires CSSF",
      "Circulaires emises par la CSSF fixant les exigences detaillees applicables aux entites surveillees en matiere de gouvernance, risques, conformite et securite informatique.",
    ],
    [
      "CSSF_REGLEMENTS",
      "Reglements CSSF",
      "Reglements contraignants de la CSSF, notamment en matiere de lutte contre le blanchiment et le financement du terrorisme (LBC/FT), de capital et de reporting.",
    ],
    [
      "CSSF_FAQ",
      "FAQ CSSF",
      "Questions et reponses officielles publiees par la CSSF clarifiant l'interpretation et l'application des textes reglementaires.",
    ],
    [
      "CSSF_COMMUNIQUES",
      "Communiques de presse CSSF",
      "Communiques de presse et annonces de la CSSF, incluant les decisions de sanction et les mises en garde aux investisseurs.",
    ],
    [
      "CSSF_EU_REGULATIONS",
      "Reglements UE (via CSSF)",
      "Reglements de l'Union europeenne pertinents pour le secteur financier luxembourgeois, references par la CSSF.",
    ],
    [
      "CSSF_EU_DIRECTIVES",
      "Directives UE (via CSSF)",
      "Directives de l'Union europeenne transposees en droit luxembourgeois et appliquees par la CSSF.",
    ],
    [
      "CSSF_GUIDELINES",
      "Orientations et recommandations",
      "Orientations (ABE, AEAPP, AEMF) et recommandations applicables au secteur financier luxembourgeois.",
    ],
    [
      "CSSF_OTHER",
      "Autres documents CSSF",
      "Documents CSSF supplementaires: formulaires, normes techniques, agendas et autres publications.",
    ],
  ];

  for (const [id, name, desc] of sourcebooks) {
    insert.run(id, name, desc);
  }
}

function isAlreadyIngested(db: Database.Database, url: string): boolean {
  const row = db
    .prepare("SELECT 1 FROM _ingest_progress WHERE url = ?")
    .get(url) as { "1": number } | undefined;
  return row !== undefined;
}

function markIngested(db: Database.Database, url: string): void {
  db.prepare(
    "INSERT OR REPLACE INTO _ingest_progress (url, status, crawled_at) VALUES (?, 'done', datetime('now'))",
  ).run(url);
}

function insertProvisions(
  db: Database.Database,
  provisions: ParsedProvision[],
  sourcebookId: string,
): number {
  const insert = db.prepare(`
    INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
    VALUES (?, ?, ?, ?, ?, 'in_force', ?, ?, ?)
  `);

  let count = 0;
  const tx = db.transaction(() => {
    for (const p of provisions) {
      insert.run(
        sourcebookId,
        p.reference,
        p.title,
        p.text,
        p.type,
        p.effectiveDate,
        p.chapter,
        p.section,
      );
      count++;
    }
  });

  tx();
  return count;
}

function insertEnforcements(
  db: Database.Database,
  actions: ParsedEnforcement[],
): number {
  const insert = db.prepare(`
    INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  let count = 0;
  const tx = db.transaction(() => {
    for (const a of actions) {
      insert.run(
        a.firmName,
        a.referenceNumber,
        a.actionType,
        a.amount,
        a.date,
        a.summary,
        a.sourcebookReferences,
      );
      count++;
    }
  });

  tx();
  return count;
}

// ─── Main ───────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const opts = parseArgs();

  console.log("CSSF (cssf.lu) Ingestion Crawler");
  console.log("================================\n");
  console.log(`  Database:   ${DB_PATH}`);
  console.log(`  Language:   ${opts.lang}`);
  console.log(`  Rate limit: ${RATE_MS}ms`);
  console.log(`  Dry run:    ${opts.dryRun}`);
  console.log(`  Resume:     ${opts.resume}`);
  console.log(`  Force:      ${opts.force}`);
  console.log(`  Type:       ${opts.type}`);
  if (opts.limit > 0) console.log(`  Limit:      ${opts.limit} documents`);
  if (opts.maxPages > 0)
    console.log(`  Max pages:  ${opts.maxPages} listing pages`);

  // Phase 1: Discovery
  const discovered = await discoverDocuments(opts);

  if (discovered.length === 0) {
    console.log("\nNo documents discovered. Exiting.");
    return;
  }

  // Apply limit
  const toProcess =
    opts.limit > 0 ? discovered.slice(0, opts.limit) : discovered;

  // Init database (unless dry run)
  let db: Database.Database | null = null;
  if (!opts.dryRun) {
    db = initDb(opts);
    ensureSourcebooks(db);
    console.log(`\n  Database ready at ${DB_PATH}`);
  }

  // Phase 2: Content extraction
  console.log("\n=== Phase 2: Content Extraction ===\n");

  let totalProvisions = 0;
  let processed = 0;
  let skipped = 0;
  let failed = 0;

  for (let i = 0; i < toProcess.length; i++) {
    const doc = toProcess[i]!;

    // Resume support: skip already-ingested URLs
    if (opts.resume && db && isAlreadyIngested(db, doc.url)) {
      skipped++;
      continue;
    }

    // Progress logging
    if ((i + 1) % 25 === 0 || i === 0) {
      console.log(
        `  [${i + 1}/${toProcess.length}] ${doc.docType}: ${doc.title.substring(0, 70)}...`,
      );
    }

    try {
      const provisions = await fetchDocumentContent(doc);

      if (provisions.length === 0) {
        failed++;
        continue;
      }

      if (!opts.dryRun && db) {
        const sourcebookId = docTypeToSourcebook(doc.docType);
        const inserted = insertProvisions(db, provisions, sourcebookId);
        totalProvisions += inserted;
        markIngested(db, doc.url);
      } else {
        totalProvisions += provisions.length;
      }

      processed++;
    } catch (err) {
      failed++;
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  ERROR processing ${doc.url}: ${msg}`);
    }
  }

  console.log(`\n  Content extraction complete:`);
  console.log(`    Processed:  ${processed}`);
  console.log(`    Skipped:    ${skipped} (already ingested)`);
  console.log(`    Failed:     ${failed}`);
  console.log(`    Provisions: ${totalProvisions}`);

  // Phase 3: Enforcement actions
  if (opts.type === "all" || opts.type === "communique") {
    const enforcementActions = await crawlEnforcementActions(discovered, opts);

    if (enforcementActions.length > 0 && !opts.dryRun && db) {
      const inserted = insertEnforcements(db, enforcementActions);
      console.log(`  Inserted ${inserted} enforcement actions`);
    }
  }

  // Summary
  if (db) {
    const provisionCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions").get() as {
        cnt: number;
      }
    ).cnt;
    const sourcebookCount = (
      db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as {
        cnt: number;
      }
    ).cnt;
    const enforcementCount = (
      db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as {
        cnt: number;
      }
    ).cnt;
    const ftsCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as {
        cnt: number;
      }
    ).cnt;

    console.log(`\n=== Database Summary ===\n`);
    console.log(`  Sourcebooks:          ${sourcebookCount}`);
    console.log(`  Provisions:           ${provisionCount}`);
    console.log(`  Enforcement actions:  ${enforcementCount}`);
    console.log(`  FTS entries:          ${ftsCount}`);
    console.log(`  HTTP requests:        ${requestCount}`);
    console.log(`\n  Database: ${DB_PATH}`);

    db.close();
  } else {
    console.log(`\n=== Dry Run Summary ===\n`);
    console.log(`  Documents discovered: ${discovered.length}`);
    console.log(`  Provisions extracted: ${totalProvisions}`);
    console.log(`  HTTP requests:        ${requestCount}`);
    console.log(`\n  No database writes (--dry-run)`);
  }

  console.log("\nDone.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
