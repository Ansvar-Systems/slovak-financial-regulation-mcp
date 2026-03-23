#!/usr/bin/env tsx
/**
 * NBS (Národná banka Slovenska) ingestion crawler.
 *
 * Crawls three sections of nbs.sk:
 *   1. Opatrenia (decrees / measures) — from the legislation listing
 *   2. Metodické usmernenia (methodological guidelines) — from the legislation listing
 *   3. Výroky právoplatných rozhodnutí (binding enforcement decisions) — from the decisions section
 *
 * Data is written directly into the SQLite database used by the MCP server.
 *
 * Usage:
 *   npx tsx scripts/ingest-nbs.ts
 *   npx tsx scripts/ingest-nbs.ts --resume       # skip already-ingested references
 *   npx tsx scripts/ingest-nbs.ts --dry-run      # crawl and log, do not write to DB
 *   npx tsx scripts/ingest-nbs.ts --force         # drop existing data and re-ingest
 *   npx tsx scripts/ingest-nbs.ts --limit 20      # cap items per sourcebook
 */

import Database from "better-sqlite3";
import * as cheerio from "cheerio";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ─── Configuration ─────────────────────────────────────────────────────────────

const DB_PATH = process.env["NBS_DB_PATH"] ?? "data/nbs.db";

const NBS_BASE = "https://nbs.sk";

/** Delay between HTTP requests (ms). */
const RATE_LIMIT_MS = 1500;

/** Maximum retry attempts per request. */
const MAX_RETRIES = 3;

/** Base back-off delay on retry (ms), doubled each attempt. */
const RETRY_BACKOFF_MS = 2000;

/** Request timeout (ms). */
const REQUEST_TIMEOUT_MS = 30_000;

const USER_AGENT =
  "AnsvarNBSCrawler/1.0 (+https://ansvar.eu; compliance research)";

// ─── NBS URL map ───────────────────────────────────────────────────────────────

/**
 * NBS legislation listing — contains opatrenia, metodické usmernenia, and
 * EU transpositions.  The page uses WordPress-style `?pg=N` pagination.
 * Slovak-language path is the canonical one.
 */
const LEGISLATION_INDEX_URL =
  `${NBS_BASE}/dohlad-nad-financnym-trhom/legislativa/legislativa/`;

/**
 * Published binding decisions (enforcement).
 * Uses `?pg=N` pagination and links to detail pages.
 */
const DECISIONS_INDEX_URL =
  `${NBS_BASE}/dohlad-nad-financnym-trhom/vyroky-pravoplatnych-rozhodnuti/`;

/**
 * Archive of older decisions (pre-2020 in most cases).
 */
const DECISIONS_ARCHIVE_URL =
  `${NBS_BASE}/dohlad-nad-financnym-trhom/vyroky-pravoplatnych-rozhodnuti/archiv-rozhodnuti/`;

// ─── CLI arguments ─────────────────────────────────────────────────────────────

interface CliFlags {
  resume: boolean;
  dryRun: boolean;
  force: boolean;
  limit: number | null;
}

function parseFlags(): CliFlags {
  const args = process.argv.slice(2);
  let limit: number | null = null;
  let resume = false;
  let dryRun = false;
  let force = false;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--resume") {
      resume = true;
    } else if (arg === "--dry-run") {
      dryRun = true;
    } else if (arg === "--force") {
      force = true;
    } else if (arg === "--limit" && args[i + 1]) {
      limit = Number.parseInt(args[i + 1]!, 10);
      i++;
    }
  }

  return { resume, dryRun, force, limit };
}

// ─── HTTP helpers ──────────────────────────────────────────────────────────────

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const controller = new AbortController();
      const timeout = setTimeout(
        () => controller.abort(),
        REQUEST_TIMEOUT_MS,
      );

      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml",
          "Accept-Language": "sk",
        },
        signal: controller.signal,
      });

      clearTimeout(timeout);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status} for ${url}`);
      }

      return await response.text();
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      if (attempt < MAX_RETRIES) {
        const backoff = RETRY_BACKOFF_MS * Math.pow(2, attempt - 1);
        console.warn(
          `  Retry ${attempt}/${MAX_RETRIES} for ${url} (${lastError.message}), waiting ${backoff}ms`,
        );
        await sleep(backoff);
      }
    }
  }

  throw lastError ?? new Error(`Failed to fetch ${url} after ${MAX_RETRIES} attempts`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── Sourcebook definitions ────────────────────────────────────────────────────

interface SourcebookDef {
  id: string;
  name: string;
  description: string;
}

const SOURCEBOOKS: SourcebookDef[] = [
  {
    id: "NBS_OPATRENIA",
    name: "NBS Opatrenia",
    description:
      "Opatrenia Národnej banky Slovenska — všeobecne záväzné právne predpisy vydávané NBS na základe zákonov upravujúcich finančný trh (kapitálové požiadavky, likvidita, obozretné podnikanie, vykazovanie).",
  },
  {
    id: "NBS_USMERNENIA",
    name: "NBS Metodické usmernenia",
    description:
      "Metodické usmernenia útvarov dohľadu nad finančným trhom NBS — interpretačné a aplikačné dokumenty pre účastníkov finančného trhu k plneniu regulačných požiadaviek.",
  },
  {
    id: "NBS_ROZHODNUTIA",
    name: "NBS Rozhodnutia",
    description:
      "Výroky právoplatných rozhodnutí NBS — zverejnené rozhodnutia o uložených sankciách, odobratí povolení a iných opatreniach na nápravu voči subjektom finančného trhu.",
  },
];

// ─── Parsed item types ─────────────────────────────────────────────────────────

interface LegislationListItem {
  title: string;
  url: string;
  /** "opatrenie" | "usmernenie" | "other" */
  type: string;
}

interface ParsedProvision {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string | null;
  chapter: string | null;
  section: string | null;
  source_url: string;
}

interface ParsedEnforcement {
  firm_name: string;
  reference_number: string | null;
  action_type: string | null;
  amount: number | null;
  date: string | null;
  summary: string;
  sourcebook_references: string | null;
  source_url: string;
}

// ─── Legislation list parser ───────────────────────────────────────────────────

/**
 * Parse the legislation index page to extract links to individual opatrenia
 * and metodické usmernenia.
 *
 * The NBS legislation listing at
 *   /dohlad-nad-financnym-trhom/legislativa/legislativa/
 * renders items in a post listing layout.  Each item links to a detail page
 * at /dohlad-nad-financnym-trhom/legislativa/legislativa/detail-dokumentu/slug/.
 */
function parseLegislationList(html: string): LegislationListItem[] {
  const $ = cheerio.load(html);
  const items: LegislationListItem[] = [];

  // NBS uses article/post listing patterns — look for links whose href
  // contains "detail-dokumentu" and whose text starts with known prefixes.
  $("a[href*='detail-dokumentu']").each((_i, el) => {
    const href = $(el).attr("href");
    const text = $(el).text().trim();
    if (!href || !text) return;

    const fullUrl = href.startsWith("http") ? href : `${NBS_BASE}${href}`;

    let type = "other";
    if (/^opatrenie\b/i.test(text) || /opatreni[ea]/i.test(text)) {
      type = "opatrenie";
    } else if (/^metod/i.test(text) || /usmerneni[ea]/i.test(text)) {
      type = "usmernenie";
    }

    // Avoid duplicates within one page
    if (!items.some((it) => it.url === fullUrl)) {
      items.push({ title: text, url: fullUrl, type });
    }
  });

  return items;
}

/**
 * Detect pagination: look for next-page links like `?pg=2`, `page/2/`, etc.
 * Returns the URL of the next page or null when at the end.
 */
function findNextPage(html: string, currentUrl: string): string | null {
  const $ = cheerio.load(html);

  // Common WordPress/NBS pagination patterns
  const nextLink =
    $("a.next, a.page-next, a[rel='next'], .pagination a:contains('ďalej'), .pagination a:contains('Ďalšia'), .pagination a:contains('>')").first();

  if (nextLink.length > 0) {
    const href = nextLink.attr("href");
    if (href) {
      return href.startsWith("http") ? href : `${NBS_BASE}${href}`;
    }
  }

  // Fallback: look for numbered page links higher than current page
  const currentPageMatch = currentUrl.match(/[?&]pg=(\d+)/);
  const currentPage = currentPageMatch ? Number.parseInt(currentPageMatch[1]!, 10) : 1;

  let maxPage = currentPage;
  $("a[href*='pg=']").each((_i, el) => {
    const href = $(el).attr("href") ?? "";
    const m = href.match(/[?&]pg=(\d+)/);
    if (m) {
      const p = Number.parseInt(m[1]!, 10);
      if (p > maxPage) maxPage = p;
    }
  });

  if (maxPage > currentPage) {
    const sep = LEGISLATION_INDEX_URL.includes("?") ? "&" : "?";
    return `${LEGISLATION_INDEX_URL}${sep}pg=${currentPage + 1}`;
  }

  return null;
}

// ─── Legislation detail parser ─────────────────────────────────────────────────

/**
 * Extract the reference number (e.g. "č. 7/2024") from the title string.
 *
 * Examples:
 *   "Opatrenie Národnej banky Slovenska z 19. novembra 2024 č. 7/2024 ..."
 *     → "7/2024"
 *   "Metodické usmernenie ... z 22. februára 2024 č. 1/2024 ..."
 *     → "1/2024"
 */
function extractReference(title: string): string | null {
  // "č. 7/2024" or "č. 375/2025 Z. z."
  const m = title.match(/č\.\s*(\d+\/\d{4})/);
  return m ? m[1]! : null;
}

/**
 * Extract an approximate effective date from the title string.
 *
 * The title typically starts with "Opatrenie Národnej banky Slovenska z DD. MMMM YYYY".
 */
function extractDateFromTitle(title: string): string | null {
  const MONTHS: Record<string, string> = {
    januára: "01", februára: "02", marca: "03", apríla: "04",
    mája: "05", júna: "06", júla: "07", augusta: "08",
    septembra: "09", októbra: "10", novembra: "11", decembra: "12",
  };

  const m = title.match(
    /z?\s*(\d{1,2})\.\s*(januára|februára|marca|apríla|mája|júna|júla|augusta|septembra|októbra|novembra|decembra)\s+(\d{4})/i,
  );
  if (!m) return null;

  const day = m[1]!.padStart(2, "0");
  const month = MONTHS[m[2]!.toLowerCase()];
  const year = m[3]!;
  if (!month) return null;

  return `${year}-${month}-${day}`;
}

/**
 * Parse the detail page of a single opatrenie or usmernenie.
 * Extracts the document body text.
 */
function parseDetailPage(
  html: string,
  listItem: LegislationListItem,
): ParsedProvision | null {
  const $ = cheerio.load(html);

  // The detail page typically has the main content in .entry-content, article,
  // or a main content div.  Try several selectors.
  const contentSelectors = [
    ".entry-content",
    ".post-content",
    "article .content",
    "article",
    ".main-content",
    "#content",
    ".single-post-content",
    ".wp-block-group",
  ];

  let bodyText = "";
  for (const sel of contentSelectors) {
    const el = $(sel);
    if (el.length > 0) {
      bodyText = el.text().trim();
      if (bodyText.length > 100) break;
    }
  }

  // Fallback: grab the largest text block from the page body
  if (bodyText.length < 100) {
    bodyText = $("main").text().trim() || $("body").text().trim();
  }

  // Clean up whitespace
  bodyText = bodyText.replace(/\s+/g, " ").trim();

  if (!bodyText) return null;

  // Truncate extremely long texts — keep first 15 000 chars to stay within
  // reasonable DB size while retaining the substantive content.
  if (bodyText.length > 15_000) {
    bodyText = bodyText.slice(0, 15_000) + " [...]";
  }

  const title = listItem.title;
  const ref = extractReference(title);
  const date = extractDateFromTitle(title);

  const sourcebookId =
    listItem.type === "opatrenie" ? "NBS_OPATRENIA" : "NBS_USMERNENIA";

  const shortRef = ref
    ? `${sourcebookId} ${ref}`
    : `${sourcebookId} ${slugFromUrl(listItem.url)}`;

  return {
    sourcebook_id: sourcebookId,
    reference: shortRef,
    title: truncate(title, 500),
    text: bodyText,
    type: listItem.type,
    status: "in_force",
    effective_date: date,
    chapter: null,
    section: null,
    source_url: listItem.url,
  };
}

// ─── Decision / enforcement parsers ────────────────────────────────────────────

interface DecisionListItem {
  title: string;
  url: string;
}

/**
 * Parse the decisions index page to extract links to individual decision detail pages.
 */
function parseDecisionsList(html: string): DecisionListItem[] {
  const $ = cheerio.load(html);
  const items: DecisionListItem[] = [];

  // Decision listings use links to detail pages or direct links to verdict pages.
  // Look for links containing "rozhodnutia-detail" or "vyrok-rozhodnutia".
  $(
    "a[href*='rozhodnutia-detail'], a[href*='vyrok-rozhodnutia'], a[href*='vyroky/']",
  ).each((_i, el) => {
    const href = $(el).attr("href");
    const text = $(el).text().trim();
    if (!href || !text || text.length < 5) return;

    const fullUrl = href.startsWith("http") ? href : `${NBS_BASE}${href}`;
    if (!items.some((it) => it.url === fullUrl)) {
      items.push({ title: text, url: fullUrl });
    }
  });

  // Also pick up links in table rows or list items that point to decision PDFs
  // on the old NBS domain
  $("a[href*='ofsrozhodnutia']").each((_i, el) => {
    const href = $(el).attr("href");
    const text = $(el).text().trim();
    if (!href || !text) return;

    const fullUrl = href.startsWith("http") ? href : `${NBS_BASE}${href}`;
    if (!items.some((it) => it.url === fullUrl)) {
      items.push({ title: text, url: fullUrl });
    }
  });

  return items;
}

/**
 * Parse a decision detail page to extract enforcement data.
 */
function parseDecisionDetail(
  html: string,
  listItem: DecisionListItem,
): ParsedEnforcement | null {
  const $ = cheerio.load(html);

  // Try to extract the decision text from the detail page
  const contentSelectors = [
    ".entry-content",
    ".post-content",
    "article",
    ".main-content",
    "#content",
    "main",
  ];

  let bodyText = "";
  for (const sel of contentSelectors) {
    const el = $(sel);
    if (el.length > 0) {
      bodyText = el.text().trim();
      if (bodyText.length > 80) break;
    }
  }

  bodyText = bodyText.replace(/\s+/g, " ").trim();
  if (!bodyText || bodyText.length < 30) return null;

  if (bodyText.length > 10_000) {
    bodyText = bodyText.slice(0, 10_000) + " [...]";
  }

  // Extract firm name — typically the first entity mentioned, often in bold
  // or as the subject of the decision.
  const firmName = extractFirmName($, listItem.title) ?? listItem.title;

  // Extract reference number from the decision body
  const refMatch = bodyText.match(
    /(?:č\.\s*(?:sp\.|k\.)?\s*:?\s*)(NBS\d?[-\s]*\d{3}[-\s]*\d{3}[-\s]*\d{3}[-\s]*\d{3})/i,
  );
  const referenceNumber = refMatch ? refMatch[1]!.replace(/\s+/g, "-") : null;

  // Classify the action type from the text
  const actionType = classifyActionType(bodyText);

  // Try to extract a fine amount
  const amount = extractFineAmount(bodyText);

  // Extract date
  const dateMatch = bodyText.match(
    /(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})/,
  );
  let date: string | null = null;
  if (dateMatch) {
    date = `${dateMatch[3]}-${dateMatch[2]!.padStart(2, "0")}-${dateMatch[1]!.padStart(2, "0")}`;
  }

  return {
    firm_name: truncate(firmName, 300),
    reference_number: referenceNumber,
    action_type: actionType,
    amount,
    date,
    summary: truncate(bodyText, 5000),
    sourcebook_references: null,
    source_url: listItem.url,
  };
}

/**
 * Try to extract a firm/entity name from the page or the list item title.
 */
function extractFirmName(
  $: cheerio.CheerioAPI,
  fallbackTitle: string,
): string | null {
  // Look for the first bold text in the content, which often names the entity
  const bold = $("article strong, .entry-content strong, main strong").first();
  if (bold.length > 0) {
    const name = bold.text().trim();
    if (name.length > 3 && name.length < 200) return name;
  }

  // Look for "voči" (against) pattern in the title
  const m = fallbackTitle.match(/voči\s+(.+?)(?:\s*[-–—,]|$)/i);
  if (m) return m[1]!.trim();

  return null;
}

/**
 * Classify the type of enforcement action from the decision text.
 */
function classifyActionType(text: string): string {
  const lower = text.toLowerCase();
  if (lower.includes("pokut") || lower.includes("penale") || lower.includes("peňažn")) {
    return "fine";
  }
  if (lower.includes("odobrat") || lower.includes("zrušen") || lower.includes("odňat")) {
    return "license_revocation";
  }
  if (lower.includes("obmedz") || lower.includes("zákaz")) {
    return "restriction";
  }
  if (lower.includes("napomenu") || lower.includes("upozorn") || lower.includes("výtk")) {
    return "warning";
  }
  if (lower.includes("opatren") && lower.includes("náprav")) {
    return "remedial_measure";
  }
  return "other";
}

/**
 * Extract a fine amount in EUR from the text.
 */
function extractFineAmount(text: string): number | null {
  // Match patterns like "100 000 eur", "50.000 EUR", "25000 eur"
  const patterns = [
    /(\d[\d\s.,]*\d)\s*(?:eur|€)/i,
    /(?:pokut[auy]?\s+(?:vo?\s+)?(?:výšk[ey]\s+)?|sumu?\s+)(\d[\d\s.,]*\d)/i,
  ];

  for (const pattern of patterns) {
    const m = text.match(pattern);
    if (m) {
      // Normalize the number string
      const numStr = m[1]!
        .replace(/\s/g, "")
        .replace(/,/g, ".");
      const num = Number.parseFloat(numStr);
      if (!Number.isNaN(num) && num > 0) return num;
    }
  }

  return null;
}

// ─── Utilities ─────────────────────────────────────────────────────────────────

function slugFromUrl(url: string): string {
  const parts = url.replace(/\/+$/, "").split("/");
  return parts[parts.length - 1] ?? "unknown";
}

function truncate(str: string, max: number): string {
  if (str.length <= max) return str;
  return str.slice(0, max - 4) + " ...";
}

// ─── Database helpers ──────────────────────────────────────────────────────────

function openDb(force: boolean): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  return db;
}

function upsertSourcebooks(db: Database.Database): void {
  const stmt = db.prepare(
    "INSERT OR REPLACE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
  );
  const tx = db.transaction(() => {
    for (const sb of SOURCEBOOKS) {
      stmt.run(sb.id, sb.name, sb.description);
    }
  });
  tx();
}

function referenceExists(db: Database.Database, reference: string): boolean {
  const row = db
    .prepare("SELECT 1 FROM provisions WHERE reference = ? LIMIT 1")
    .get(reference) as { 1: number } | undefined;
  return row !== undefined;
}

function firmExists(db: Database.Database, firmName: string, refNum: string | null): boolean {
  if (refNum) {
    const row = db
      .prepare("SELECT 1 FROM enforcement_actions WHERE reference_number = ? LIMIT 1")
      .get(refNum) as { 1: number } | undefined;
    return row !== undefined;
  }
  const row = db
    .prepare("SELECT 1 FROM enforcement_actions WHERE firm_name = ? LIMIT 1")
    .get(firmName) as { 1: number } | undefined;
  return row !== undefined;
}

// ─── Main crawl orchestration ──────────────────────────────────────────────────

async function crawlLegislation(
  db: Database.Database,
  flags: CliFlags,
): Promise<{ opatreniaCount: number; usmerneniaCount: number; skipped: number; errors: number }> {
  console.log("\n--- Crawling NBS legislation index ---");
  console.log(`  Start URL: ${LEGISLATION_INDEX_URL}`);

  let allItems: LegislationListItem[] = [];
  let pageUrl: string | null = LEGISLATION_INDEX_URL;
  let pageNum = 1;

  // Paginate through the legislation index
  while (pageUrl) {
    console.log(`  Fetching page ${pageNum}: ${pageUrl}`);
    const html = await rateLimitedFetch(pageUrl);
    const items = parseLegislationList(html);
    console.log(`    Found ${items.length} items on page ${pageNum}`);
    allItems = allItems.concat(items);

    pageUrl = findNextPage(html, pageUrl);
    pageNum++;

    // Safety limit to avoid infinite pagination loops
    if (pageNum > 100) {
      console.warn("  Pagination safety limit reached (100 pages)");
      break;
    }
  }

  // Filter to opatrenia and usmernenia only
  const opatrenia = allItems.filter((it) => it.type === "opatrenie");
  const usmernenia = allItems.filter((it) => it.type === "usmernenie");

  console.log(
    `  Total items: ${allItems.length} (${opatrenia.length} opatrenia, ${usmernenia.length} usmernenia, ${allItems.length - opatrenia.length - usmernenia.length} other)`,
  );

  // Apply limit
  const targetOpatrenia = flags.limit
    ? opatrenia.slice(0, flags.limit)
    : opatrenia;
  const targetUsmernenia = flags.limit
    ? usmernenia.slice(0, flags.limit)
    : usmernenia;

  const insertStmt = db.prepare(`
    INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let opatreniaCount = 0;
  let usmerneniaCount = 0;
  let skipped = 0;
  let errors = 0;

  const combined = [
    ...targetOpatrenia.map((it) => ({ ...it, sourcebookId: "NBS_OPATRENIA" })),
    ...targetUsmernenia.map((it) => ({ ...it, sourcebookId: "NBS_USMERNENIA" })),
  ];

  for (const item of combined) {
    const ref = extractReference(item.title);
    const shortRef = ref
      ? `${item.sourcebookId} ${ref}`
      : `${item.sourcebookId} ${slugFromUrl(item.url)}`;

    // Resume support — skip if reference already in DB
    if (flags.resume && referenceExists(db, shortRef)) {
      skipped++;
      continue;
    }

    console.log(`  [${shortRef}] Fetching detail page...`);
    try {
      const detailHtml = await rateLimitedFetch(item.url);
      const provision = parseDetailPage(detailHtml, item);

      if (!provision) {
        console.log(`    SKIP (no content extracted)`);
        skipped++;
        continue;
      }

      if (flags.dryRun) {
        console.log(
          `    DRY-RUN: would insert ${provision.reference} (${provision.text.length} chars)`,
        );
      } else {
        insertStmt.run(
          provision.sourcebook_id,
          provision.reference,
          provision.title,
          provision.text,
          provision.type,
          provision.status,
          provision.effective_date,
          provision.chapter,
          provision.section,
        );
        console.log(
          `    OK (${provision.text.length} chars, date: ${provision.effective_date ?? "unknown"})`,
        );
      }

      if (item.sourcebookId === "NBS_OPATRENIA") opatreniaCount++;
      else usmerneniaCount++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`    ERROR: ${msg}`);
      errors++;
    }
  }

  return { opatreniaCount, usmerneniaCount, skipped, errors };
}

async function crawlDecisions(
  db: Database.Database,
  flags: CliFlags,
): Promise<{ count: number; skipped: number; errors: number }> {
  console.log("\n--- Crawling NBS enforcement decisions ---");

  let allDecisions: DecisionListItem[] = [];

  // Crawl both the main decisions page and the archive
  for (const indexUrl of [DECISIONS_INDEX_URL, DECISIONS_ARCHIVE_URL]) {
    let pageUrl: string | null = indexUrl;
    let pageNum = 1;

    while (pageUrl) {
      console.log(`  Fetching decisions page ${pageNum}: ${pageUrl}`);
      const html = await rateLimitedFetch(pageUrl);
      const items = parseDecisionsList(html);
      console.log(`    Found ${items.length} decision links on page ${pageNum}`);

      for (const item of items) {
        if (!allDecisions.some((d) => d.url === item.url)) {
          allDecisions.push(item);
        }
      }

      pageUrl = findNextPage(html, pageUrl);
      pageNum++;

      if (pageNum > 50) {
        console.warn("  Decision pagination safety limit reached (50 pages)");
        break;
      }
    }
  }

  console.log(`  Total unique decision links: ${allDecisions.length}`);

  if (flags.limit) {
    allDecisions = allDecisions.slice(0, flags.limit);
  }

  const insertStmt = db.prepare(`
    INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  let count = 0;
  let skipped = 0;
  let errors = 0;

  for (const item of allDecisions) {
    // Skip PDF links — we cannot parse them with cheerio
    if (item.url.endsWith(".pdf")) {
      console.log(`  [PDF] Skipping: ${item.title}`);
      skipped++;
      continue;
    }

    console.log(`  [decision] Fetching: ${truncate(item.title, 80)}...`);
    try {
      const html = await rateLimitedFetch(item.url);
      const enforcement = parseDecisionDetail(html, item);

      if (!enforcement) {
        console.log(`    SKIP (no content extracted)`);
        skipped++;
        continue;
      }

      // Resume support
      if (flags.resume && firmExists(db, enforcement.firm_name, enforcement.reference_number)) {
        console.log(`    SKIP (already in DB)`);
        skipped++;
        continue;
      }

      if (flags.dryRun) {
        console.log(
          `    DRY-RUN: would insert enforcement against "${enforcement.firm_name}" (${enforcement.action_type})`,
        );
      } else {
        insertStmt.run(
          enforcement.firm_name,
          enforcement.reference_number,
          enforcement.action_type,
          enforcement.amount,
          enforcement.date,
          enforcement.summary,
          enforcement.sourcebook_references,
        );
        console.log(
          `    OK (${enforcement.action_type}, amount: ${enforcement.amount ?? "n/a"}, date: ${enforcement.date ?? "unknown"})`,
        );
      }
      count++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`    ERROR: ${msg}`);
      errors++;
    }
  }

  return { count, skipped, errors };
}

// ─── Entry point ───────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const flags = parseFlags();

  console.log("NBS Ingestion Crawler — Národná banka Slovenska");
  console.log("================================================");
  console.log(`Database:  ${DB_PATH}`);
  console.log(`Flags:     ${flags.resume ? "--resume " : ""}${flags.dryRun ? "--dry-run " : ""}${flags.force ? "--force " : ""}${flags.limit ? `--limit ${flags.limit}` : ""}`);
  console.log(`Rate limit: ${RATE_LIMIT_MS}ms between requests`);
  console.log(`Retries:   ${MAX_RETRIES} attempts with exponential backoff`);

  const db = openDb(flags.force);
  upsertSourcebooks(db);
  console.log(`Sourcebooks: ${SOURCEBOOKS.length} registered`);

  const legResult = await crawlLegislation(db, flags);
  const decResult = await crawlDecisions(db, flags);

  // Print summary
  const provisionCount = (
    db.prepare("SELECT count(*) as cnt FROM provisions").get() as { cnt: number }
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

  console.log("\n================================================");
  console.log("Ingestion summary");
  console.log("================================================");
  console.log(`Opatrenia ingested:    ${legResult.opatreniaCount}`);
  console.log(`Usmernenia ingested:   ${legResult.usmerneniaCount}`);
  console.log(`Decisions ingested:    ${decResult.count}`);
  console.log(`Skipped:               ${legResult.skipped + decResult.skipped}`);
  console.log(`Errors:                ${legResult.errors + decResult.errors}`);
  console.log("---");
  console.log(`DB provisions total:   ${provisionCount}`);
  console.log(`DB enforcement total:  ${enforcementCount}`);
  console.log(`DB FTS entries:        ${ftsCount}`);
  console.log(`\nDone. Database at ${DB_PATH}`);

  db.close();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
