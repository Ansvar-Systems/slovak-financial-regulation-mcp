/**
 * Seed the Slovak Financial Regulation database with sample provisions for testing.
 *
 * Inserts provisions from NBS_Opatrenia (capital requirements measures),
 * NBS_Usmernenia (guidelines), and NBS_Rozhodnutia (decisions) sourcebooks.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["NBS_DB_PATH"] ?? "data/nbs.db";
const force = process.argv.includes("--force");

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

console.log(`Database initialised at ${DB_PATH}`);

interface SourcebookRow {
  id: string;
  name: string;
  description: string;
}

const sourcebooks: SourcebookRow[] = [
  {
    id: "NBS_OPATRENIA",
    name: "NBS Opatrenia",
    description: "Opatrenia Narodnej banky Slovenska o kapitalovych poziadavkach, likvidite a obozretnom podnikaní pre banky a investicne spolocnosti.",
  },
  {
    id: "NBS_USMERNENIA",
    name: "NBS Metodicke usmernenia",
    description: "Metodicke usmernenia NBS pre ucastnikov financneho trhu o postupoch, vypoctoch a osvedcenych postupoch pri plneni regulacnych poziadaviek.",
  },
  {
    id: "NBS_ROZHODNUTIA",
    name: "NBS Rozhodnutia",
    description: "Rozhodnutia Narodnej banky Slovenska o udeleni, zmene alebo odnatí povolení a o sankciach ulozených subjektom financneho trhu.",
  },
];

const insertSourcebook = db.prepare(
  "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
);

for (const sb of sourcebooks) {
  insertSourcebook.run(sb.id, sb.name, sb.description);
}

console.log(`Inserted ${sourcebooks.length} sourcebooks`);

interface ProvisionRow {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string;
  chapter: string;
  section: string;
}

const provisions: ProvisionRow[] = [
  {
    sourcebook_id: "NBS_OPATRENIA",
    reference: "NBS_OPATRENIA 4/2007 par.3",
    title: "Minimalne kapitalove poziadavky pre banky",
    text: "Banky su povinne udrziavat celkovy pomer kapitalovej primerenosti najmenej na urovni 8% rizikovo vazenych aktiv. NBS moze v ramci procesu SREP ulozit banke individualnu kapitalovu poziadavku nad ramec minimalnych regulacnych poziadaviek, ak to odovodnuje jej rizikovy profil.",
    type: "opatrenie",
    status: "in_force",
    effective_date: "2007-01-01",
    chapter: "II",
    section: "3",
  },
  {
    sourcebook_id: "NBS_OPATRENIA",
    reference: "NBS_OPATRENIA 4/2007 par.8",
    title: "Kapitalove vankuse",
    text: "Banky su povinne udrziavat kapitalovy vankus na zachovanie kapitalu vo vyske 2,5% celkovych rizikovo vazenych aktiv. NBS moze ulozit proticyklicky kapitalovy vankus a vankuse pre systemovo dolezite institucie. Nesplnenie vankusovych poziadaviek automaticky obmedzuje rozdelovanie ziskov.",
    type: "opatrenie",
    status: "in_force",
    effective_date: "2016-01-01",
    chapter: "III",
    section: "8",
  },
  {
    sourcebook_id: "NBS_OPATRENIA",
    reference: "NBS_OPATRENIA 7/2015 par.4",
    title: "Ukazovatel krytia likvidity",
    text: "Banky su povinne udrziavat dostatocnu uroven vysoko likvidnych aktiv, aby pokryli ciste odlivy hotovosti pocas 30-dnovho stresoveho obdobia. Ukazovatel krytia likvidity (LCR) musi byt nepretrzite na urovni najmenej 100%.",
    type: "opatrenie",
    status: "in_force",
    effective_date: "2015-10-01",
    chapter: "IV",
    section: "4",
  },
  {
    sourcebook_id: "NBS_OPATRENIA",
    reference: "NBS_OPATRENIA 10/2018 par.6",
    title: "Poziadavky na vnutornu spravu a riadenie rizik",
    text: "Banky musia mat zavedene spolahlive opatrenia vnutornej spravy, ktore zahrnaju jasnu organizacnu strukturu s dobre definovanymi liniami zodpovednosti, ucinne procesy identifikacie, merania a riadenia rizik a robustne mechanizmy vnutornej kontroly.",
    type: "opatrenie",
    status: "in_force",
    effective_date: "2018-07-01",
    chapter: "V",
    section: "6",
  },
  {
    sourcebook_id: "NBS_USMERNENIA",
    reference: "NBS_USMERNENIA 1/2020 bod.3",
    title: "Usmernenia k vypoctu rizikovo vazenych aktiv pri kreditnom riziku",
    text: "Pre ucel vypoctu minimalnych kapitalovych poziadaviek pri kreditnom riziku mozu banky pouzit standardizovany pristup alebo pristup zalozeny na internom hodnoteni (IRB). Banky, ktore chcu pouzit pristup IRB, musia preukzat NBS, ze ich interne modely hodnotenia rizik spnajú minimálne regulacne normy.",
    type: "usmernenie",
    status: "in_force",
    effective_date: "2020-03-01",
    chapter: "II",
    section: "3",
  },
  {
    sourcebook_id: "NBS_USMERNENIA",
    reference: "NBS_USMERNENIA 3/2022 bod.5",
    title: "Usmernenia k riadeniu operacneho rizika a digitalnej odolnosti",
    text: "Banky musia mat zavedeny ramec riadenia operacneho rizika, ktory pokryva aj IT a kyberneticke riziká. V sulade s nariadenim DORA musia identifikovat kriticke informacne systemy, nastavit tolerancie prerusenia pre kriticke funkcie a pravidelnymi testmi overovat schopnost obnovy.",
    type: "usmernenie",
    status: "in_force",
    effective_date: "2022-11-01",
    chapter: "III",
    section: "5",
  },
  {
    sourcebook_id: "NBS_ROZHODNUTIA",
    reference: "NBS_ROZHODNUTIA 2019/002 clanok.1",
    title: "Udelenie povolenia na cinnost obchodnika s cennymi papiermi",
    text: "NBS udeluje povolenie na vykon cinnosti obchodnika s cennymi papiermi spolocnosti, ktora splna podmienky zakona o cennych papieroch, vratane poziadaviek na minimalny vlastny kapital, obsadenie riadiacich funkcii a systemy riadenia rizik.",
    type: "rozhodnutie",
    status: "in_force",
    effective_date: "2019-05-01",
    chapter: "I",
    section: "1",
  },
  {
    sourcebook_id: "NBS_ROZHODNUTIA",
    reference: "NBS_ROZHODNUTIA 2021/018 clanok.3",
    title: "Ulozenie sankcie za porusenie pravidiel ochrany investorov",
    text: "NBS ulozila pokutu vo vyske 100 000 eur obchodnikovi s cennymi papiermi za systematicke porusovanie pravidiel posudenia vhodnosti a primeranosti pri poskytovani investicnych sluzieb retailovym klientom.",
    type: "rozhodnutie",
    status: "in_force",
    effective_date: "2021-09-01",
    chapter: "III",
    section: "3",
  },
];

const insertProvision = db.prepare(`
  INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertAll = db.transaction(() => {
  for (const p of provisions) {
    insertProvision.run(
      p.sourcebook_id, p.reference, p.title, p.text,
      p.type, p.status, p.effective_date, p.chapter, p.section,
    );
  }
});

insertAll();

console.log(`Inserted ${provisions.length} sample provisions`);

interface EnforcementRow {
  firm_name: string;
  reference_number: string;
  action_type: string;
  amount: number;
  date: string;
  summary: string;
  sourcebook_references: string;
}

const enforcements: EnforcementRow[] = [
  {
    firm_name: "Tatra banka a.s.",
    reference_number: "NBS-2022-SAN-007",
    action_type: "fine",
    amount: 150000,
    date: "2022-07-05",
    summary: "NBS ulozila banke pokutu za nedostatky v systeme riadenia operacneho rizika a v ramci opatrenií proti praniu spinavych penazi. Banka mala neucinne postupy monitorovania transakcii a nedostatky v procese starostlivosti o klienta.",
    sourcebook_references: "NBS_OPATRENIA 10/2018 par.6, NBS_USMERNENIA 3/2022 bod.5",
  },
  {
    firm_name: "Prima banka Slovensko a.s.",
    reference_number: "NBS-2023-SAN-014",
    action_type: "restriction",
    amount: 0,
    date: "2023-04-18",
    summary: "NBS ulozila banke obmedzujuce opatrenie v podobe zakazu distribuovania ziskov po dobu 12 mesiacov v dosledku nedostatocnych kapitalovych vankusov a zistených nedostatkov pri výpocte rizikovo vazenych aktiv.",
    sourcebook_references: "NBS_OPATRENIA 4/2007 par.8, NBS_OPATRENIA 4/2007 par.3",
  },
];

const insertEnforcement = db.prepare(`
  INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const insertEnforcementsAll = db.transaction(() => {
  for (const e of enforcements) {
    insertEnforcement.run(
      e.firm_name, e.reference_number, e.action_type, e.amount,
      e.date, e.summary, e.sourcebook_references,
    );
  }
});

insertEnforcementsAll();

console.log(`Inserted ${enforcements.length} sample enforcement actions`);

const provisionCount = (db.prepare("SELECT count(*) as cnt FROM provisions").get() as { cnt: number }).cnt;
const sourcebookCount = (db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as { cnt: number }).cnt;
const enforcementCount = (db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as { cnt: number }).cnt;
const ftsCount = (db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as { cnt: number }).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Sourcebooks:          ${sourcebookCount}`);
console.log(`  Provisions:           ${provisionCount}`);
console.log(`  Enforcement actions:  ${enforcementCount}`);
console.log(`  FTS entries:          ${ftsCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
