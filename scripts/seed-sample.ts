/**
 * Seed the CSSF Luxembourg database with sample provisions for testing.
 *
 * Inserts well-known CSSF circulaires, reglements, and enforcement actions
 * so MCP tools can be tested without running a full ingestion crawl.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["CSSF_DB_PATH"] ?? "data/cssf.db";
const force = process.argv.includes("--force");

// ── Bootstrap database ───────────────────────────────────────────────────────

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

// ── Sourcebooks ──────────────────────────────────────────────────────────────

interface SourcebookRow {
  id: string;
  name: string;
  description: string;
}

const sourcebooks: SourcebookRow[] = [
  {
    id: "CSSF_CIRCULAIRES",
    name: "Circulaires CSSF",
    description:
      "Circulaires emises par la CSSF fixant les exigences detaillees applicables aux entites surveillees en matiere de gouvernance, risques, conformite et securite informatique.",
  },
  {
    id: "CSSF_REGLEMENTS",
    name: "Reglements CSSF",
    description:
      "Reglements contraignants de la CSSF, notamment en matiere de lutte contre le blanchiment et le financement du terrorisme (LBC/FT), de capital et de reporting.",
  },
  {
    id: "CSSF_FAQ",
    name: "FAQ CSSF",
    description:
      "Questions et reponses officielles publiees par la CSSF clarifiant l'interpretation et l'application des textes reglementaires.",
  },
  {
    id: "CSSF_COMMUNIQUES",
    name: "Communiques de presse CSSF",
    description:
      "Communiques de presse et annonces de la CSSF, incluant les decisions de sanction et les mises en garde aux investisseurs.",
  },
];

const insertSourcebook = db.prepare(
  "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
);

for (const sb of sourcebooks) {
  insertSourcebook.run(sb.id, sb.name, sb.description);
}

console.log(`Inserted ${sourcebooks.length} sourcebooks`);

// ── Sample provisions ────────────────────────────────────────────────────────

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
  // ── Circulaire 12/552 — Gouvernance des systemes d'information ──────────
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 12/552",
    title: "Exigences concernant la gouvernance et la surveillance des domaines Systemes d'Information et Communication",
    text: "La presente circulaire fixe les exigences minimales que doivent respecter les etablissements de credit et les autres professionnels du secteur financier en matiere de gouvernance et de surveillance des domaines Systemes d'Information et Communication (SIC). Elle s'applique egalement aux entreprises d'investissement et aux organismes de placement collectif agrees au Luxembourg.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2012-12-19",
    chapter: "1",
    section: "1.1",
  },
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 12/552 Chapitre 2",
    title: "Gouvernance et organisation des SIC",
    text: "Les etablissements doivent mettre en place une structure de gouvernance claire pour les SIC comprenant: (a) une strategie SIC approuvee par l'organe de direction; (b) une politique de securite des SIC documentee et regulierement mise a jour; (c) des procedures de gestion des changements et des incidents; (d) un processus de gestion des risques SIC integre dans le dispositif global de gestion des risques de l'etablissement.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2012-12-19",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 12/552 Chapitre 3",
    title: "Securite des systemes d'information",
    text: "Les etablissements doivent etablir et maintenir un niveau adequat de securite de leurs systemes d'information. Cela comprend notamment: la protection contre les acces non autorises, la confidentialite et l'integrite des donnees, la disponibilite des systemes critiques, et la mise en oeuvre de controles preventifs, detectifs et correctifs.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2012-12-19",
    chapter: "3",
    section: "3.1",
  },
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 12/552 Chapitre 6",
    title: "Continuite des activites et plans de secours",
    text: "Les etablissements doivent disposer de plans de continuite des activites (PCA) et de plans de reprise apres sinistre (PRS) couvrant leurs systemes d'information critiques. Ces plans doivent etre testes regulierement, au moins une fois par an, et mis a jour en fonction des resultats des tests et des changements de l'environnement operationnel.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2012-12-19",
    chapter: "6",
    section: "6.1",
  },

  // ── Circulaire 20/750 — ICT et gestion des risques de securite ──────────
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 20/750",
    title: "Exigences specifiques pour la gouvernance et la gestion des risques lies aux technologies de l'information et de la communication (ICT) et a la securite",
    text: "La presente circulaire transpose les orientations de l'ABE (EBA/GL/2019/04) sur la gestion des risques ICT et de securite. Elle etablit des exigences detaillees pour les etablissements de credit, les entreprises d'investissement et les etablissements de paiement en matiere de cadre de gouvernance ICT, de gestion des risques, de securite de l'information et de gestion des incidents.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2021-01-04",
    chapter: "1",
    section: "1.1",
  },
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 20/750 Section 3",
    title: "Cadre de gestion des risques ICT",
    text: "Les etablissements doivent mettre en place un cadre de gestion des risques ICT solide comprenant: (a) des politiques et procedures documentees de gestion des risques ICT; (b) une classification des actifs ICT et des donnees; (c) une analyse reguliere des risques ICT; (d) des mesures de prevention, de detection et de correction; (e) un programme de tests de securite incluant des tests de penetration et des evaluations de vulnerabilites.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2021-01-04",
    chapter: "3",
    section: "3.1",
  },
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 20/750 Section 5",
    title: "Gestion des incidents de securite ICT",
    text: "Les etablissements doivent etablir et maintenir un processus de gestion des incidents de securite ICT comprenant: la detection et la classification des incidents, la notification a la CSSF des incidents majeurs, la communication avec les parties prenantes affectees, et les procedures de retour a la normale. Les incidents majeurs doivent etre notifies a la CSSF dans les delais prevus par la reglementation applicable.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2021-01-04",
    chapter: "5",
    section: "5.2",
  },

  // ── Circulaire 17/654 — Gouvernance OPC ─────────────────────────────────
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 17/654",
    title: "Gouvernance des organismes de placement collectif (OPC)",
    text: "La presente circulaire precise les exigences de gouvernance applicables aux organismes de placement collectif (OPC) agrees au Luxembourg. Elle couvre notamment la composition et le fonctionnement du conseil d'administration, les politiques de remuneration, la gestion des conflits d'interets, et les obligations de reporting envers la CSSF.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2017-09-27",
    chapter: "1",
    section: "1.1",
  },
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 17/654 Chapitre 2",
    title: "Composition et fonctionnement du conseil d'administration des OPC",
    text: "Le conseil d'administration d'un OPC doit comprendre un nombre suffisant de membres disposant des competences, de l'honorabilite et de la disponibilite necessaires. Au moins un tiers des administrateurs doivent etre independants de la societe de gestion. Le conseil doit se reunir au moins quatre fois par an et disposer d'un reglement interne documentant ses procedures de prise de decision.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2017-09-27",
    chapter: "2",
    section: "2.1",
  },

  // ── Circulaire 22/806 — Preparation DORA ────────────────────────────────
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 22/806",
    title: "Preparation au reglement DORA (Digital Operational Resilience Act)",
    text: "La presente circulaire informe les entites financieres soumises a la surveillance de la CSSF des exigences du reglement (UE) 2022/2554 relatif a la resilience operationnelle numerique du secteur financier (DORA). Elle precise le calendrier de mise en conformite, les principales exigences en matiere de gestion des risques ICT, de tests de resilience, de gestion des risques lies aux prestataires tiers, et de notification des incidents.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2022-12-14",
    chapter: "1",
    section: "1.1",
  },
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 22/806 Section 2",
    title: "Exigences DORA en matiere de gestion des risques ICT",
    text: "Le reglement DORA impose aux entites financieres de disposer d'un cadre de gestion des risques ICT interne solide incluant: des strategies, politiques, procedures et outils ICT documentees; une gouvernance claire avec des responsabilites definies; des mesures de protection et de prevention des risques ICT; des capacites de detection des anomalies et des incidents; et des plans de communication et de continuite des activites.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2022-12-14",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "CSSF_CIRCULAIRES",
    reference: "Circulaire 22/806 Section 4",
    title: "Tests de resilience operationnelle numerique sous DORA",
    text: "DORA exige des entites financieres qu'elles mettent en oeuvre un programme de tests de resilience operationnelle numerique comprenant: des tests de base (tests de vulnerabilite, revisions open source, evaluations de la securite du reseau); des tests avances de penetration basee sur la menace (TLPT) pour les entites significatives; et des evaluations periodiques regulieres des outils et systemes ICT.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2022-12-14",
    chapter: "4",
    section: "4.1",
  },

  // ── Reglement LBC/FT ────────────────────────────────────────────────────
  {
    sourcebook_id: "CSSF_REGLEMENTS",
    reference: "Reglement CSSF N 12-02",
    title: "Reglement relatif a la lutte contre le blanchiment et contre le financement du terrorisme",
    text: "Le present reglement fixe les obligations des professionnels du secteur financier en matiere de lutte contre le blanchiment de capitaux et le financement du terrorisme (LBC/FT). Il transpose les dispositions pertinentes de la directive (UE) 2015/849 (4eme directive anti-blanchiment) et de ses actes delegues en droit luxembourgeois.",
    type: "reglement",
    status: "in_force",
    effective_date: "2012-01-01",
    chapter: "1",
    section: "1.1",
  },
  {
    sourcebook_id: "CSSF_REGLEMENTS",
    reference: "Reglement CSSF N 12-02 Art. 3",
    title: "Obligations de vigilance a l'egard de la clientele",
    text: "Les professionnels du secteur financier sont tenus d'appliquer des mesures de vigilance a l'egard de leur clientele comprenant: (a) l'identification et la verification de l'identite du client; (b) l'identification du beneficiaire effectif; (c) la comprehension de l'objet et de la nature envisagee de la relation d'affaires; (d) une surveillance continue de la relation d'affaires. Des mesures de vigilance renforcees sont requises pour les clients presentant un risque eleve de blanchiment.",
    type: "reglement",
    status: "in_force",
    effective_date: "2012-01-01",
    chapter: "1",
    section: "3.1",
  },
  {
    sourcebook_id: "CSSF_REGLEMENTS",
    reference: "Reglement CSSF N 12-02 Art. 9",
    title: "Declaration de soupcon",
    text: "Tout professionnel qui sait, suspecte ou a des raisons de suspecter qu'un blanchiment de capitaux ou un financement du terrorisme est en cours ou a ete commis est tenu d'en informer sans delai la Cellule de renseignement financier (CRF) Luxembourg. La declaration doit etre effectuee via le systeme goAML et doit contenir toutes les informations disponibles permettant l'identification des personnes concernees.",
    type: "reglement",
    status: "in_force",
    effective_date: "2012-01-01",
    chapter: "2",
    section: "9.1",
  },

  // ── FAQ CSSF — DORA ─────────────────────────────────────────────────────
  {
    sourcebook_id: "CSSF_FAQ",
    reference: "FAQ DORA 2024-001",
    title: "DORA — Champ d'application: quelles entites sont concernees?",
    text: "Le reglement DORA s'applique a un large eventail d'entites financieres etablies dans l'UE, notamment: les etablissements de credit, les etablissements de paiement, les etablissements de monnaie electronique, les entreprises d'investissement, les prestataires de services sur crypto-actifs, les societes de gestion d'OPC et GFIA, les institutions de retraite professionnelle, les compagnies d'assurance et de reassurance, et leurs prestataires tiers de services TIC critiques.",
    type: "faq",
    status: "in_force",
    effective_date: "2024-01-17",
    chapter: "1",
    section: "1.1",
  },
  {
    sourcebook_id: "CSSF_FAQ",
    reference: "FAQ DORA 2024-002",
    title: "DORA — Notification des incidents: quels delais s'appliquent?",
    text: "Sous DORA, les entites financieres doivent notifier les incidents majeurs lies aux TIC selon le schema suivant: (1) notification initiale a la CSSF dans les 4 heures suivant la classification de l'incident comme majeur, mais au plus tard 24 heures apres la detection; (2) rapport intermediaire dans les 72 heures; (3) rapport final dans le mois suivant la cloture de l'incident. La classification comme incident majeur repose sur des criteres definis par les normes techniques de reglementation (RTS) de l'ABE.",
    type: "faq",
    status: "in_force",
    effective_date: "2024-01-17",
    chapter: "2",
    section: "2.1",
  },
];

const insertProvision = db.prepare(`
  INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertAll = db.transaction(() => {
  for (const p of provisions) {
    insertProvision.run(
      p.sourcebook_id,
      p.reference,
      p.title,
      p.text,
      p.type,
      p.status,
      p.effective_date,
      p.chapter,
      p.section,
    );
  }
});

insertAll();

console.log(`Inserted ${provisions.length} sample provisions`);

// ── Sample enforcement actions ───────────────────────────────────────────────

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
    firm_name: "Clearstream Banking S.A.",
    reference_number: "CSSF-2023-ENF-001",
    action_type: "fine",
    amount: 4_950_000,
    date: "2023-07-06",
    summary:
      "La CSSF a inflige une amende administrative de 4.950.000 EUR a Clearstream Banking S.A. pour des manquements aux obligations de lutte contre le blanchiment de capitaux et le financement du terrorisme (LBC/FT). Les defaillances constatees portaient notamment sur l'insuffisance des procedures de vigilance a l'egard de la clientele, le retard dans la mise a jour des dossiers clients et des lacunes dans le systeme de surveillance des transactions.",
    sourcebook_references: "Reglement CSSF N 12-02 Art. 3, Art. 9",
  },
  {
    firm_name: "Societe de gestion de fonds (anonymisee)",
    reference_number: "CSSF-2022-ENF-007",
    action_type: "fine",
    amount: 250_000,
    date: "2022-11-15",
    summary:
      "Amende administrative infligee a une societe de gestion d'OPC pour manquements aux exigences de gouvernance prevues par la Circulaire CSSF 17/654. Les violations portaient sur la composition insuffisante du conseil d'administration (nombre d'administrateurs independants inferieur au minimum requis) et l'absence de documentation adequate des procedures de gestion des conflits d'interets.",
    sourcebook_references: "Circulaire 17/654 Chapitre 2",
  },
  {
    firm_name: "Etablissement de credit (anonymise)",
    reference_number: "CSSF-2021-ENF-003",
    action_type: "restriction",
    amount: 0,
    date: "2021-06-22",
    summary:
      "La CSSF a impose des restrictions operationnelles a un etablissement de credit suite a des deficiences graves dans son systeme de gouvernance des technologies de l'information. L'etablissement a ete contraint de suspendre l'onboarding de nouveaux clients jusqu'a remediation complete des lacunes identifiees dans la Circulaire CSSF 12/552, notamment en matiere de plans de continuite et de gestion des incidents de securite ICT.",
    sourcebook_references: "Circulaire 12/552 Chapitre 3, Circulaire 12/552 Chapitre 6",
  },
];

const insertEnforcement = db.prepare(`
  INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const insertEnforcementsAll = db.transaction(() => {
  for (const e of enforcements) {
    insertEnforcement.run(
      e.firm_name,
      e.reference_number,
      e.action_type,
      e.amount,
      e.date,
      e.summary,
      e.sourcebook_references,
    );
  }
});

insertEnforcementsAll();

console.log(`Inserted ${enforcements.length} sample enforcement actions`);

// ── Summary ──────────────────────────────────────────────────────────────────

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

console.log(`\nDatabase summary:`);
console.log(`  Sourcebooks:          ${sourcebookCount}`);
console.log(`  Provisions:           ${provisionCount}`);
console.log(`  Enforcement actions:  ${enforcementCount}`);
console.log(`  FTS entries:          ${ftsCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
