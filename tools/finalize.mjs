import fs from 'node:fs';
import path from 'node:path';
import { root, chapters, chapterKey, deliveryDir, readJson, writeJson, hashFile, assert } from './common.mjs';

const manifest = readJson(path.join(root, 'manifest.json'));
const chapterResults = readJson(path.join(root, 'verification', 'chapter-results.json'));
const visual = readJson(path.join(root, 'verification', 'visual-inspection.json'));
const rebuild = readJson(path.join(root, 'verification', 'rebuild-check.json'));
const equivalence = readJson(path.join(root, 'verification', 'final-equivalence.json'));

assert(manifest.chapters.length === 20, 'root manifest does not contain 20 chapters');
assert(chapterResults.chapters.length === 20, 'verification results do not contain 20 chapters');
assert(chapterResults.chapters.every(chapter => chapter.commands.every(command => command.exitCode === 0)), 'a chapter verification command failed');
assert(visual.chapters.length === 20 && visual.chapters.every(chapter => chapter.visualReview === 'passed' && chapter.inspectedPages.length >= 3), 'visual inspection is incomplete');
assert(rebuild.singleChapterRebuild && rebuild.fullRebuild && rebuild.finalEquivalence && rebuild.commands.every(command => command.exitCode === 0), 'clean rebuild evidence is incomplete');
assert(equivalence.equal && equivalence.sourceFileCount === 45 && equivalence.chapterFileCount === 45, 'final ws equivalence failed');

const artifacts = [];
for (const chapter of chapters) {
  const key = chapterKey(chapter);
  const pdf = path.join(deliveryDir(chapter), `${key}.pdf`);
  const zip = path.join(deliveryDir(chapter), `${key}.zip`);
  assert(fs.existsSync(pdf) && fs.existsSync(zip), `${key}: missing artifact`);
  const entry = manifest.chapters.find(item => item.number === chapter.number);
  assert(hashFile(pdf) === entry.pdfSha256 && hashFile(zip) === entry.zipSha256, `${key}: manifest hash mismatch`);
  artifacts.push({ chapter: key, pdfBytes: fs.statSync(pdf).size, zipBytes: fs.statSync(zip).size, pdfSha256: entry.pdfSha256, zipSha256: entry.zipSha256 });
}

const summary = {
  chapterCount: 20,
  pdfCount: artifacts.length,
  zipCount: artifacts.length,
  totalPages: visual.chapters.reduce((sum, chapter) => sum + chapter.pageCount, 0),
  totalPdfBytes: artifacts.reduce((sum, artifact) => sum + artifact.pdfBytes, 0),
  totalZipBytes: artifacts.reduce((sum, artifact) => sum + artifact.zipBytes, 0),
  executedCommandCount: chapterResults.chapters.reduce((sum, chapter) => sum + chapter.commands.length, 0),
  finalWsFiles: equivalence.sourceFileCount,
  finalWsEqual: equivalence.equal,
  singleChapterRebuild: rebuild.singleChapterRebuild,
  fullCleanRebuild: rebuild.fullRebuild,
  visualInspection: '20/20 passed',
  verificationBoundary: 'Memory, dynamodbtest.Fake, miniredis, API Gateway SDK fake/FakePusher only; no real AWS, DynamoDB Local, Redis or API Gateway network verification.',
  artifacts,
  verifiedAt: new Date().toISOString(),
};
writeJson(path.join(root, 'verification', 'summary.json'), summary);
console.log(`final audit passed: 20 PDFs, 20 ZIPs, ${summary.totalPages} pages, ${summary.executedCommandCount} verification commands`);
