import fs from 'node:fs';
import path from 'node:path';
import { root, chapters, chapterByNumber, chapterKey, deliveryDir, hashFile, readJson, writeJson, assert } from './common.mjs';
import { validateMarkdown } from './render.mjs';
import { extractZip } from './zip.mjs';
import { verifySourceSnapshot, compareFinalWs, goCommand } from './build.mjs';

function checkDeliveredPdf(chapter) {
  const key = chapterKey(chapter);
  const file = path.join(deliveryDir(chapter), `${key}.pdf`);
  const contents = fs.readFileSync(file);
  assert(contents.length > 1000, `${key}: delivered PDF is trivial`);
  assert(contents.subarray(0, 5).toString() === '%PDF-', `${key}: delivered PDF header invalid`);
  assert(contents.subarray(Math.max(0, contents.length - 64)).includes(Buffer.from('%%EOF')), `${key}: delivered PDF ending invalid`);
  return file;
}

async function verifyChapter(chapter) {
  const key = chapterKey(chapter);
  const manifest = readJson(path.join(root, 'manifest.json'));
  const entry = manifest.chapters.find(item => item.number === chapter.number);
  assert(entry, `${key}: missing root manifest entry`);
  const pdf = checkDeliveredPdf(chapter);
  const zip = path.join(deliveryDir(chapter), `${key}.zip`);
  assert(fs.existsSync(zip), `${key}: missing ZIP`);
  assert(hashFile(pdf) === entry.pdfSha256, `${key}: PDF hash differs from manifest`);
  assert(hashFile(zip) === entry.zipSha256, `${key}: ZIP hash differs from manifest`);
  const { snippetCount } = validateMarkdown(chapter);
  const extractRoot = path.join(root, 'work', 'extract', key);
  const entries = await extractZip(zip, extractRoot, key);
  const extractedModule = path.join(extractRoot, key);
  const logDir = path.join(root, 'verification', 'chapters', key);
  const commands = [];
  commands.push(await goCommand(extractedModule, ['test', '-mod=readonly', '-count=1', './...'], path.join(logDir, 'verify-test.log')));
  if (chapter.race) commands.push(await goCommand(extractedModule, ['test', '-mod=readonly', '-race', '-count=1', './...'], path.join(logDir, 'verify-race.log')));
  if (chapter.number === 20) {
    commands.push(await goCommand(extractedModule, ['vet', '-mod=readonly', './...'], path.join(logDir, 'verify-vet.log')));
    commands.push(await goCommand(extractedModule, ['test', '-mod=readonly', '-race', '-count=20', './tests', '-run', 'TestWSIntegrationConcurrentDisconnectReconnectReplayAck|TestWSIntegrationRaceBroadcastAndCleanup'], path.join(logDir, 'verify-concurrency-repeat.log')));
    compareFinalWs(extractedModule);
  }
  return { ...entry, snippetCount, zipEntryCount: entries.length, commands, verifiedAt: new Date().toISOString() };
}

let selected = chapters;
if (process.argv.includes('--chapter')) selected = [chapterByNumber(process.argv[process.argv.indexOf('--chapter') + 1])];
verifySourceSnapshot();
if (selected.length === 20) {
  const files = chapters.flatMap(chapter => {
    const key = chapterKey(chapter);
    return [path.join(deliveryDir(chapter), `${key}.pdf`), path.join(deliveryDir(chapter), `${key}.zip`)];
  });
  assert(files.every(file => fs.existsSync(file)), 'expected exactly 20 delivered PDFs and ZIPs');
  const allDeliveryFiles = chapters.flatMap(chapter => fs.readdirSync(deliveryDir(chapter)).filter(file => file.endsWith('.pdf') || file.endsWith('.zip')));
  assert(allDeliveryFiles.length === 40, `expected 40 chapter artifacts, got ${allDeliveryFiles.length}`);
}
const results = [];
for (const chapter of selected) {
  const result = await verifyChapter(chapter);
  results.push(result);
  console.log(`verified ${chapterKey(chapter)}: ${result.commands.length} commands`);
}
if (selected.length === 20) writeJson(path.join(root, 'verification', 'chapter-results.json'), { verifiedAt: new Date().toISOString(), chapters: results });
else {
  const file = path.join(root, 'verification', 'chapter-results.json');
  const existing = fs.existsSync(file) ? readJson(file) : { chapters: [] };
  for (const result of results) {
    existing.chapters = existing.chapters.filter(item => item.number !== result.number);
    existing.chapters.push(result);
  }
  existing.chapters.sort((a, b) => a.number - b.number);
  existing.verifiedAt = new Date().toISOString();
  writeJson(file, existing);
}
