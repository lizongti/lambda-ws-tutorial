import fs from 'node:fs';
import path from 'node:path';
import { root, chapters, chapterByNumber, chapterKey, snapshotDir, deliveryDir, walk, hashFile, readJson, writeJson, run, atomicRename, assert } from './common.mjs';
import { renderChapter } from './render.mjs';
import { packChapter, extractZip } from './zip.mjs';

export function verifySourceSnapshot() {
  const manifest = readJson(path.join(root, 'verification', 'source-manifest.json'));
  const source = path.join(root, 'source');
  const actual = walk(source);
  const expected = manifest.files.map(file => file.path).sort();
  assert(JSON.stringify(actual) === JSON.stringify(expected), 'source snapshot file set has changed');
  for (const file of manifest.files) {
    const full = path.join(source, file.path);
    assert(fs.statSync(full).size === file.size && hashFile(full) === file.sha256, `source snapshot changed: ${file.path}`);
  }
  return manifest;
}

export function compareFinalWs(extractedModule) {
  const sourceWs = path.join(root, 'source', 'ws');
  const chapterWs = path.join(extractedModule, 'ws');
  const sourceFiles = walk(sourceWs);
  const chapterFiles = walk(chapterWs);
  const sourceSet = new Set(sourceFiles);
  const chapterSet = new Set(chapterFiles);
  const missing = sourceFiles.filter(file => !chapterSet.has(file));
  const extra = chapterFiles.filter(file => !sourceSet.has(file));
  const different = sourceFiles.filter(file => chapterSet.has(file) && hashFile(path.join(sourceWs, file)) !== hashFile(path.join(chapterWs, file)));
  const report = {
    sourceRoot: 'source/ws',
    chapterRoot: 'work/extract/20-integration/20-integration/ws',
    sourceFileCount: sourceFiles.length,
    chapterFileCount: chapterFiles.length,
    missing,
    extra,
    different,
    equal: missing.length === 0 && extra.length === 0 && different.length === 0,
  };
  writeJson(path.join(root, 'verification', 'final-equivalence.json'), report);
  assert(report.equal, `final ws mismatch: ${JSON.stringify({ missing, extra, different })}`);
  return report;
}

export async function goCommand(directory, args, logFile) {
  const before = ['go.mod', 'go.sum'].map(file => hashFile(path.join(directory, file)));
  const result = await run('go', args, directory, logFile);
  const after = ['go.mod', 'go.sum'].map(file => hashFile(path.join(directory, file)));
  assert(JSON.stringify(before) === JSON.stringify(after), `Go command modified dependency files in ${directory}`);
  if (result.exitCode !== 0) {
    const tail = result.output.split(/\r?\n/).slice(-35).join('\n');
    throw new Error(`${result.command} failed in ${directory}\n${tail}`);
  }
  const { output, ...record } = result;
  return record;
}

export async function buildChapter(chapter) {
  const key = chapterKey(chapter);
  verifySourceSnapshot();
  const snapshot = snapshotDir(chapter);
  const logDir = path.join(root, 'verification', 'chapters', key);
  fs.mkdirSync(logDir, { recursive: true });
  const commands = [];
  commands.push(await goCommand(snapshot, ['test', '-mod=readonly', '-count=1', './...'], path.join(logDir, '01-work-test.log')));

  const rendered = await renderChapter(chapter);
  const packed = await packChapter(chapter);
  const extractRoot = path.join(root, 'work', 'extract', key);
  const entries = await extractZip(packed.zipPath, extractRoot, key);
  const extractedModule = path.join(extractRoot, key);
  commands.push(await goCommand(extractedModule, ['test', '-mod=readonly', '-count=1', './...'], path.join(logDir, '02-extracted-test.log')));
  if (chapter.race) commands.push(await goCommand(extractedModule, ['test', '-mod=readonly', '-race', '-count=1', './...'], path.join(logDir, '03-extracted-race.log')));
  if (chapter.number === 20) {
    commands.push(await goCommand(extractedModule, ['vet', '-mod=readonly', './...'], path.join(logDir, '04-vet.log')));
    commands.push(await goCommand(extractedModule, ['test', '-mod=readonly', '-race', '-count=20', './tests', '-run', 'TestWSIntegrationConcurrentDisconnectReconnectReplayAck|TestWSIntegrationRaceBroadcastAndCleanup'], path.join(logDir, '05-concurrency-repeat.log')));
    compareFinalWs(extractedModule);
  }

  const delivery = deliveryDir(chapter);
  const pdfDestination = path.join(delivery, `${key}.pdf`);
  const zipDestination = path.join(delivery, `${key}.zip`);
  atomicRename(rendered.pdfPath, pdfDestination);
  atomicRename(packed.zipPath, zipDestination);
  const result = {
    number: chapter.number,
    slug: chapter.slug,
    title: chapter.title,
    pdf: `chapters/${key}/${key}.pdf`,
    zip: `chapters/${key}/${key}.zip`,
    snapshot: `work/snapshots/${key}`,
    testCommand: chapter.testCommand,
    race: chapter.race,
    snippetCount: rendered.snippetCount,
    zipEntryCount: entries.length,
    commands,
    pdfSha256: hashFile(pdfDestination),
    zipSha256: hashFile(zipDestination),
    verifiedAt: new Date().toISOString(),
  };
  writeJson(path.join(logDir, 'result.json'), result);
  const manifestPath = path.join(root, 'manifest.json');
  const manifest = fs.existsSync(manifestPath) ? readJson(manifestPath) : { chapters: [] };
  manifest.chapters = manifest.chapters.filter(item => item.number !== chapter.number);
  manifest.chapters.push(result);
  manifest.chapters.sort((a, b) => a.number - b.number);
  writeJson(manifestPath, manifest);
  console.log(`built ${key}: ${commands.length} commands, ${rendered.snippetCount} snippets, ${entries.length} ZIP files`);
  return result;
}

if (process.argv[1] && process.argv[1].endsWith('build.mjs')) {
  let selected = chapters;
  if (process.argv.includes('--chapter')) selected = [chapterByNumber(process.argv[process.argv.indexOf('--chapter') + 1])];
  if (process.argv.includes('--from')) {
    const from = Number(process.argv[process.argv.indexOf('--from') + 1]);
    const to = process.argv.includes('--to') ? Number(process.argv[process.argv.indexOf('--to') + 1]) : from;
    selected = chapters.filter(chapter => chapter.number >= from && chapter.number <= to);
  }
  for (const chapter of selected) await buildChapter(chapter);
}
