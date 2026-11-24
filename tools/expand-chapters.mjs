import fs from 'node:fs';
import path from 'node:path';
import { root, chapters, chapterKey, walk, hashFile, readJson, writeJson, assert } from './common.mjs';
import { extractZip } from './zip.mjs';

const bases = [root, path.join(root, 'work', 'rebuild-check')];
const expanded = [];

function expectedFiles(base, chapter) {
  const snapshot = path.join(base, 'work', 'snapshots', chapterKey(chapter));
  const files = [];
  for (const allowed of chapter.allowlist) {
    const full = path.join(snapshot, allowed);
    if (!fs.existsSync(full)) continue;
    if (fs.statSync(full).isDirectory()) files.push(...walk(full).map(relative => `${allowed}/${relative}`));
    else files.push(allowed);
  }
  return { snapshot, files: [...new Set(files)].sort() };
}

for (const base of bases) {
  for (const chapter of chapters) {
    const key = chapterKey(chapter);
    const zip = path.join(base, 'chapters', key, `${key}.zip`);
    assert(fs.existsSync(zip), `missing ZIP ${zip}`);
    const extractRoot = path.join(base, 'work', 'expanded-delivery', key);
    await extractZip(zip, extractRoot, key);
    const extractedModule = path.join(extractRoot, key);
    const { snapshot, files } = expectedFiles(base, chapter);
    const extractedFiles = walk(extractedModule);
    assert(JSON.stringify(extractedFiles) === JSON.stringify(files), `${key}: extracted file set differs`);
    for (const relative of files) assert(hashFile(path.join(snapshot, relative)) === hashFile(path.join(extractedModule, relative)), `${key}: extracted content differs at ${relative}`);
    expanded.push({ base, chapter, key, zip, extractedModule, files });
  }
}

for (const item of expanded) {
  const destination = path.join(item.base, 'chapters', item.key);
  fs.cpSync(item.extractedModule, destination, { recursive: true });
  for (const relative of item.files) assert(hashFile(path.join(item.extractedModule, relative)) === hashFile(path.join(destination, relative)), `${item.key}: delivery copy differs at ${relative}`);
}
for (const item of expanded) fs.rmSync(item.zip);

for (const base of bases) {
  const manifestPath = path.join(base, 'manifest.json');
  const manifest = readJson(manifestPath);
  for (const entry of manifest.chapters) {
    const key = chapterKey(entry);
    const item = expanded.find(candidate => candidate.base === base && candidate.key === key);
    assert(item, `manifest entry without expansion ${key}`);
    delete entry.zip;
    entry.codeDirectory = `chapters/${key}/`;
    entry.codeFileCount = item.files.length;
    entry.codeExpanded = true;
    entry.expandedFromZipSha256 = entry.zipSha256;
    delete entry.zipSha256;
  }
  writeJson(manifestPath, manifest);
  const summaryPath = path.join(base, 'verification', 'summary.json');
  if (fs.existsSync(summaryPath)) {
    const summary = readJson(summaryPath);
    summary.previousZipCount = summary.zipCount;
    summary.previousTotalZipBytes = summary.totalZipBytes;
    summary.zipCount = 0;
    summary.totalZipBytes = 0;
    summary.codeDirectoryCount = 20;
    summary.zipsExpandedAndRemoved = true;
    summary.expandedAt = new Date().toISOString();
    writeJson(summaryPath, summary);
  }
}

const report = expanded.filter(item => item.base === root).map(item => ({ chapter: item.key, codeDirectory: `chapters/${item.key}/`, fileCount: item.files.length, zipRemoved: true }));
writeJson(path.join(root, 'verification', 'expanded-code.json'), { chapters: report, verifiedAt: new Date().toISOString() });
console.log(`expanded and verified ${expanded.length} archives; removed all chapter ZIP files`);
