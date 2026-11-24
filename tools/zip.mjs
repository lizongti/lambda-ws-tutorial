import fs from 'node:fs';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import archiver from 'archiver';
import yauzl from 'yauzl';
import { chapterKey, snapshotDir, walk, safeRelative, assert } from './common.mjs';

export function packageFiles(chapter) {
  const snapshot = snapshotDir(chapter);
  const files = [];
  for (const allowed of chapter.allowlist) {
    const full = path.join(snapshot, allowed);
    assert(fs.existsSync(full), `${chapterKey(chapter)}: allowlist path missing: ${allowed}`);
    const stat = fs.statSync(full);
    if (stat.isDirectory()) files.push(...walk(full).map(relative => `${allowed}/${relative}`));
    else files.push(allowed);
  }
  const unique = [...new Set(files)].sort();
  for (const relative of unique) {
    assert(safeRelative(relative), `${chapterKey(chapter)}: unsafe package path ${relative}`);
    assert(!/(^|\/)(?:\.git|\.kiro|\.vscode|node_modules|work|chapters)(?:\/|$)/.test(relative), `${chapterKey(chapter)}: excluded path ${relative}`);
    assert(!/(^|\/)(?:\.env|credentials(?:\.json)?|id_rsa)(?:$|\/)/i.test(relative), `${chapterKey(chapter)}: sensitive file ${relative}`);
    if (/\.(?:go|mod|sum|md)$/.test(relative)) {
      const contents = fs.readFileSync(path.join(snapshot, relative), 'utf8');
      assert(!/(?:[A-Za-z]:\\\\(?:github|Users)\\\\|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\bAKIA[0-9A-Z]{16}\b)/.test(contents), `${chapterKey(chapter)}: machine path or secret in ${relative}`);
    }
  }
  return unique;
}

export async function packChapter(chapter) {
  const key = chapterKey(chapter);
  const snapshot = snapshotDir(chapter);
  const files = packageFiles(chapter);
  const destination = path.join(snapshot, '..', '..', 'tmp', `${key}.zip`);
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.rmSync(destination, { force: true });
  const output = fs.createWriteStream(destination);
  const archive = archiver('zip', { zlib: { level: 9 } });
  const completed = new Promise((resolve, reject) => {
    output.on('close', resolve);
    output.on('error', reject);
    archive.on('error', reject);
  });
  archive.pipe(output);
  for (const relative of files) archive.file(path.join(snapshot, relative), { name: `${key}/${relative}` });
  await archive.finalize();
  await completed;
  assert(fs.statSync(destination).size > 500, `${key}: ZIP is empty or trivial`);
  return { zipPath: destination, files };
}

export function validateEntryName(entryName, expectedRoot) {
  assert(entryName && !entryName.includes('\\') && !path.isAbsolute(entryName) && !/^[A-Za-z]:/.test(entryName), `unsafe ZIP entry ${entryName}`);
  const normalized = entryName.replace(/\/+$/, '');
  assert(safeRelative(normalized), `unsafe ZIP entry ${entryName}`);
  assert(normalized === expectedRoot || normalized.startsWith(expectedRoot + '/'), `unexpected ZIP root ${entryName}`);
}

export async function extractZip(zipPath, destination, expectedRoot) {
  fs.rmSync(destination, { recursive: true, force: true });
  fs.mkdirSync(destination, { recursive: true });
  const zip = await new Promise((resolve, reject) => yauzl.open(zipPath, { lazyEntries: true, autoClose: true, decodeStrings: true }, (error, value) => error ? reject(error) : resolve(value)));
  const names = new Set();
  await new Promise((resolve, reject) => {
    zip.on('error', reject);
    zip.on('entry', async entry => {
      try {
        validateEntryName(entry.fileName, expectedRoot);
        const normalized = entry.fileName.replace(/\/+$/, '');
        assert(!names.has(normalized), `duplicate ZIP entry ${normalized}`);
        names.add(normalized);
        const target = path.join(destination, ...normalized.split('/'));
        if (/\/$/.test(entry.fileName)) {
          fs.mkdirSync(target, { recursive: true });
          zip.readEntry();
          return;
        }
        fs.mkdirSync(path.dirname(target), { recursive: true });
        const input = await new Promise((res, rej) => zip.openReadStream(entry, (error, stream) => error ? rej(error) : res(stream)));
        await pipeline(input, fs.createWriteStream(target));
        zip.readEntry();
      } catch (error) {
        reject(error);
      }
    });
    zip.on('end', resolve);
    zip.readEntry();
  });
  return [...names].filter(name => name !== expectedRoot).sort();
}
