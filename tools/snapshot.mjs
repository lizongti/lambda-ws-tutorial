import fs from 'node:fs';
import path from 'node:path';
import { root, walk, copyFile, hashFile, writeJson, run, assert } from './common.mjs';

const sourceRepo = process.argv[2];
if (!sourceRepo) throw new Error('usage: node tools/snapshot.mjs <source-repository>');
const sourceRoot = path.join(root, 'source');
const manifestPath = path.join(root, 'verification', 'source-manifest.json');
assert(!fs.existsSync(manifestPath), 'source snapshot is already initialized; refusing to overwrite it');
assert(walk(sourceRoot).length === 0, 'source directory is not empty; refusing to merge or overwrite it');
assert(fs.existsSync(path.join(sourceRepo, 'go.mod')), 'source repository has no go.mod');

const directories = ['dynamic', 'event', 'http', 'reqresp', 'server', 'sqs', 'ws'];
for (const directory of directories) {
  const from = path.join(sourceRepo, directory);
  assert(fs.existsSync(from), `missing source directory ${directory}`);
  for (const relative of walk(from)) copyFile(path.join(from, relative), path.join(sourceRoot, directory, relative));
}
const testsRoot = path.join(sourceRepo, 'tests');
for (const entry of fs.readdirSync(testsRoot)) {
  if (/^ws.*\.go$/.test(entry)) copyFile(path.join(testsRoot, entry), path.join(sourceRoot, 'tests', entry));
}
for (const file of ['go.mod', 'go.sum']) copyFile(path.join(sourceRepo, file), path.join(sourceRoot, file));

const go = await run('go', ['version'], root);
assert(go.exitCode === 0, 'go version failed');
const files = walk(sourceRoot).map(relative => ({
  path: relative,
  size: fs.statSync(path.join(sourceRoot, relative)).size,
  sha256: hashFile(path.join(sourceRoot, relative)),
}));
writeJson(manifestPath, {
  module: 'github.com/aura-studio/lambda',
  goVersion: go.output.trim(),
  capturedAt: new Date().toISOString(),
  files,
});
console.log(`captured ${files.length} files, including ${files.filter(file => file.path.startsWith('ws/')).length} ws files`);
