import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { root, chapters, copyFile, hashFile, writeFile, safeRelative, assert } from './common.mjs';
import { packageFiles, validateEntryName } from './zip.mjs';
import { validateMarkdown } from './render.mjs';
import { verifySourceSnapshot, goCommand } from './build.mjs';

const fixture = path.join(root, 'work', 'tmp', 'self-test');
fs.rmSync(fixture, { recursive: true, force: true });
fs.mkdirSync(fixture, { recursive: true });

const crlf = Buffer.from('first\r\nsecond\r\n', 'utf8');
writeFile(path.join(fixture, 'source.txt'), crlf);
copyFile(path.join(fixture, 'source.txt'), path.join(fixture, 'copy.txt'));
assert(hashFile(path.join(fixture, 'source.txt')) === hashFile(path.join(fixture, 'copy.txt')), 'CRLF bytes changed during copy');

assert(safeRelative('ws/protocol.go'), 'safe relative path rejected');
for (const unsafe of ['../escape', '/absolute', 'C:/absolute', 'ws\\protocol.go']) assert(!safeRelative(unsafe), `unsafe path accepted: ${unsafe}`);
for (const unsafe of ['../evil', '/evil', 'C:/evil', '01-protocol/../evil']) {
  let rejected = false;
  try { validateEntryName(unsafe, '01-protocol'); } catch { rejected = true; }
  assert(rejected, `unsafe ZIP entry accepted: ${unsafe}`);
}

const secondSnapshot = spawnSync(process.execPath, [path.join(root, 'tools', 'snapshot.mjs'), path.join(root, 'source')], { encoding: 'utf8' });
assert(secondSnapshot.status !== 0 && secondSnapshot.stderr.includes('refusing to overwrite'), 'second source snapshot did not fail safely');
verifySourceSnapshot();

for (const chapter of chapters) {
  const files = packageFiles(chapter);
  assert(files.every(file => safeRelative(file) && !file.startsWith('chapters/') && !file.startsWith('work/')), `${chapter.slug}: invalid package allowlist`);
  const { snippetCount } = validateMarkdown(chapter);
  assert(snippetCount >= 3, `${chapter.slug}: insufficient valid snippets`);
}

const goFixture = path.join(fixture, 'go-failure');
writeFile(path.join(goFixture, 'go.mod'), 'module tutorial.local/failure\n\ngo 1.24.1\n');
writeFile(path.join(goFixture, 'go.sum'), '');
writeFile(path.join(goFixture, 'failure.go'), 'package failure\nfunc Broken() {\n');
let buildFailed = false;
try { await goCommand(goFixture, ['test', '-mod=readonly', '-count=1', './...'], path.join(fixture, 'expected-failure.log')); } catch { buildFailed = true; }
assert(buildFailed, 'broken Go fixture did not return failure');
assert(!fs.existsSync(path.join(goFixture, 'broken.pdf')) && !fs.existsSync(path.join(goFixture, 'broken.zip')), 'failed build produced artifacts');

const message = `self-test passed: byte copy, snapshot refusal, ${chapters.length} allowlists, Markdown snippets, ZIP paths, Go failure propagation`;
writeFile(path.join(root, 'verification', 'self-test.log'), message + '\n');
console.log(message);
fs.rmSync(fixture, { recursive: true, force: true });
