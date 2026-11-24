import fs from 'node:fs';
import path from 'node:path';
import { root, run, writeJson, assert } from './common.mjs';

const destination = path.join(root, 'work', 'rebuild-check');
fs.rmSync(destination, { recursive: true, force: true });
fs.mkdirSync(destination, { recursive: true });
for (const directory of ['source', 'docs']) fs.cpSync(path.join(root, directory), path.join(destination, directory), { recursive: true });
fs.mkdirSync(path.join(destination, 'tools'), { recursive: true });
for (const file of fs.readdirSync(path.join(root, 'tools'))) {
  if (/\.(?:mjs|json|py)$/.test(file)) fs.copyFileSync(path.join(root, 'tools', file), path.join(destination, 'tools', file));
}
for (const directory of ['chapters', 'work/snapshots', 'work/html', 'work/extract', 'work/tmp', 'verification/chapters', 'verification/visual']) {
  fs.mkdirSync(path.join(destination, directory), { recursive: true });
}
fs.copyFileSync(path.join(root, 'verification', 'source-manifest.json'), path.join(destination, 'verification', 'source-manifest.json'));

const commands = [];
async function execute(command, args, cwd = destination) {
  const name = `${String(commands.length + 1).padStart(2, '0')}-${path.basename(command, '.exe')}.log`;
  const result = await run(command, args, cwd, path.join(destination, 'verification', name));
  if (result.exitCode !== 0) throw new Error(`${result.command}\n${result.output.split(/\r?\n/).slice(-30).join('\n')}`);
  const { output, ...record } = result;
  commands.push(record);
}

const npmCli = path.join(path.dirname(process.execPath), 'node_modules', 'npm', 'bin', 'npm-cli.js');
await execute(process.execPath, [npmCli, 'ci', '--no-audit', '--no-fund'], path.join(destination, 'tools'));
await execute(process.execPath, ['tools/make-snapshots.mjs']);
await execute(process.execPath, ['tools/make-docs.mjs']);
await execute(process.execPath, ['tools/self-test.mjs']);
await execute(process.execPath, ['tools/build.mjs', '--chapter', '1']);
await execute(process.execPath, ['tools/build.mjs', '--from', '1', '--to', '20']);
assert(fs.existsSync(path.join(destination, 'verification', 'final-equivalence.json')), 'clean rebuild has no final equivalence report');
const equivalence = JSON.parse(fs.readFileSync(path.join(destination, 'verification', 'final-equivalence.json'), 'utf8'));
assert(equivalence.equal, 'clean rebuild final ws differs');
writeJson(path.join(root, 'verification', 'rebuild-check.json'), {
  workspace: 'work/rebuild-check',
  commands,
  singleChapterRebuild: true,
  fullRebuild: true,
  finalEquivalence: true,
  verifiedAt: new Date().toISOString(),
});
console.log(`clean rebuild passed: ${commands.length} commands, single-chapter and all 20 chapters`);
