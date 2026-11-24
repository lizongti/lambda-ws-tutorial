import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const toolsDir = path.dirname(fileURLToPath(import.meta.url));
export const root = path.resolve(toolsDir, '..');
export const chapters = JSON.parse(fs.readFileSync(path.join(toolsDir, 'chapters.json'), 'utf8'));

export function posix(value) {
  return value.split(path.sep).join('/');
}

export function safeRelative(value) {
  return value && !path.isAbsolute(value) && !/^[A-Za-z]:/.test(value) && !value.includes('\\') && !value.split('/').includes('..');
}

export function walk(directory, base = directory) {
  if (!fs.existsSync(directory)) return [];
  const result = [];
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) result.push(...walk(full, base));
    else if (entry.isFile()) result.push(posix(path.relative(base, full)));
  }
  return result.sort();
}

export function hashFile(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

export function copyFile(source, destination) {
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.copyFileSync(source, destination);
}

export function writeFile(file, contents) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, contents);
}

export function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

export function writeJson(file, value) {
  writeFile(file, JSON.stringify(value, null, 2) + '\n');
}

export function chapterKey(chapter) {
  return `${String(chapter.number).padStart(2, '0')}-${chapter.slug}`;
}

export function chapterByNumber(number) {
  const chapter = chapters.find(item => item.number === Number(number));
  if (!chapter) throw new Error(`unknown chapter ${number}`);
  return chapter;
}

export function snapshotDir(chapter) {
  return path.join(root, 'work', 'snapshots', chapterKey(chapter));
}

export function deliveryDir(chapter) {
  return path.join(root, 'chapters', chapterKey(chapter));
}

export function run(command, args, cwd, logFile) {
  return new Promise(resolve => {
    const started = new Date().toISOString();
    const child = spawn(command, args, { cwd, shell: false, env: { ...process.env, GOTOOLCHAIN: 'local' } });
    let output = '';
    child.stdout.on('data', chunk => { output += chunk; });
    child.stderr.on('data', chunk => { output += chunk; });
    child.on('error', error => {
      output += String(error);
      finish(1);
    });
    child.on('close', code => finish(code ?? 1));
    let finished = false;
    function finish(exitCode) {
      if (finished) return;
      finished = true;
      const record = { command: [command, ...args].join(' '), cwd: posix(path.relative(root, cwd)), startedAt: started, exitCode, log: logFile ? posix(path.relative(root, logFile)) : '', output };
      if (logFile) writeFile(logFile, `$ ${record.command}\n${output}`);
      const { output: ignored, ...withoutOutput } = record;
      resolve({ ...withoutOutput, output });
    }
  });
}

export function assert(condition, message) {
  if (!condition) throw new Error(message);
}

export function atomicRename(temp, destination) {
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.rmSync(destination, { force: true });
  fs.renameSync(temp, destination);
}
