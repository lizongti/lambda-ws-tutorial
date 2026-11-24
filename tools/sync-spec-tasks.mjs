import fs from 'node:fs';
import path from 'node:path';
import { root, chapters, chapterKey, writeJson, assert } from './common.mjs';

const specPath = 'W:/github.com/aura-studio/lambda/.kiro/specs/ws-iterative-tutorial/tasks.md';
const lines = fs.readFileSync(specPath, 'utf8').split(/\r?\n/);
const evidence = [];
let top = 0;
for (let index = 0; index < lines.length; index++) {
  const match = lines[index].match(/^(\s*)- \[ \] (\d+(?:\.\d+)?)\.? (.*)$/);
  if (!match) continue;
  if (!match[1]) top = Number(match[2].split('.')[0]);
  const files = [];
  if (top === 1) files.push('tools/snapshot.mjs', 'verification/source-manifest.json', 'verification/self-test.log');
  else if (top === 2) files.push('tools/build.mjs', 'tools/chapters.json', 'verification/self-test.log');
  else if (top === 3) files.push('tools/render.mjs', 'docs/assets/tutorial.css', 'verification/visual-inspection.json', 'verification/self-test.log');
  else if (top === 4) files.push('tools/zip.mjs', 'tools/verify.mjs', 'manifest.json', 'verification/self-test.log');
  else if (top >= 5 && top <= 24) {
    const chapter = chapters[top - 5];
    const key = chapterKey(chapter);
    files.push(`chapters/${key}/${key}.pdf`, `chapters/${key}/${key}.zip`, `verification/chapters/${key}/result.json`, 'verification/visual-inspection.json');
  } else if (top === 25) {
    files.push('verification/chapter-results.json', 'verification/final-equivalence.json', 'verification/visual-inspection.json', 'verification/rebuild-check.json', 'verification/summary.json');
  }
  for (const file of files) assert(fs.existsSync(path.join(root, file)), `task ${match[2]} missing evidence ${file}`);
  if (top >= 5 && top <= 24) {
    const chapter = chapters[top - 5];
    const key = chapterKey(chapter);
    const result = JSON.parse(fs.readFileSync(path.join(root, 'verification', 'chapters', key, 'result.json'), 'utf8'));
    assert(result.commands.every(command => command.exitCode === 0), `task ${match[2]} has failed chapter command`);
  }
  evidence.push({ task: match[2], text: match[3], specLine: index + 1, evidence: files, status: 'passed' });
}
assert(evidence.length === 128, `expected 128 leaf/task evidence entries, got ${evidence.length}`);
const visual = JSON.parse(fs.readFileSync(path.join(root, 'verification', 'visual-inspection.json'), 'utf8'));
assert(visual.chapters.length === 20 && visual.chapters.every(chapter => chapter.visualReview === 'passed'), 'visual review is not complete');
const equivalence = JSON.parse(fs.readFileSync(path.join(root, 'verification', 'final-equivalence.json'), 'utf8'));
assert(equivalence.equal, 'final equivalence failed');
writeJson(path.join(root, 'verification', 'spec-task-evidence.json'), { entries: evidence, verifiedAt: new Date().toISOString() });
const completed = lines.map(line => line.replace(/^(\s*)- \[ \]/, '$1- [x]')).join('\n');
fs.writeFileSync(specPath, completed);
console.log(`checked off ${evidence.length} specification tasks with evidence`);
