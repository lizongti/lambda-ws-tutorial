import fs from 'node:fs';
import path from 'node:path';
import { marked } from 'marked';
import puppeteer from 'puppeteer-core';
import { root, chapterKey, snapshotDir, safeRelative, assert } from './common.mjs';

function normalized(text) {
  return text.replace(/\r\n/g, '\n');
}

export function validateMarkdown(chapter) {
  const key = chapterKey(chapter);
  const markdownPath = path.join(root, 'docs', `${key}.md`);
  const markdown = fs.readFileSync(markdownPath, 'utf8');
  const snapshot = snapshotDir(chapter);
  const fence = /```([A-Za-z0-9]+)(?:\s+path=([^\s]+))?\n([\s\S]*?)```/g;
  let match;
  let snippetCount = 0;
  while ((match = fence.exec(markdown)) !== null) {
    const [, language, relative, code] = match;
    if (!relative) continue;
    assert(language === 'go', `${key}: non-Go source fence ${relative}`);
    assert(safeRelative(relative), `${key}: unsafe source path ${relative}`);
    assert(!/(?:^|\n)\s*(?:\/\/\s*)?\.\.\.(?:\n|$)|\/\/\s*omitted|\bTODO\b/.test(code), `${key}: placeholder in ${relative}`);
    const sourcePath = path.join(snapshot, relative);
    assert(fs.existsSync(sourcePath), `${key}: missing snippet source ${relative}`);
    const source = fs.readFileSync(sourcePath, 'utf8');
    assert(normalized(source).includes(normalized(code.trimEnd())), `${key}: snippet does not match ${relative}`);
    snippetCount++;
  }
  assert(snippetCount >= 3, `${key}: fewer than three source snippets`);
  return { markdownPath, markdown, snippetCount };
}

function browserPath() {
  if (process.env.TUTORIAL_BROWSER) return process.env.TUTORIAL_BROWSER;
  const candidates = [
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
  ];
  const found = candidates.find(candidate => fs.existsSync(candidate));
  assert(found, 'Microsoft Edge or Chrome is required for PDF rendering');
  return found;
}

function checkPdf(file, key) {
  const contents = fs.readFileSync(file);
  assert(contents.length > 1000, `${key}: PDF is empty or trivial`);
  assert(contents.subarray(0, 5).toString() === '%PDF-', `${key}: invalid PDF header`);
  assert(contents.subarray(Math.max(0, contents.length - 64)).includes(Buffer.from('%%EOF')), `${key}: invalid PDF ending`);
}

export async function renderChapter(chapter) {
  const key = chapterKey(chapter);
  const { markdown, snippetCount } = validateMarkdown(chapter);
  const css = fs.readFileSync(path.join(root, 'docs', 'assets', 'tutorial.css'), 'utf8');
  const body = marked.parse(markdown, { gfm: true, breaks: false });
  const title = `第 ${String(chapter.number).padStart(2, '0')} 节：${chapter.title}`;
  const html = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>${title}</title><style>${css}</style></head><body>${body}</body></html>`;
  const htmlPath = path.join(root, 'work', 'html', `${key}.html`);
  fs.mkdirSync(path.dirname(htmlPath), { recursive: true });
  fs.writeFileSync(htmlPath, html);
  const pdfPath = path.join(root, 'work', 'tmp', `${key}.pdf`);
  fs.mkdirSync(path.dirname(pdfPath), { recursive: true });
  fs.rmSync(pdfPath, { force: true });

  const browser = await puppeteer.launch({ executablePath: browserPath(), headless: true, args: ['--no-sandbox'] });
  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: 'networkidle0' });
    await page.pdf({
      path: pdfPath,
      format: 'A4',
      printBackground: true,
      preferCSSPageSize: true,
      displayHeaderFooter: true,
      headerTemplate: `<div style="width:100%;font-size:8px;color:#526577;padding:0 16mm;text-align:right;font-family:'Microsoft YaHei',sans-serif;">${title}</div>`,
      footerTemplate: '<div style="width:100%;font-size:8px;color:#526577;padding:0 16mm;text-align:center;"><span class="pageNumber"></span> / <span class="totalPages"></span></div>',
      margin: { top: '18mm', right: '16mm', bottom: '20mm', left: '16mm' },
    });
  } finally {
    await browser.close();
  }
  checkPdf(pdfPath, key);
  return { pdfPath, htmlPath, snippetCount };
}

if (process.argv[1] && process.argv[1].endsWith('render.mjs')) {
  const { chapters, chapterByNumber } = await import('./common.mjs');
  const selected = process.argv.includes('--chapter') ? [chapterByNumber(process.argv[process.argv.indexOf('--chapter') + 1])] : chapters;
  for (const chapter of selected) {
    const result = await renderChapter(chapter);
    console.log(`${chapterKey(chapter)}: ${result.pdfPath}`);
  }
}
