import json
import re
import sys
from pathlib import Path

import fitz

root = Path(__file__).resolve().parent.parent
visual_root = root / "verification" / "visual"
visual_root.mkdir(parents=True, exist_ok=True)
results = []

for pdf_path in sorted((root / "chapters").glob("*/*.pdf")):
    key = pdf_path.parent.name
    document = fitz.open(pdf_path)
    assert document.page_count > 0, f"{key}: no pages"
    page_texts = [page.get_text() for page in document]
    assert all(len(text.strip()) >= 20 for text in page_texts), f"{key}: blank or nearly blank page"
    assert not any("\ufffd" in text for text in page_texts), f"{key}: replacement glyph in extracted text"
    chinese = len(re.findall(r"[\u4e00-\u9fff]", "\n".join(page_texts)))
    assert chinese >= 100, f"{key}: insufficient extractable Chinese text"
    assert "1 /" in page_texts[0], f"{key}: page number missing"
    code_index = max(range(document.page_count), key=lambda index: page_texts[index].count("func ") + page_texts[index].count("type "))
    selected = sorted(set([0, code_index, document.page_count - 1]))
    chapter_visual = visual_root / key
    chapter_visual.mkdir(exist_ok=True)
    rendered = []
    for index in selected:
        pixmap = document[index].get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False)
        output = chapter_visual / f"page-{index + 1:03d}.png"
        pixmap.save(output)
        rendered.append(str(output.relative_to(root)).replace("\\", "/"))

    contact_document = fitz.open()
    contact_page = contact_document.new_page(width=900, height=420 * len(selected) + 30)
    for slot, index in enumerate(selected):
        source_pixmap = document[index].get_pixmap(matrix=fitz.Matrix(1, 1), alpha=False)
        rectangle = fitz.Rect(20, 20 + slot * 420, 880, 420 + slot * 420)
        contact_page.insert_image(rectangle, pixmap=source_pixmap, keep_proportion=True)
    contact_path = visual_root / f"{key}-contact.png"
    contact_page.get_pixmap(matrix=fitz.Matrix(1, 1), alpha=False).save(contact_path)
    contact_document.close()
    results.append({
        "chapter": key,
        "pageCount": document.page_count,
        "chineseCharacterCount": chinese,
        "inspectedPages": [index + 1 for index in selected],
        "renderedPages": rendered,
        "contactSheet": str(contact_path.relative_to(root)).replace("\\", "/"),
        "blankPages": [],
        "replacementGlyphs": 0,
        "visualReview": "pending",
    })
    document.close()
    print(f"{key}: {results[-1]['pageCount']} pages, inspected {results[-1]['inspectedPages']}")

output = root / "verification" / "visual-inspection.json"
output.write_text(json.dumps({"chapters": results}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"wrote {output}")
