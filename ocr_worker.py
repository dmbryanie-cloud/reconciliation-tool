"""Reads a scanned PDF statement by OCR, in a process of its own: the models and page images take memory
the web app shouldn't keep, and a crash here can't take the app down. Everything stays on this server.

Input: the PDF's bytes on stdin; its password, if any, in the PDF_PASSWORD environment variable.
Output: JSON on stdout -- a list per page of words {text, x0, x1, top, bottom}, in PDF points from the
page's top left (pdfplumber's shape). Progress lines go to stderr. Exit 3: OCR isn't installed.
"""
import json
import os
import sys

OCR_SCALE = 3          # pages are read at 216 dpi: small print on a scan stays legible
ZERO_LOOKALIKES = ("U", "O", "o", "D")   # a lone zero in a money column, misread as a letter


def main():
    try:
        import numpy as np
        import pypdfium2 as pdfium
        from onnxtr.models import ocr_predictor
    except Exception as e:
        print(f"OCR isn't installed: {e}", file=sys.stderr)
        sys.exit(3)
    data = sys.stdin.buffer.read()
    doc = pdfium.PdfDocument(data, password=os.environ.get("PDF_PASSWORD") or None)
    model = ocr_predictor(det_arch="db_mobilenet_v3_large", reco_arch="crnn_mobilenet_v3_small",
                          assume_straight_pages=True, detect_orientation=False)
    pages = []
    for i in range(len(doc)):
        print(f"page {i + 1} of {len(doc)}", file=sys.stderr, flush=True)
        page = doc[i]
        w, h = page.get_size()
        img = np.asarray(page.render(scale=OCR_SCALE).to_pil().convert("RGB"))
        words = []
        for b in model([img]).pages[0].blocks:
            for ln in b.lines:
                for wd in ln.words:
                    (x0, y0), (x1, y1) = wd.geometry
                    t = wd.value.strip()
                    if t:
                        words.append({"text": "0" if t in ZERO_LOOKALIKES else t,
                                      "x0": x0 * w, "x1": x1 * w, "top": y0 * h, "bottom": y1 * h})
        pages.append(words)
    json.dump(pages, sys.stdout)


if __name__ == "__main__":
    main()
