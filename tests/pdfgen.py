"""Tiny PDF writer for test statements: text placed at exact positions, like a bank's PDF.

pages: list of pages; each page a list of (x, y, text) or (x, y, text, "r") for right-aligned
text ending at x. y is measured from the top of an A4 page. Helvetica 9pt.
"""

W = {**{c: 556 for c in "0123456789"}, ",": 278, ".": 278, "-": 333, "(": 333, ")": 333, " ": 278}


def width(text, size=9):
    return sum(W.get(ch, 600) for ch in text) * size / 1000


def _esc(t):
    return t.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def make_pdf(pages, size=9):
    objs = ["<< /Type /Catalog /Pages 2 0 R >>", None,
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"]
    kids = []
    for items in pages:
        ops = []
        for it in items:
            x, y, text = it[:3]
            if len(it) > 3 and it[3] == "r":
                x = x - width(text, size)
            ops.append(f"BT /F1 {size} Tf {x:.2f} {842 - y:.2f} Td ({_esc(text)}) Tj ET")
        stream = "\n".join(ops).encode("latin-1")
        objs.append(f"<< /Length {len(stream)} >>\nstream\n".encode("latin-1") + stream + b"\nendstream")
        content_no = len(objs)
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents {content_no} 0 R "
                    f"/Resources << /Font << /F1 3 0 R >> >> >>")
        kids.append(len(objs))
    objs[1] = f"<< /Type /Pages /Kids [{' '.join(f'{k} 0 R' for k in kids)}] /Count {len(kids)} >>"
    out, offsets = bytearray(b"%PDF-1.4\n"), []
    for i, o in enumerate(objs, 1):
        offsets.append(len(out))
        body = o if isinstance(o, bytes) else o.encode("latin-1")
        out += f"{i} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    for off in offsets:
        out += f"{off:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)


def encrypt(pdf_bytes, password):
    """The same PDF locked with a password, the way banks email e-statements."""
    import io
    from pypdf import PdfReader, PdfWriter
    w = PdfWriter(clone_from=PdfReader(io.BytesIO(pdf_bytes)))
    w.encrypt(password, algorithm="AES-128")
    buf = io.BytesIO(); w.write(buf)
    return buf.getvalue()
