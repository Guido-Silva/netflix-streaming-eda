"""Convierte informe_tecnico.md a informe_tecnico.pdf usando markdown + xhtml2pdf."""
import re
from io import BytesIO
from pathlib import Path

import markdown
from xhtml2pdf import pisa

SRC = Path(__file__).parent / "informe_tecnico.md"
OUT = Path(__file__).parent / "informe_tecnico.pdf"

# --- leer markdown -----------------------------------------------------------
md_text = SRC.read_text(encoding="utf-8")

# Eliminar etiquetas <img ...> de GitHub sin src local
md_text = re.sub(r'<img\s[^>]*>', '', md_text)
# Eliminar referencias a imágenes locales inexistentes
md_text = re.sub(r'!\[[^\]]*\]\([^)]+\)', '', md_text)

# --- convertir a HTML --------------------------------------------------------
html_body = markdown.markdown(
    md_text,
    extensions=["tables", "fenced_code", "nl2br", "sane_lists"],
)

CSS_STYLES = """
@page {
    size: A4;
    margin: 2cm 1.5cm 2cm 1.5cm;
}
body {
    font-family: Helvetica, Arial, sans-serif;
    font-size: 9.5pt;
    line-height: 1.45;
    color: #1a1a1a;
}
h1 { font-size: 16pt; color: #cc0000; margin-top: 0; }
h2 { font-size: 12pt; color: #1a1a6e; margin-top: 16px; }
h3 { font-size: 10.5pt; color: #1a1a6e; margin-top: 10px; }
h4 { font-size: 9.5pt; font-weight: bold; color: #333; margin-top: 8px; }
table {
    border-collapse: collapse;
    width: 100%;
    font-size: 7pt;
    margin: 5px 0;
    table-layout: fixed;
}
th {
    background-color: #1a1a6e;
    color: #ffffff;
    padding: 1px 3px;
    text-align: left;
    word-wrap: break-word;
}
td {
    padding: 1px 3px;
    border: 1px solid #cccccc;
    word-wrap: break-word;
}
code {
    background-color: #f4f4f4;
    padding: 0px 2px;
    font-size: 7.5pt;
    font-family: Courier, monospace;
}
pre {
    background-color: #f4f4f4;
    border-left: 2px solid #1a1a6e;
    padding: 4px 6px;
    font-size: 7pt;
    font-family: Courier, monospace;
}
blockquote {
    border-left: 3px solid #aaaacc;
    margin: 4px 0;
    padding: 2px 8px;
    color: #444444;
    background-color: #f8f8ff;
    font-size: 8pt;
}
a { color: #1a1a6e; }
hr { border-top: 1px solid #cccccc; margin: 10px 0; }
"""

full_html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Informe Técnico — Netflix EDA</title>
  <style>{CSS_STYLES}</style>
</head>
<body>{html_body}</body>
</html>"""

# --- generar PDF -------------------------------------------------------------
buf = BytesIO()
result = pisa.CreatePDF(full_html.encode("utf-8"), dest=buf, encoding="utf-8")

if result.err:
    print(f"ERROR al generar PDF (código {result.err})")
else:
    OUT.write_bytes(buf.getvalue())
    size_kb = OUT.stat().st_size / 1024
    print(f"PDF generado: {OUT}  ({size_kb:.1f} KB)")
