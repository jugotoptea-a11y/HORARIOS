import re
import sys
import unicodedata
from pathlib import Path
import pdfplumber
import pandas as pd


BASE_DIR = Path(r"C:\Users\sebas\Downloads\HORARIO\2026")
OUTPUT_CSV = Path(r"C:\Users\sebas\Downloads\HORARIO\horarios_extraidos.csv")

DIAS = [
    "LUNES","MARTES","MIERCOLES","MIÉRCOLES",
    "JUEVES","VIERNES","SABADO","SÁBADO","DOMINGO"
]


LIGATURES = {
    "\ufb01": "fi", "\ufb02": "fl", "\ufb00": "ff",
    "\ufb03": "ffi", "\ufb04": "ffl",
    "\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"',
    "\u2013": "-", "\u2014": "-", "\u00a0": " ",
}


def clean(text):
    if not text:
        return ""
    for lig, repl in LIGATURES.items():
        text = text.replace(lig, repl)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    return " ".join(text.split())


def clean_keep_newlines(text):
    """Limpia caracteres especiales pero conserva saltos de línea."""
    if not text:
        return ""
    for lig, repl in LIGATURES.items():
        text = text.replace(lig, repl)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    return text


def parse_header(text):

    info = {
        "Periodo": "",
        "ID_Estudiante": "",
        "Nombre_Estudiante": "",
    }

    m = re.search(r"Periodo\s*[:\-]?\s*(\d+)", text, re.IGNORECASE)
    if m:
        info["Periodo"] = m.group(1)

    m = re.search(
        r"Estudiante\s*[:\-]?\s*(\d+)\s+([A-ZÁÉÍÓÚÑ\s]+?)(?=\s+HORAS|\n|$)",
        text
    )

    if m:
        info["ID_Estudiante"] = m.group(1)
        info["Nombre_Estudiante"] = clean(m.group(2))

    return info


def parse_cell_text(cell_text):

    if not cell_text:
        return []

    materias = []

    bloques = re.split(r"\n{2,}", cell_text.strip())

    for bloque in bloques:

        lineas = [l.strip() for l in bloque.splitlines() if l.strip()]

        mat = {
            "Prog": "",
            "Codigo_Clase": "",
            "Materia": "",
            "Hora_Inicio": "",
            "Hora_Fin": "",
        }

        nombre_lines = []

        for l in lineas:

            if re.match(r"^Prog\.?\s*", l, re.I):

                mat["Prog"] = re.sub(
                    r"^Prog\.?\s*", "", l, flags=re.I
                ).strip()

            elif "PRESENCIAL-" in l:

                m = re.search(r"PRESENCIAL-(\d+)", l)

                if m:
                    mat["Codigo_Clase"] = m.group(1)

            elif re.search(r"\d{1,2}:\d{2}\s*(am|pm)", l, re.I):

                partes = re.split(r"\s*-\s*", l)

                if len(partes) == 2:

                    mat["Hora_Inicio"] = partes[0].strip()
                    mat["Hora_Fin"] = partes[1].strip()

            elif not re.match(r"(Grupo|SubGrupo|Aula|Cod)", l, re.I):

                nombre_lines.append(l)

        if nombre_lines:

            mat["Materia"] = " ".join(nombre_lines)

        if mat["Materia"] or mat["Codigo_Clase"]:

            materias.append(mat)

    return materias


def extraer_pdf(pdf_path, promocion):

    rows = []

    with pdfplumber.open(pdf_path) as pdf:

        full_text = "\n".join(
            clean_keep_newlines(page.extract_text() or "")
            for page in pdf.pages
        )

        header = parse_header(full_text)

        for page in pdf.pages:

            tables = page.extract_tables()

            if not tables:
                continue

            for table in tables:

                header_row_idx = None
                dias = []

                for idx, row in enumerate(table):

                    row_text = [clean(c or "") for c in row]

                    found = [
                        c for c in row_text
                        if c.upper() in DIAS
                    ]

                    if found:
                        header_row_idx = idx
                        dias = row_text
                        break

                if header_row_idx is None:
                    continue

                for row in table[header_row_idx+1:]:

                    for col_idx, cell in enumerate(row[1:], start=1):

                        dia = dias[col_idx] if col_idx < len(dias) else ""

                        materias = parse_cell_text(clean_keep_newlines(cell or ""))

                        for mat in materias:

                            fila = {
                                "Promocion": promocion,
                                "Periodo": header["Periodo"],
                                "ID_Estudiante": header["ID_Estudiante"],
                                "Nombre_Estudiante": header["Nombre_Estudiante"],
                                "Dia": dia.upper(),
                            }

                            fila.update(mat)

                            rows.append(fila)

    return rows


def main():

    if not BASE_DIR.exists():
        sys.exit("Carpeta no encontrada")

    promocion = BASE_DIR.name

    pdfs = sorted(BASE_DIR.rglob("*.pdf"))

    if not pdfs:
        sys.exit("No se encontraron PDFs")

    print("PDFs encontrados:", len(pdfs))

    all_rows = []

    for pdf_path in pdfs:

        print("Procesando:", pdf_path.name)

        try:

            rows = extraer_pdf(pdf_path, promocion)

            print("  materias:", len(rows))

            all_rows.extend(rows)

        except Exception as e:

            print("  error:", e)

    if not all_rows:
        sys.exit("No se extrajeron datos")

    df = pd.DataFrame(all_rows)

    cols = [
        "Promocion",
        "Periodo",
        "ID_Estudiante",
        "Nombre_Estudiante",
        "Dia",
        "Hora_Inicio",
        "Hora_Fin",
        "Prog",
        "Codigo_Clase",
        "Materia"
    ]

    df = df[[c for c in cols if c in df.columns]]

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print("\nCSV generado:")
    print(OUTPUT_CSV)

    print("\nPrimeras filas:")
    print(df.head())


if __name__ == "__main__":
    main()