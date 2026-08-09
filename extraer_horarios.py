import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
import pdfplumber


BASE_DIR = Path(r"C:\Users\sebas\Downloads\HORARIO\H")
OUTPUT_CSV = Path(r"C:\Users\sebas\Downloads\HORARIO\horarios_extraidos.csv")

DIAS = [
    "LUNES", "MARTES", "MIERCOLES", "JUEVES", "VIERNES",
    "SABADO", "DOMINGO",
]

DIA_MAP = {
    "LUN": "LUNES",
    "MAR": "MARTES",
    "MIE": "MIERCOLES",
    "JUE": "JUEVES",
    "VIE": "VIERNES",
    "SAB": "SABADO",
    "DOM": "DOMINGO",
}

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
    if not text:
        return ""
    for lig, repl in LIGATURES.items():
        text = text.replace(lig, repl)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    return text


def strip_accents(text):
    text = unicodedata.normalize("NFKD", str(text or ""))
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def parse_header(text):
    info = {
        "Periodo": "",
        "ID_Estudiante": "",
        "Nombre_Estudiante": "",
    }

    m = re.search(r"Periodo\s*[:\-]?\s*(\d+)", text, re.IGNORECASE)
    if m:
        info["Periodo"] = m.group(1)

    m = re.search(r"Alumno\s*:\s*([^\n(]+)\(([^)]+)\)", text, re.IGNORECASE)
    if m:
        info["Nombre_Estudiante"] = clean(m.group(1))
        info["ID_Estudiante"] = clean(m.group(2))
        return info

    text_plain = strip_accents(text).upper()
    m = re.search(
        r"Estudiante\s*[:\-]?\s*(\d+)\s+([A-Z\s]+?)(?=\s+HORAS|\n|$)",
        text_plain,
    )
    if m:
        info["ID_Estudiante"] = m.group(1)
        info["Nombre_Estudiante"] = clean(m.group(2))

    return info


def normalizar_dia(dia):
    key = strip_accents(dia).upper().strip()[:3]
    return DIA_MAP.get(key, "")


def es_bive(materia):
    return bool(re.search(r"\bBIVE\b", strip_accents(materia).upper()))


def parse_time_line(line):
    day = r"(?:Lun|Mar|Mi[eé]|Jue|Vie|S[aá]b|Dom)"
    pattern = (
        rf"^(?P<dias>{day}(?:\s*,\s*{day})*)"
        r"\s+(?P<inicio>\d{1,2}:\d{2})\s*-\s*(?P<fin>\d{1,2}:\d{2})"
    )
    match = re.search(pattern, line, re.IGNORECASE)
    if not match:
        return []

    dias = [normalizar_dia(d) for d in re.split(r"\s*,\s*", match.group("dias"))]
    return [
        {
            "Dia": dia,
            "Hora_Inicio": match.group("inicio"),
            "Hora_Fin": match.group("fin"),
        }
        for dia in dias
        if dia
    ]


def parse_new_format(text):
    rows = []
    lines = [clean(line) for line in clean_keep_newlines(text).splitlines()]
    lines = [line for line in lines if line]

    i = 0
    while i < len(lines):
        code_match = re.match(r"Materia\s*:\s*(.+)$", lines[i], re.IGNORECASE)
        if not code_match:
            i += 1
            continue

        codigo = clean(code_match.group(1))
        block = []
        i += 1
        while i < len(lines) and not re.match(r"Materia\s*:", lines[i], re.IGNORECASE):
            block.append(lines[i])
            i += 1

        if not block:
            continue

        materia_lines = []
        docente = ""
        horarios = []

        for line in block:
            parsed_times = parse_time_line(line)
            if parsed_times:
                horarios.extend(parsed_times)
                continue
            if "Grupo " in line:
                docente = clean(line.split("Grupo ", 1)[0]).strip(" |")
                continue
            if docente:
                continue
            if re.search(r"Sin\s+horario|^\d{2}\.\d{2}\.\d{4}|^-$|B\s*\|", line, re.IGNORECASE):
                continue
            materia_lines.append(line)

        materia = clean(" ".join(materia_lines))
        if not materia or es_bive(materia):
            continue

        base = {
            "Codigo_Clase": codigo,
            "Materia": materia,
            "Docente": docente,
            "Prog": "",
        }

        if horarios:
            for horario in horarios:
                row = dict(base)
                row.update(horario)
                rows.append(row)
        else:
            rows.append({
                **base,
                "Dia": "",
                "Hora_Inicio": "",
                "Hora_Fin": "",
            })

    return rows


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
            "Docente": "",
            "Hora_Inicio": "",
            "Hora_Fin": "",
        }
        nombre_lines = []

        for line in lineas:
            if re.match(r"^Prog\.?\s*", line, re.I):
                mat["Prog"] = re.sub(r"^Prog\.?\s*", "", line, flags=re.I).strip()
            elif "PRESENCIAL-" in line:
                m = re.search(r"PRESENCIAL-(\d+)", line)
                if m:
                    mat["Codigo_Clase"] = m.group(1)
            elif re.search(r"\d{1,2}:\d{2}\s*(am|pm)", line, re.I):
                partes = re.split(r"\s*-\s*", line)
                if len(partes) == 2:
                    mat["Hora_Inicio"] = partes[0].strip()
                    mat["Hora_Fin"] = partes[1].strip()
            elif not re.match(r"(Grupo|SubGrupo|Aula|Cod)", line, re.I):
                nombre_lines.append(line)

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
        new_rows = parse_new_format(full_text)
        if new_rows:
            for mat in new_rows:
                fila = {
                    "Promocion": promocion,
                    "Periodo": header["Periodo"],
                    "ID_Estudiante": header["ID_Estudiante"],
                    "Nombre_Estudiante": header["Nombre_Estudiante"],
                }
                fila.update(mat)
                rows.append(fila)
            return rows

        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue

            for table in tables:
                header_row_idx = None
                dias = []

                for idx, row in enumerate(table):
                    row_text = [clean(c or "") for c in row]
                    found = [c for c in row_text if strip_accents(c).upper() in DIAS]
                    if found:
                        header_row_idx = idx
                        dias = row_text
                        break

                if header_row_idx is None:
                    continue

                for row in table[header_row_idx + 1:]:
                    for col_idx, cell in enumerate(row[1:], start=1):
                        dia = normalizar_dia(dias[col_idx]) if col_idx < len(dias) else ""
                        materias = parse_cell_text(clean_keep_newlines(cell or ""))

                        for mat in materias:
                            if es_bive(mat.get("Materia", "")):
                                continue
                            fila = {
                                "Promocion": promocion,
                                "Periodo": header["Periodo"],
                                "ID_Estudiante": header["ID_Estudiante"],
                                "Nombre_Estudiante": header["Nombre_Estudiante"],
                                "Dia": dia,
                            }
                            fila.update(mat)
                            rows.append(fila)

    return rows


def main():
    if not BASE_DIR.exists():
        sys.exit("Carpeta no encontrada")

    all_rows = []

    for promocion_dir in sorted(BASE_DIR.iterdir()):
        if not promocion_dir.is_dir():
            continue

        promocion = promocion_dir.name
        pdfs = sorted(promocion_dir.rglob("*.pdf"))

        if not pdfs:
            print(f"No se encontraron PDFs en {promocion}")
            continue

        print(f"\nPromocion: {promocion}")
        print(f"PDFs encontrados: {len(pdfs)}")

        for pdf_path in pdfs:
            print(f"  Procesando: {pdf_path.name}")
            try:
                rows = extraer_pdf(pdf_path, promocion)
                print(f"    Materias/franjas: {len(rows)}")
                all_rows.extend(rows)
            except Exception as e:
                print(f"    Error: {e}")

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
        "Materia",
        "Docente",
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
