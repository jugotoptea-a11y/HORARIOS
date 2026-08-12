import os
import uuid
import calendar
import re
import unicodedata
from io import BytesIO
from urllib.parse import urlencode
from flask import Flask, render_template, request, jsonify, redirect, url_for, send_file
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, inspect, text
import pandas as pd
import requests
from datetime import datetime, timedelta
from threading import Lock

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ruta_csv = os.path.join(BASE_DIR, "horarios_extraidos.csv")
ruta_excel_local = os.path.join(BASE_DIR, "Informaci\u00f3n.xlsx")

# Configuración opcional para Excel en la nube
# URL fijada por defecto (puedes sobrescribirla con la variable de entorno EXCEL_URL)
EXCEL_URL = os.environ.get("EXCEL_URL", "https://universidaddelacosta-my.sharepoint.com/:x:/g/personal/sbarriosb_cuc_edu_co/IQCQInUk0TAsRKREO6BIYHEWAYTOW10Tw65VVjKnMc63Xkw?e=pZiwUW")  # URL pública o pre-signed para descargar el .xlsx
STUDENTS_SHEET_NAME = os.environ.get("STUDENTS_SHEET_NAME", "General")
STAFF_EVENTS_CSV = os.path.join(BASE_DIR, "staff_eventos.csv")
SYNC_TOKEN = os.environ.get("DASHBOARD_SYNC_TOKEN", "").strip()
STUDENTS_DB_PATH = os.path.join(BASE_DIR, "app.db")


def _normalize_database_uri(raw):
    raw = (raw or "").strip()
    if not raw:
        return ""
    if raw.startswith("postgres://"):
        return raw.replace("postgres://", "postgresql+psycopg://", 1)
    if raw.startswith("postgresql://"):
        return raw.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw


SERVICES_DATABASE_URL = _normalize_database_uri(os.environ.get("DATABASE_URL"))

app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{STUDENTS_DB_PATH}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
services_engine = create_engine(SERVICES_DATABASE_URL, pool_pre_ping=True, future=True) if SERVICES_DATABASE_URL else None


class Student(db.Model):
    __tablename__ = "students"

    documento = db.Column(db.String(64), primary_key=True)
    nombre_completo = db.Column(db.String(255), nullable=False)
    nombre_norm = db.Column(db.String(255), nullable=False, index=True)
    promo = db.Column(db.String(64), nullable=True, default="")
    correo = db.Column(db.String(255), nullable=True, default="")
    contacto = db.Column(db.String(64), nullable=True, default="")
    municipio = db.Column(db.String(255), nullable=True, default="")
    programa = db.Column(db.String(255), nullable=True, default="")
    actualizado_en = db.Column(db.String(32), nullable=True, default="")


_students_db_ready = False
_students_excel_mtime = None
_students_synced = False
_services_db_ready = False


def ensure_students_db():
    global _students_db_ready
    if _students_db_ready:
        return
    db.create_all()
    sync_students_from_excel()
    if services_engine is None:
        _migrar_eventos_db_a_csv_si_aplica()
    _students_db_ready = True


# Cache para evitar recargar CSV en cada petición
_cached_df = None
_cached_mtime = None
_cache_lock = Lock()


def generar_horas():

    inicio = datetime.strptime("06:00", "%H:%M")
    fin = datetime.strptime("22:00", "%H:%M")

    horas = []

    while inicio <= fin:

        horas.append(inicio.strftime("%I:%M %p"))
        inicio += timedelta(minutes=30)

    return horas

# Generar horas una vez (constante)
HORAS = generar_horas()


def _titulo_nombre(token):
    return str(token).lower().capitalize()


def _tomar_apellido(tokens, start):
    particulas = {"DE", "DEL", "LA", "LAS", "LOS"}
    apellido = []
    idx = start
    while idx < len(tokens) and tokens[idx] in particulas:
        apellido.append(tokens[idx])
        idx += 1
    if idx < len(tokens):
        apellido.append(tokens[idx])
        idx += 1
    return apellido, idx


def _formatear_nombre_becado(nombre):
    raw = _safe_text(nombre)
    if not raw:
        return ""
    tokens = raw.split()
    # Mantener el orden original tal como aparece en el PDF,
    # solo aplicar capitalización correcta si el texto está en mayúsculas.
    if raw.upper() == raw:
        return " ".join(_titulo_nombre(t) for t in tokens)
    return raw


def convertir_24(hora):
    """Convierte una hora en formato 12h o 24h a minutos desde medianoche (int).
    Retorna None si no se puede parsear.
    """
    if not hora or not str(hora).strip():
        return None
    s = str(hora).strip().upper()
    # Intentar formatos comunes: '6:00 AM' o '06:00'
    try:
        dt = datetime.strptime(s, "%I:%M %p")
    except Exception:
        try:
            dt = datetime.strptime(s, "%H:%M")
        except Exception:
            return None
    return dt.hour * 60 + dt.minute


def _series_col(df, col):
    if col in df.columns:
        return df[col]
    return pd.Series([""] * len(df), index=df.index)


def _parse_horas_series(series):
    s = series.astype(str).str.strip().str.upper()
    parsed_12h = pd.to_datetime(s, format="%I:%M %p", errors="coerce")
    parsed_24h = pd.to_datetime(s, format="%H:%M", errors="coerce")
    return parsed_12h.fillna(parsed_24h)


def cargar():

    global _cached_df, _cached_mtime
    try:
        mtime = os.path.getmtime(ruta_csv)
    except OSError:
        # Archivo no encontrado; devolver DataFrame vacío
        return pd.DataFrame()

    with _cache_lock:
        if _cached_df is not None and _cached_mtime == mtime:
            return _cached_df

        df = pd.read_csv(ruta_csv, encoding="utf-8-sig")

        # Normalizar y parsear horas (vectorizado)
        inicio_dt = _parse_horas_series(_series_col(df, "Hora_Inicio"))
        fin_dt = _parse_horas_series(_series_col(df, "Hora_Fin"))

        df["_horas_validas"] = inicio_dt.notna() & fin_dt.notna()

        # Mantener columnas legibles para la UI y añadir columnas numéricas para comparar
        df["Hora_Inicio"] = inicio_dt.dt.strftime("%H:%M")
        df["Hora_Fin"] = fin_dt.dt.strftime("%H:%M")

        df["Hora_Inicio_min"] = (inicio_dt.dt.hour * 60 + inicio_dt.dt.minute)
        df["Hora_Fin_min"] = (fin_dt.dt.hour * 60 + fin_dt.dt.minute)

        df["Dia"] = df.get("Dia", "").astype(str).str.strip().str.upper()
        if "Nombre_Estudiante" in df.columns:
            df["Nombre_Estudiante"] = df["Nombre_Estudiante"].apply(_formatear_nombre_becado)

        _cached_df = df
        _cached_mtime = mtime

        return _cached_df


# ---------------------- Funciones para Excel en la nube ----------------------
def download_excel_bytes():
    """Descarga el archivo .xlsx desde EXCEL_URL (si está configurado) y retorna BytesIO.
    Devuelve None si no está configurado o falla la descarga.
    """
    if not EXCEL_URL:
        return None
    try:
        r = requests.get(EXCEL_URL, timeout=30)
        r.raise_for_status()
        return BytesIO(r.content)
    except Exception as e:
        app.logger.warning("No se pudo descargar Excel desde EXCEL_URL: %s", e)
        return None


def _normalizar_lookup(value):
    text_value = "" if pd.isna(value) else str(value)
    text_value = unicodedata.normalize("NFKD", text_value)
    text_value = "".join(ch for ch in text_value if not unicodedata.combining(ch))
    text_value = re.sub(r"\s+", " ", text_value).strip().lower()
    return text_value


def _dedupe_columns(columns):
    result = []
    seen = {}
    for idx, col in enumerate(columns):
        name = "" if pd.isna(col) else str(col).strip()
        if not name or name.lower().startswith("unnamed"):
            name = f"col_{idx}"
        count = seen.get(name, 0)
        seen[name] = count + 1
        result.append(name if count == 0 else f"{name}_{count + 1}")
    return result


def _detect_header_row(raw):
    for idx, row in raw.head(20).iterrows():
        labels = [_normalizar_lookup(value) for value in row.tolist()]
        has_id = any(label in {"id", "documento", "identificacion"} for label in labels)
        has_name = any("nombre" in label for label in labels)
        has_contact = any(label in {"correo", "email", "contacto", "telefono", "celular"} for label in labels)
        if has_id and has_name and has_contact:
            return idx
    return 0


def _read_excel_flexible(source, preferred_sheet=None):
    xls = pd.ExcelFile(source)
    sheet = preferred_sheet if preferred_sheet in xls.sheet_names else xls.sheet_names[0]
    raw = xls.parse(sheet_name=sheet, header=None, dtype=str)
    if raw.empty:
        return pd.DataFrame()

    header_idx = _detect_header_row(raw)
    df = raw.iloc[header_idx + 1:].copy()
    df.columns = _dedupe_columns(raw.iloc[header_idx].tolist())
    df = df.dropna(how="all")
    return df


def read_cloud_general_df():
    """Lee la hoja 'General' del Excel en la nube y la devuelve como DataFrame.
    Si no está disponible retorna DataFrame vacío.
    """
    if os.path.exists(ruta_excel_local):
        try:
            return _read_excel_flexible(ruta_excel_local, STUDENTS_SHEET_NAME)
        except Exception as e:
            app.logger.warning("Error leyendo Excel local '%s': %s", ruta_excel_local, e)

    b = download_excel_bytes()
    if b is None:
        return pd.DataFrame()
    try:
        return _read_excel_flexible(b, STUDENTS_SHEET_NAME)
    except Exception as e:
        app.logger.warning("Error leyendo hoja '%s' del Excel: %s", STUDENTS_SHEET_NAME, e)
        return pd.DataFrame()


def _clean_text_value(value):
    if value is None or pd.isna(value):
        return ""
    value = str(value).strip()
    if value.lower() in {"nan", "none", "nat"}:
        return ""
    return value


def _clean_identifier(value):
    value = _clean_text_value(value)
    if re.fullmatch(r"\d+\.0", value):
        return value[:-2]
    return value


def _find_column(df, candidates):
    """Busca en df una columna que coincida con cualquiera de 'candidates' (lista de nombres posibles).
    Retorna el nombre de columna encontrado o None.
    """
    if df is None or df.columns is None:
        return None
    cols = list(df.columns)
    # coincidencia exacta (case-insensitive)
    lowered = {_normalizar_lookup(c): c for c in cols}
    for cand in candidates:
        if cand is None:
            continue
        key = _normalizar_lookup(cand)
        if key in lowered:
            return lowered[key]
    # buscar por inclusión
    for col in cols:
        col_l = _normalizar_lookup(col)
        for cand in candidates:
            if cand and _normalizar_lookup(cand) in col_l:
                return col
    return None


def _students_dataframe_from_excel():
    if not os.path.exists(ruta_excel_local):
        return pd.DataFrame()
    try:
        return _read_excel_flexible(ruta_excel_local, STUDENTS_SHEET_NAME)
    except Exception as e:
        app.logger.warning("No se pudo leer %s para sincronizar estudiantes: %s", ruta_excel_local, e)
        return pd.DataFrame()


def _student_payloads_from_dataframe(df):
    if df.empty:
        return []

    prom_col = _find_column(df, ["prom", "promocion", "promo", "promoción"])
    id_col = _find_column(df, ["id", "id_estudiante", "documento", "identificacion", "identificación"])
    nombre_col = _find_column(df, ["nombres y apellidos", "nombre y apellidos", "nombres", "nombre"])
    correo_col = _find_column(df, ["correo", "email", "e-mail", "correo_electronico", "correo electrónico"])
    contacto_col = _find_column(df, ["contacto", "telefono", "teléfono", "celular", "cel"])
    municipio_col = _find_column(df, ["municipio", "ciudad"])
    programa_col = _find_column(df, ["programa", "carrera"])

    if not id_col or not nombre_col:
        app.logger.warning("El Excel de estudiantes no tiene columnas suficientes: documento=%s nombre=%s", id_col, nombre_col)
        return []

    payloads = []
    for _, row in df.iterrows():
        documento = _clean_identifier(row.get(id_col))
        nombre = _clean_text_value(row.get(nombre_col))
        if not documento or not nombre:
            continue
        payloads.append({
            "documento": documento,
            "nombre_completo": nombre,
            "nombre_norm": _normalizar_lookup(nombre),
            "promo": _clean_identifier(row.get(prom_col)) if prom_col else "",
            "correo": _clean_text_value(row.get(correo_col)) if correo_col else "",
            "contacto": _clean_identifier(row.get(contacto_col)) if contacto_col else "",
            "municipio": _clean_text_value(row.get(municipio_col)) if municipio_col else "",
            "programa": _clean_text_value(row.get(programa_col)) if programa_col else "",
        })
    return payloads


def sync_students_from_excel(force=False):
    """Sincroniza Información.xlsx hacia app.db. Si el Excel no existe, la app usa lo que ya tenga la DB."""
    global _students_excel_mtime, _students_synced

    if not os.path.exists(ruta_excel_local):
        _students_synced = True
        return 0

    mtime = os.path.getmtime(ruta_excel_local)
    if not force and _students_synced and _students_excel_mtime == mtime:
        return 0

    payloads = _student_payloads_from_dataframe(_students_dataframe_from_excel())
    now_value = datetime.now().isoformat(timespec="seconds")
    seen_docs = set()

    for payload in payloads:
        seen_docs.add(payload["documento"])
        student = Student.query.get(payload["documento"])
        if student is None:
            student = Student(documento=payload["documento"])
            db.session.add(student)
        student.nombre_completo = payload["nombre_completo"]
        student.nombre_norm = payload["nombre_norm"]
        student.promo = payload["promo"]
        student.correo = payload["correo"]
        student.contacto = payload["contacto"]
        student.municipio = payload["municipio"]
        student.programa = payload["programa"]
        student.actualizado_en = now_value

    if seen_docs:
        for student in Student.query.filter(~Student.documento.in_(seen_docs)).all():
            db.session.delete(student)

    db.session.commit()
    _students_excel_mtime = mtime
    _students_synced = True
    app.logger.info("Sincronizados %s estudiantes desde Información.xlsx hacia app.db.", len(payloads))
    return len(payloads)


def _get_student_info_by_names_from_excel_unused(names):
    """Devuelve un dict mapping nombre_original -> info dict (PROM, ID, NOMBRE Y APELLIDOS, CORREO, CONTACTO).
    Usa la hoja 'General' del Excel en la nube. Si no encuentra, rellena con valores vacíos.
    """
    gen = read_cloud_general_df()
    result = {}
    if gen.empty:
        for n in names:
            result[n] = {"PROM": "", "ID": "", "NOMBRE Y APELLIDOS": n, "CORREO": "", "CONTACTO": ""}
        return result

    # Mapear columnas probables
    prom_col = _find_column(gen, ["prom", "promocion", "promo", "promoción"])
    id_col = _find_column(gen, ["id", "id_estudiante", "documento", "identificacion"])
    nombre_col = _find_column(gen, ["nombre", "nombre y apellidos", "nombres apellidos", "nombres"])
    correo_col = _find_column(gen, ["correo", "email", "e-mail", "correo_electronico"])
    contacto_col = _find_column(gen, ["contacto", "telefono", "teléfono", "celular", "cel"])

    # Indexar por nombre normalizado
    index_map = {}
    if nombre_col is None:
        # no hay columna nombre; devolver vacíos
        for n in names:
            result[n] = {"PROM": "", "ID": "", "NOMBRE Y APELLIDOS": n, "CORREO": "", "CONTACTO": ""}
        return result

    for _, row in gen.iterrows():
        nm = str(row.get(nombre_col, "")).strip()
        if not nm:
            continue
        index_map[_normalizar_lookup(nm)] = row

    for n in names:
        key = str(n).strip()
        row = None
        lookup_key = _normalizar_lookup(key)
        # búsqueda exacta
        if lookup_key in index_map:
            row = index_map[lookup_key]
        else:
            # búsqueda por inclusión (parcial)
            for k, r in index_map.items():
                if lookup_key in k or k in lookup_key:
                    row = r
                    break

        if row is None:
            result[n] = {"PROM": "", "ID": "", "NOMBRE Y APELLIDOS": key, "CORREO": "", "CONTACTO": ""}
        else:
            result[n] = {
                "PROM": str(row.get(prom_col, "")) if prom_col else "",
                "ID": str(row.get(id_col, "")) if id_col else "",
                "NOMBRE Y APELLIDOS": str(row.get(nombre_col, key)),
                "CORREO": str(row.get(correo_col, "")) if correo_col else "",
                "CONTACTO": str(row.get(contacto_col, "")) if contacto_col else "",
            }

    return result
# NOTA: edición/subida automática del Excel remoto deshabilitada por petición del usuario.
# Se conservan funciones de lectura del Excel remoto (si EXCEL_URL está configurada).


def get_student_info_by_names(names):
    """Devuelve datos maestros desde app.db, cruzando por nombre normalizado.

    Estrategia de matching (en orden de prioridad):
    1. Coincidencia exacta por nombre_norm.
    2. Substring bidireccional.
    3. Bag-of-words: mismas palabras sin importar el orden
       (maneja casos como "Arias Marianella Montenegro" vs "Marianella Montenegro Arias").
    """
    ensure_students_db()
    result = {}
    clean_names = [_safe_text(n) for n in names if _safe_text(n)]
    lookup_keys = {_normalizar_lookup(n) for n in clean_names}
    students = Student.query.filter(Student.nombre_norm.in_(lookup_keys)).all() if lookup_keys else []
    exact_map = {student.nombre_norm: student for student in students}
    all_students = None

    for n in clean_names:
        key = _safe_text(n)
        lookup_key = _normalizar_lookup(key)
        student = exact_map.get(lookup_key)

        if student is None:
            if all_students is None:
                all_students = Student.query.all()
            lookup_words = set(lookup_key.split()) if lookup_key else set()
            for candidate in all_students:
                candidate_key = _safe_text(candidate.nombre_norm)
                # Paso 2: substring bidireccional
                if lookup_key and (lookup_key in candidate_key or candidate_key in lookup_key):
                    student = candidate
                    break
                # Paso 3: bag-of-words (mismo conjunto de palabras, sin importar orden)
                if lookup_words and lookup_words == set(candidate_key.split()):
                    student = candidate
                    break

        if student is None:
            result[n] = {"PROM": "", "ID": "", "NOMBRE Y APELLIDOS": key, "CORREO": "", "CONTACTO": ""}
        else:
            result[n] = {
                "PROM": _safe_text(student.promo),
                "ID": _safe_text(student.documento),
                "NOMBRE Y APELLIDOS": _safe_text(student.nombre_completo) or key,
                "CORREO": _safe_text(student.correo),
                "CONTACTO": _safe_text(student.contacto),
            }

    return result


def buscar_disponibles(df, dias, inicio, fin, estudiantes_seleccionados):
    inicio_min = convertir_24(inicio)
    fin_min = convertir_24(fin)

    # Si no hay hora inicio/fin, usar valores por defecto (en minutos)
    if inicio_min is None:
        inicio_min = 6 * 60
    if fin_min is None:
        fin_min = 22 * 60

    if estudiantes_seleccionados:
        df = df[df["Nombre_Estudiante"].isin(estudiantes_seleccionados)]

    todos = set(df["Nombre_Estudiante"].dropna())

    # Solo usar filas con horas válidas para determinar quién está ocupado
    df_valido = df[df["_horas_validas"] == True]

    ocupados = df_valido[
        (df_valido["Dia"].isin(dias)) &
        ~((df_valido["Hora_Fin_min"] <= inicio_min) | (df_valido["Hora_Inicio_min"] >= fin_min))
    ]

    ocupados_set = set(ocupados["Nombre_Estudiante"].dropna())

    libres = sorted(todos - ocupados_set)

    return libres


def buscar_no_disponibles(df, dias, inicio, fin, estudiantes_seleccionados):
    """Antidisponibilidad: retorna estudiantes que TIENEN clase en el bloque indicado."""

    inicio_min = convertir_24(inicio)
    fin_min = convertir_24(fin)

    if inicio_min is None:
        inicio_min = 6 * 60
    if fin_min is None:
        fin_min = 22 * 60

    if estudiantes_seleccionados:
        df = df[df["Nombre_Estudiante"].isin(estudiantes_seleccionados)]

    df_valido = df[df["_horas_validas"] == True]

    ocupados = df_valido[
        (df_valido["Dia"].isin(dias)) &
        ~((df_valido["Hora_Fin_min"] <= inicio_min) | (df_valido["Hora_Inicio_min"] >= fin_min))
    ]

    ocupados_set = sorted(set(ocupados["Nombre_Estudiante"].dropna()))

    return ocupados_set


def construir_info_estudiantes(df):
    """Retorna un dict {Nombre_Estudiante: {id, promo, correo, contacto}}."""
    info = {}
    for _, row in df.drop_duplicates(subset="Nombre_Estudiante").iterrows():
        nombre = row["Nombre_Estudiante"]
        try:
            doc = str(int(row["ID_Estudiante"]))
        except (ValueError, TypeError):
            doc = str(row["ID_Estudiante"]) if pd.notna(row["ID_Estudiante"]) else ""
        promo = str(row["Promocion"]) if pd.notna(row.get("Promocion", None)) else ""
        info[nombre] = {"id": doc, "promo": promo, "correo": "", "contacto": ""}

    excel_info = get_student_info_by_names(info.keys())
    for nombre, data in info.items():
        extra = excel_info.get(nombre, {})
        data["id"] = str(extra.get("ID") or data.get("id") or "")
        data["promo"] = str(extra.get("PROM") or data.get("promo") or "")
        data["correo"] = str(extra.get("CORREO") or "")
        data["contacto"] = str(extra.get("CONTACTO") or "")
    return info


COLORES = [
    "#3a7afe", "#e74c3c", "#2ecc71", "#f39c12", "#9b59b6",
    "#1abc9c", "#e67e22", "#2980b9", "#c0392b", "#27ae60",
    "#8e44ad", "#16a085", "#d35400", "#2c3e50", "#f1c40f",
]

ESTADOS_ASISTENCIA = {"pendiente", "asistio", "excusa", "no"}


def _safe_text(value):
    if value is None:
        return ""
    return str(value).strip()


def _json_error(message, status=400):
    return jsonify({"ok": False, "error": message}), status


def _sync_authorized():
    if not SYNC_TOKEN:
        return True
    auth_header = request.headers.get("Authorization", "")
    bearer = auth_header.replace("Bearer ", "", 1).strip() if auth_header.startswith("Bearer ") else ""
    provided = (
        bearer
        or request.headers.get("X-Dashboard-Token", "").strip()
        or request.args.get("token", "").strip()
    )
    return provided == SYNC_TOKEN


def _normalizar_estado_asistencia(value):
    estado = _safe_text(value).lower()
    if not estado:
        return "pendiente"

    alias = {
        "n": "no",
        "no": "no",
        "no_fue": "no",
        "no fue": "no",
        "nofue": "no",
    }
    estado = alias.get(estado, estado)
    if estado not in ESTADOS_ASISTENCIA:
        return "pendiente"
    return estado


def _parse_month_key(month_key):
    try:
        return datetime.strptime(month_key, "%Y-%m")
    except Exception:
        return datetime.now().replace(day=1)


def _shift_month(month_key, delta):
    base = _parse_month_key(month_key)
    year = base.year
    month = base.month + delta
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    return f"{year:04d}-{month:02d}"


def _split_promociones(texto):
    raw = _safe_text(texto)
    if not raw:
        return []
    return [p.strip() for p in raw.split("|") if p.strip()]


def _eventos_a_filas_csv(events):
    filas = []
    for ev in events:
        base = {
            "event_id": _safe_text(ev.get("id")),
            "nombre": _safe_text(ev.get("nombre")),
            "fecha": _safe_text(ev.get("fecha")),
            "hora_inicio": _safe_text(ev.get("hora_inicio")),
            "hora_fin": _safe_text(ev.get("hora_fin")),
            "promociones": "|".join([_safe_text(p) for p in ev.get("promociones", []) if _safe_text(p)]),
            "comentarios": _safe_text(ev.get("comentarios")),
            "creado_en": _safe_text(ev.get("creado_en")),
        }
        staff = ev.get("staff", []) or []
        if not staff:
            filas.append({
                **base,
                "staff_nombre": "",
                "staff_id": "",
                "staff_promo": "",
                "staff_estado": "",
                "staff_nota": "",
            })
            continue
        for st in staff:
            filas.append({
                **base,
                "staff_nombre": _safe_text(st.get("nombre")),
                "staff_id": _safe_text(st.get("id")),
                "staff_promo": _safe_text(st.get("promo")),
                "staff_estado": _safe_text(st.get("estado")),
                "staff_nota": _safe_text(st.get("nota")),
            })
    return filas


def _filas_csv_a_eventos(df_csv):
    if df_csv is None or df_csv.empty:
        return []
    events = []
    for event_id, grp in df_csv.groupby("event_id", dropna=False):
        grp = grp.fillna("")
        first = grp.iloc[0]
        ev = {
            "id": _safe_text(first.get("event_id")) or uuid.uuid4().hex,
            "nombre": _safe_text(first.get("nombre")) or "Evento sin nombre",
            "fecha": _safe_text(first.get("fecha")),
            "hora_inicio": _safe_text(first.get("hora_inicio")),
            "hora_fin": _safe_text(first.get("hora_fin")),
            "promociones": _split_promociones(first.get("promociones")),
            "comentarios": _safe_text(first.get("comentarios")),
            "staff": [],
            "creado_en": _safe_text(first.get("creado_en")),
        }
        for _, row in grp.iterrows():
            staff_name = _safe_text(row.get("staff_nombre"))
            if not staff_name:
                continue
            estado = _normalizar_estado_asistencia(row.get("staff_estado"))
            ev["staff"].append({
                "nombre": staff_name,
                "id": _safe_text(row.get("staff_id")),
                "promo": _safe_text(row.get("staff_promo")),
                "estado": estado,
                "nota": _safe_text(row.get("staff_nota")),
            })
        events.append(ev)
    return events


def _payload_a_eventos_staff(payload):
    if not isinstance(payload, dict):
        return []

    if isinstance(payload.get("events"), list):
        return normalizar_eventos_staff(payload.get("events"))

    rows = payload.get("rows")
    if isinstance(rows, list):
        df_rows = pd.DataFrame(rows)
        return normalizar_eventos_staff(_filas_csv_a_eventos(df_rows))

    if "event_id" in payload or "staff_nombre" in payload:
        return normalizar_eventos_staff(_filas_csv_a_eventos(pd.DataFrame([payload])))

    if "id" in payload or "fecha" in payload:
        return normalizar_eventos_staff([payload])

    return []


def _merge_eventos_staff(existing_events, incoming_events):
    merged = {ev.get("id"): ev for ev in normalizar_eventos_staff(existing_events) if ev.get("id")}
    for ev in normalizar_eventos_staff(incoming_events):
        ev_id = _safe_text(ev.get("id")) or uuid.uuid4().hex
        ev["id"] = ev_id
        merged[ev_id] = ev
    return normalizar_eventos_staff(list(merged.values()))


def _leer_eventos_legacy_en_disco():
    if os.path.exists(STAFF_EVENTS_CSV):
        try:
            df_csv = pd.read_csv(STAFF_EVENTS_CSV, dtype=str, keep_default_na=False)
            return _filas_csv_a_eventos(df_csv)
        except Exception as e:
            app.logger.warning("No se pudo leer %s: %s", STAFF_EVENTS_CSV, e)

    return []


def _eventos_db_a_eventos():
    inspector = inspect(db.engine)
    table_names = set(inspector.get_table_names())
    if "staff_events" not in table_names:
        return []

    event_columns = {col["name"] for col in inspector.get_columns("staff_events")}
    comentarios_expr = "ev.comentarios" if "comentarios" in event_columns else "''"

    if "staff_event_members" in table_names:
        rows = db.session.execute(text(f"""
            SELECT
                ev.id AS event_id,
                ev.nombre AS nombre,
                ev.fecha AS fecha,
                ev.hora_inicio AS hora_inicio,
                ev.hora_fin AS hora_fin,
                ev.promociones AS promociones,
                {comentarios_expr} AS comentarios,
                ev.creado_en AS creado_en,
                st.nombre AS staff_nombre,
                st.staff_id AS staff_id,
                st.promo AS staff_promo,
                st.estado AS staff_estado,
                st.nota AS staff_nota
            FROM staff_events ev
            LEFT JOIN staff_event_members st ON st.event_id = ev.id
            ORDER BY ev.fecha, ev.hora_inicio, ev.nombre, st.nombre
        """)).mappings().all()
    else:
        rows = db.session.execute(text(f"""
            SELECT
                ev.id AS event_id,
                ev.nombre AS nombre,
                ev.fecha AS fecha,
                ev.hora_inicio AS hora_inicio,
                ev.hora_fin AS hora_fin,
                ev.promociones AS promociones,
                {comentarios_expr} AS comentarios,
                ev.creado_en AS creado_en,
                NULL AS staff_nombre,
                NULL AS staff_id,
                NULL AS staff_promo,
                NULL AS staff_estado,
                NULL AS staff_nota
            FROM staff_events ev
            ORDER BY ev.fecha, ev.hora_inicio, ev.nombre
        """)).mappings().all()

    if not rows:
        return []

    return normalizar_eventos_staff(_filas_csv_a_eventos(pd.DataFrame(rows)))


def _drop_staff_tables_from_students_db_if_present():
    inspector = inspect(db.engine)
    table_names = set(inspector.get_table_names())
    dropped = False
    for table_name in ["staff_event_members", "staff_events"]:
        if table_name in table_names:
            db.session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            dropped = True
    if dropped:
        db.session.commit()
        app.logger.info("Tablas legacy de staff removidas de app.db.")


def _guardar_eventos_staff_csv(events):
    events = normalizar_eventos_staff(events)
    filas = _eventos_a_filas_csv(events)
    cols = [
        "event_id",
        "nombre",
        "fecha",
        "hora_inicio",
        "hora_fin",
        "promociones",
        "creado_en",
        "comentarios",
        "staff_nombre",
        "staff_id",
        "staff_promo",
        "staff_estado",
        "staff_nota",
    ]
    pd.DataFrame(filas, columns=cols).to_csv(STAFF_EVENTS_CSV, index=False, encoding="utf-8-sig")


def _migrar_eventos_db_a_csv_si_aplica():
    db_events = _eventos_db_a_eventos()
    if db_events:
        csv_events = _leer_eventos_legacy_en_disco()
        merged = _merge_eventos_staff(csv_events, db_events)
        _guardar_eventos_staff_csv(merged)
        app.logger.info("Migrados %s eventos/asistencias legacy de app.db a staff_eventos.csv.", len(db_events))

    _drop_staff_tables_from_students_db_if_present()


def _services_has_staff_events(conn):
    row = conn.execute(text("SELECT 1 FROM staff_events LIMIT 1")).first()
    return row is not None


def _eventos_services_a_eventos():
    with services_engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT
                ev.id AS event_id,
                ev.nombre AS nombre,
                ev.fecha AS fecha,
                ev.hora_inicio AS hora_inicio,
                ev.hora_fin AS hora_fin,
                ev.promociones AS promociones,
                ev.comentarios AS comentarios,
                ev.creado_en AS creado_en,
                st.nombre AS staff_nombre,
                st.staff_id AS staff_id,
                st.promo AS staff_promo,
                st.estado AS staff_estado,
                st.nota AS staff_nota
            FROM staff_events ev
            LEFT JOIN staff_event_members st ON st.event_id = ev.id
            ORDER BY ev.fecha, ev.hora_inicio, ev.nombre, st.nombre
        """)).mappings().all()

    if not rows:
        return []

    return normalizar_eventos_staff(_filas_csv_a_eventos(pd.DataFrame(rows)))


def _guardar_eventos_staff_services_unchecked(events):
    events = normalizar_eventos_staff(events)
    with services_engine.begin() as conn:
        conn.execute(text("DELETE FROM staff_event_members"))
        conn.execute(text("DELETE FROM staff_events"))

        for ev in events:
            conn.execute(text("""
                INSERT INTO staff_events (
                    id, nombre, fecha, hora_inicio, hora_fin, promociones, comentarios, creado_en
                )
                VALUES (
                    :id, :nombre, :fecha, :hora_inicio, :hora_fin, :promociones, :comentarios, :creado_en
                )
            """), {
                "id": _safe_text(ev.get("id")) or uuid.uuid4().hex,
                "nombre": _safe_text(ev.get("nombre")) or "Evento sin nombre",
                "fecha": _safe_text(ev.get("fecha")),
                "hora_inicio": _safe_text(ev.get("hora_inicio")),
                "hora_fin": _safe_text(ev.get("hora_fin")),
                "promociones": "|".join([_safe_text(p) for p in ev.get("promociones", []) if _safe_text(p)]),
                "comentarios": _safe_text(ev.get("comentarios")),
                "creado_en": _safe_text(ev.get("creado_en")),
            })

            for st in ev.get("staff", []) or []:
                nombre_staff = _safe_text(st.get("nombre"))
                if not nombre_staff:
                    continue
                conn.execute(text("""
                    INSERT INTO staff_event_members (
                        event_id, nombre, staff_id, promo, estado, nota
                    )
                    VALUES (
                        :event_id, :nombre, :staff_id, :promo, :estado, :nota
                    )
                """), {
                    "event_id": _safe_text(ev.get("id")),
                    "nombre": nombre_staff,
                    "staff_id": _safe_text(st.get("id")),
                    "promo": _safe_text(st.get("promo")),
                    "estado": _normalizar_estado_asistencia(st.get("estado")),
                    "nota": _safe_text(st.get("nota")),
                })


def _migrar_eventos_legacy_a_services_si_aplica():
    if services_engine is None:
        return

    with services_engine.begin() as conn:
        if _services_has_staff_events(conn):
            _drop_staff_tables_from_students_db_if_present()
            return

    csv_events = _leer_eventos_legacy_en_disco()
    db_events = _eventos_db_a_eventos()
    merged = _merge_eventos_staff(csv_events, db_events)

    if merged:
        _guardar_eventos_staff_services_unchecked(merged)
        app.logger.info("Migrados %s eventos/asistencias legacy hacia DATABASE_URL.", len(merged))

    _drop_staff_tables_from_students_db_if_present()


def ensure_services_db():
    global _services_db_ready
    if services_engine is None:
        return False
    if _services_db_ready:
        return True

    with services_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS staff_events (
                id VARCHAR(64) PRIMARY KEY,
                nombre VARCHAR(255) NOT NULL,
                fecha VARCHAR(32) NOT NULL,
                hora_inicio VARCHAR(16),
                hora_fin VARCHAR(16),
                promociones TEXT,
                comentarios TEXT,
                creado_en VARCHAR(32)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS staff_event_members (
                event_id VARCHAR(64) NOT NULL,
                nombre VARCHAR(255) NOT NULL,
                staff_id VARCHAR(64),
                promo VARCHAR(64),
                estado VARCHAR(32),
                nota TEXT,
                PRIMARY KEY (event_id, nombre),
                FOREIGN KEY (event_id) REFERENCES staff_events (id)
            )
        """))

    _services_db_ready = True
    _migrar_eventos_legacy_a_services_si_aplica()
    return True


def _cargar_eventos_staff_services():
    ensure_services_db()
    try:
        return _eventos_services_a_eventos()
    except Exception as e:
        app.logger.warning("No se pudo leer staff desde DATABASE_URL: %s", e)
        return []


def cargar_eventos_staff():
    if services_engine is not None:
        return _cargar_eventos_staff_services()

    if not os.path.exists(STAFF_EVENTS_CSV):
        return []
    try:
        df_csv = pd.read_csv(STAFF_EVENTS_CSV, dtype=str, keep_default_na=False)
        return normalizar_eventos_staff(_filas_csv_a_eventos(df_csv))
    except Exception as e:
        app.logger.warning("No se pudo leer %s: %s", STAFF_EVENTS_CSV, e)
        return []


def guardar_eventos_staff(events):
    if services_engine is not None:
        ensure_services_db()
        _guardar_eventos_staff_services_unchecked(events)
        return

    _guardar_eventos_staff_csv(events)


def construir_catalogo_staff(df):
    info = construir_info_estudiantes(df)
    por_promo = {}
    for nombre, row in info.items():
        promo = _safe_text(row.get("promo")) or "SIN_PROMOCION"
        por_promo.setdefault(promo, []).append({
            "nombre": nombre,
            "id": _safe_text(row.get("id")),
            "promo": promo,
        })
    for promo in por_promo:
        por_promo[promo] = sorted(por_promo[promo], key=lambda x: x["nombre"])
    promociones = sorted(por_promo.keys(), key=str, reverse=True)
    return promociones, por_promo


def _normalizar_evento_staff(raw_event):
    staff_raw = raw_event.get("staff", [])
    staff = []
    for s in staff_raw:
        estado = _normalizar_estado_asistencia(s.get("estado"))
        staff.append({
            "nombre": _safe_text(s.get("nombre")),
            "id": _safe_text(s.get("id")),
            "promo": _safe_text(s.get("promo")),
            "estado": estado,
            "nota": _safe_text(s.get("nota")),
        })
    staff = [s for s in staff if s["nombre"]]
    return {
        "id": _safe_text(raw_event.get("id")) or uuid.uuid4().hex,
        "nombre": _safe_text(raw_event.get("nombre")) or "Evento sin nombre",
        "fecha": _safe_text(raw_event.get("fecha")),
        "hora_inicio": _safe_text(raw_event.get("hora_inicio")),
        "hora_fin": _safe_text(raw_event.get("hora_fin")),
        "promociones": [_safe_text(p) for p in raw_event.get("promociones", []) if _safe_text(p)],
        "comentarios": _safe_text(raw_event.get("comentarios")),
        "staff": staff,
        "creado_en": _safe_text(raw_event.get("creado_en")),
    }


def normalizar_eventos_staff(events):
    normalizados = []
    for event in events:
        if not isinstance(event, dict):
            continue
        ev = _normalizar_evento_staff(event)
        if ev["fecha"]:
            normalizados.append(ev)
    normalizados.sort(key=lambda x: (x["fecha"], x["hora_inicio"], x["nombre"]))
    return normalizados


def filtrar_eventos_staff(events, month_key, promociones_seleccionadas):
    month_prefix = f"{month_key}-"
    filtrados = [e for e in events if _safe_text(e.get("fecha")).startswith(month_prefix)]
    if promociones_seleccionadas:
        promos_set = {str(p) for p in promociones_seleccionadas}
        filtrados = [
            e for e in filtrados
            if promos_set.intersection(set([str(p) for p in e.get("promociones", [])]))
        ]
    return filtrados


def resumen_evento_staff(event):
    resumen = {"asistio": 0, "excusa": 0, "no": 0, "pendiente": 0}
    for s in event.get("staff", []):
        estado = _normalizar_estado_asistencia(s.get("estado"))
        if estado not in resumen:
            estado = "pendiente"
        resumen[estado] += 1
    return resumen


def construir_calendario(month_key, eventos_mes):
    dt = _parse_month_key(month_key)
    year = dt.year
    month = dt.month
    cal = calendar.Calendar(firstweekday=0)

    eventos_por_fecha = {}
    for ev in eventos_mes:
        fecha = ev.get("fecha")
        if not fecha:
            continue
        eventos_por_fecha.setdefault(fecha, []).append(ev)

    weeks = []
    for week in cal.monthdatescalendar(year, month):
        row = []
        for day in week:
            day_key = day.strftime("%Y-%m-%d")
            eventos_dia = eventos_por_fecha.get(day_key, [])
            resumen = {"asistio": 0, "excusa": 0, "no": 0, "pendiente": 0}
            event_names = []
            for ev in eventos_dia:
                r = resumen_evento_staff(ev)
                resumen["asistio"] += r["asistio"]
                resumen["excusa"] += r["excusa"]
                resumen["no"] += r["no"]
                resumen["pendiente"] += r["pendiente"]
                nombre_evento = _safe_text(ev.get("nombre"))
                if nombre_evento:
                    event_names.append(nombre_evento)
            row.append({
                "date": day_key,
                "day": day.day,
                "in_month": day.month == month,
                "event_count": len(eventos_dia),
                "resumen": resumen,
                "event_names": event_names,
            })
        weeks.append(row)
    return weeks, eventos_por_fecha


@app.route("/", methods=["GET", "POST"])
def index():

    df = cargar()

    horas = HORAS

    orden_dias = ["LUNES", "MARTES", "MIERCOLES", "JUEVES", "VIERNES", "SABADO"]
    dias_csv = df["Dia"].dropna().str.upper().unique().tolist()
    dias = [d for d in orden_dias if d in dias_csv]

    # Extraer promociones únicas
    promociones = sorted(df["Promocion"].dropna().unique().tolist(), key=str, reverse=True)

    disponibles = []
    disponibles_info = {}  # {nombre: id_documento}
    disponibles_promo = {}  # {nombre: promo}
    disponibles_correo = {}  # {nombre: correo}
    disponibles_contacto = {}  # {nombre: telefono/contacto}
    horario = []
    sel_estudiante = []
    sel_dias = []
    sel_inicio = ""
    sel_fin = ""
    sel_promociones = []
    estudiantes = []

    modo = "disponibilidad"  # valor por defecto

    if request.method == "POST":
        modo = request.form.get("modo", "disponibilidad")

        sel_promociones_raw = request.form.getlist("promociones")  # Obtener lista de promociones
        # Detectar si se seleccionó "TODAS" o si no hay selección
        todas_seleccionadas = "TODAS" in sel_promociones_raw
        # Filtrar solo promociones reales (excluir el valor especial "TODAS" y vacíos)
        sel_promociones = [p for p in sel_promociones_raw if p and p != "TODAS"]

        sel_dias = request.form.getlist("dias")  # Obtener lista de días
        sel_inicio = request.form["inicio"]
        sel_fin = request.form["fin"]
        
        sel_estudiante_raw = request.form.getlist("estudiante")
        if "TODOS" in sel_estudiante_raw or not sel_estudiante_raw:
            sel_estudiante = []
        else:
            sel_estudiante = [e for e in sel_estudiante_raw if e and e != "TODOS"]

        # Filtrar por promociones (si hay seleccionadas y NO es "Todas")
        if sel_promociones and not todas_seleccionadas:
            df_filtered = df[df["Promocion"].astype(str).isin([str(p) for p in sel_promociones])]
        else:
            df_filtered = df
            # Si es "Todas", limpiar sel_promociones para que quede vacío (sin checkmarks individuales)
            if todas_seleccionadas:
                sel_promociones = []
        estudiantes = sorted(df_filtered["Nombre_Estudiante"].dropna().unique().tolist())

        # Buscar según el modo seleccionado
        if sel_dias:
            if modo == "antidisponibilidad":
                # Unión: aparece si tiene clase en CUALQUIERA de los días seleccionados
                ocupados_union: set = set()
                for dia in sel_dias:
                    inicio_dia = str(request.form.get(f"inicio_{dia}", sel_inicio))
                    fin_dia = str(request.form.get(f"fin_{dia}", sel_fin))
                    ocupados_dia = set(buscar_no_disponibles(df_filtered, [dia], inicio_dia, fin_dia, sel_estudiante))
                    ocupados_union = ocupados_union.union(ocupados_dia)
                disponibles = sorted(ocupados_union)
            else:
                # Disponibilidad: intersección — libre en TODOS los días seleccionados
                libres_comunes: set = set()
                primer_dia = True
                for dia in sel_dias:
                    inicio_dia = str(request.form.get(f"inicio_{dia}", sel_inicio))
                    fin_dia = str(request.form.get(f"fin_{dia}", sel_fin))
                    libres_dia = set(buscar_disponibles(df_filtered, [dia], inicio_dia, fin_dia, sel_estudiante))
                    if primer_dia:
                        libres_comunes = libres_dia
                        primer_dia = False
                    else:
                        libres_comunes = libres_comunes.intersection(libres_dia)
                disponibles = sorted(libres_comunes)
        else:
            disponibles = []

        # Construir mapa nombre -> {id, promo} para los disponibles
        info_map = construir_info_estudiantes(df_filtered)
        disponibles_info = {nombre: info_map.get(nombre, {"id": "", "promo": ""}).get("id", "") for nombre in disponibles}
        disponibles_promo = {nombre: info_map.get(nombre, {"id": "", "promo": ""}).get("promo", "") for nombre in disponibles}
        disponibles_correo = {nombre: info_map.get(nombre, {}).get("correo", "") for nombre in disponibles}
        disponibles_contacto = {nombre: info_map.get(nombre, {}).get("contacto", "") for nombre in disponibles}

        if sel_estudiante:
            clases = df_filtered[df_filtered["Nombre_Estudiante"].isin(sel_estudiante)]
            materias_unicas = clases["Materia"].unique().tolist()
            color_map = {m: COLORES[i % len(COLORES)] for i, m in enumerate(materias_unicas)}

            for _, row in clases.iterrows():
                # Saltar filas sin horas válidas para el horario visual
                if not row.get("_horas_validas", False):
                    continue
                nombre_estudiante = _safe_text(row["Nombre_Estudiante"])
                maestro = info_map.get(nombre_estudiante, {})
                codigo = _safe_text(maestro.get("id")) or "--"
                correo = _safe_text(maestro.get("correo"))
                contacto = _safe_text(maestro.get("contacto"))

                nom_comp = nombre_estudiante.split()
                nombre_corto = nom_comp[0] if nom_comp else ""
                sufijo_nombre = f" ({nombre_corto})" if len(sel_estudiante) > 1 else ""
                
                horario.append({
                    "dia": row["Dia"],
                        "inicio": row["Hora_Inicio"],
                    "fin": row["Hora_Fin"],
                    "materia": str(row["Materia"]) + sufijo_nombre,
                    "docente": str(row.get("Docente", "")) if pd.notna(row.get("Docente", "")) else "",
                    "codigo": codigo,
                    "correo": correo,
                    "contacto": contacto,
                    "color": color_map.get(row["Materia"], "#3a7afe"),
                })
    else:
        # Mostrar todos los estudiantes si no se ha filtrado por promoción
        estudiantes = sorted(df["Nombre_Estudiante"].dropna().unique().tolist())

    # Construir mapa promocion -> lista de estudiantes para filtrado dinámico en JS
    todos_por_promo = {}
    for promo in promociones:
        est_promo = sorted(
            df[df["Promocion"].astype(str) == str(promo)]["Nombre_Estudiante"]
            .dropna().unique().tolist()
        )
        todos_por_promo[str(promo)] = est_promo

    return render_template(
        "index.html",
        active_tab="disponibilidad",
        horas=horas,
        dias=dias,
        promociones=promociones,
        estudiantes=estudiantes,
        disponibles=disponibles,
        disponibles_info=disponibles_info,
        disponibles_promo=disponibles_promo,
        disponibles_correo=disponibles_correo,
        disponibles_contacto=disponibles_contacto,
        horario=horario,
        dias_semana=dias,
        sel_estudiante=sel_estudiante,
        sel_dias=sel_dias,
        sel_inicio=sel_inicio,
        sel_fin=sel_fin,
        sel_promociones=sel_promociones,
        todos_por_promo=todos_por_promo,
        modo=modo,
    )


@app.route("/staff", methods=["GET", "POST"])
def staff():
    df = cargar()
    promociones, estudiantes_por_promo = construir_catalogo_staff(df)
    info_map = construir_info_estudiantes(df)

    mes_actual = request.args.get("mes") or datetime.now().strftime("%Y-%m")
    dia_seleccionado = request.args.get("dia", "")
    promociones_sel_get = request.args.getlist("promociones")
    promociones_sel = [p for p in promociones_sel_get if p]

    eventos = normalizar_eventos_staff(cargar_eventos_staff())

    if request.method == "POST":
        accion = request.form.get("accion", "")
        mes_post = request.form.get("mes", mes_actual)
        dia_post = request.form.get("dia", dia_seleccionado)
        promos_post = [p for p in request.form.getlist("promociones_contexto") if p]

        def find_event(event_id):
            for ev in eventos:
                if ev.get("id") == event_id:
                    return ev
            return None

        if accion == "crear_evento_staff":
            nombre_evento = _safe_text(request.form.get("nombre_evento")) or "Evento"
            fecha = _safe_text(request.form.get("fecha"))
            hora_inicio = _safe_text(request.form.get("hora_inicio"))
            hora_fin = _safe_text(request.form.get("hora_fin"))
            promociones_form = [p for p in request.form.getlist("promociones_evento") if p]
            staff_sel = sorted(set([n for n in request.form.getlist("staff") if _safe_text(n)]))

            if fecha:
                staff_rows = []
                for nombre in staff_sel:
                    person = info_map.get(nombre, {})
                    staff_rows.append({
                        "nombre": nombre,
                        "id": _safe_text(person.get("id")),
                        "promo": _safe_text(person.get("promo")),
                        "estado": "pendiente",
                        "nota": "",
                    })

                eventos.append({
                    "id": uuid.uuid4().hex,
                    "nombre": nombre_evento,
                    "fecha": fecha,
                    "hora_inicio": hora_inicio,
                    "hora_fin": hora_fin,
                    "promociones": promociones_form,
                    "comentarios": "",
                    "staff": staff_rows,
                    "creado_en": datetime.now().isoformat(timespec="seconds"),
                })
                eventos = normalizar_eventos_staff(eventos)
                guardar_eventos_staff(eventos)

                mes_post = fecha[:7]
                dia_post = fecha
                promos_post = promociones_form

        elif accion == "editar_evento_staff":
            event_id = _safe_text(request.form.get("event_id"))
            ev = find_event(event_id)
            if ev is not None:
                nombre_evento = _safe_text(request.form.get("nombre_evento")) or ev.get("nombre", "Evento")
                fecha = _safe_text(request.form.get("fecha")) or ev.get("fecha", "")
                hora_inicio = _safe_text(request.form.get("hora_inicio"))
                hora_fin = _safe_text(request.form.get("hora_fin"))
                promociones_text = _safe_text(request.form.get("promociones_texto"))
                comentarios = _safe_text(request.form.get("comentarios"))
                promociones_edit = [p.strip() for p in promociones_text.split(",") if p.strip()]
                staff_names = request.form.getlist("staff_name")
                staff_estados = request.form.getlist("staff_estado")

                ev["nombre"] = nombre_evento
                ev["fecha"] = fecha
                ev["hora_inicio"] = hora_inicio
                ev["hora_fin"] = hora_fin
                ev["promociones"] = promociones_edit
                ev["comentarios"] = comentarios

                if staff_names and staff_estados:
                    estado_por_staff = {}
                    for staff_name, staff_estado in zip(staff_names, staff_estados):
                        nombre_staff = _safe_text(staff_name)
                        if not nombre_staff:
                            continue
                        estado_por_staff[nombre_staff] = _normalizar_estado_asistencia(staff_estado)

                    for st in ev.get("staff", []):
                        nombre_staff = _safe_text(st.get("nombre"))
                        if nombre_staff in estado_por_staff:
                            st["estado"] = estado_por_staff[nombre_staff]

                guardar_eventos_staff(eventos)
                mes_post = fecha[:7] if len(fecha) >= 7 else mes_post
                dia_post = fecha or dia_post
                promos_post = promociones_edit if promociones_edit else promos_post

        elif accion == "eliminar_evento_staff":
            event_id = _safe_text(request.form.get("event_id"))
            prev_len = len(eventos)
            eventos = [ev for ev in eventos if ev.get("id") != event_id]
            if len(eventos) != prev_len:
                guardar_eventos_staff(eventos)

        elif accion == "agregar_staff_evento":
            event_id = _safe_text(request.form.get("event_id"))
            nombre_staff = _safe_text(request.form.get("staff_name"))
            ev = find_event(event_id)
            if ev is not None and nombre_staff:
                exists = any(_safe_text(s.get("nombre")) == nombre_staff for s in ev.get("staff", []))
                if not exists:
                    person = info_map.get(nombre_staff, {})
                    ev.setdefault("staff", []).append({
                        "nombre": nombre_staff,
                        "id": _safe_text(person.get("id")),
                        "promo": _safe_text(person.get("promo")),
                        "estado": "pendiente",
                        "nota": "",
                    })
                    guardar_eventos_staff(eventos)

        elif accion == "quitar_staff_evento":
            event_id = _safe_text(request.form.get("event_id"))
            staff_name = _safe_text(request.form.get("staff_name"))
            ev = find_event(event_id)
            if ev is not None and staff_name:
                before = len(ev.get("staff", []))
                ev["staff"] = [s for s in ev.get("staff", []) if _safe_text(s.get("nombre")) != staff_name]
                if len(ev.get("staff", [])) != before:
                    guardar_eventos_staff(eventos)

        elif accion == "actualizar_asistencia":
            event_id = _safe_text(request.form.get("event_id"))
            staff_name = _safe_text(request.form.get("staff_name"))
            estado = _normalizar_estado_asistencia(request.form.get("estado"))

            updated = False
            ev = find_event(event_id)
            if ev is not None:
                for st in ev.get("staff", []):
                    if _safe_text(st.get("nombre")) == staff_name:
                        st["estado"] = estado
                        updated = True
                        break
            if updated:
                guardar_eventos_staff(eventos)

        params = [("mes", mes_post)]
        if dia_post:
            params.append(("dia", dia_post))
        for promo in promos_post:
            params.append(("promociones", promo))
        return redirect(f"{url_for('staff')}?{urlencode(params)}")

    eventos_mes = filtrar_eventos_staff(eventos, mes_actual, promociones_sel)
    calendario, eventos_por_fecha = construir_calendario(mes_actual, eventos_mes)

    if not dia_seleccionado:
        dia_seleccionado = datetime.now().strftime("%Y-%m-%d")
    if not dia_seleccionado.startswith(f"{mes_actual}-"):
        dia_seleccionado = f"{mes_actual}-01"

    eventos_del_dia = eventos_por_fecha.get(dia_seleccionado, [])
    staff_catalogo = sorted(info_map.keys())
    info_map_lower = {str(k).strip().lower(): v for k, v in info_map.items()}
    eventos_resumen = []
    resumen_promocion = {}

    for ev in eventos_del_dia:
        staff_rows = []
        for s in ev.get("staff", []):
            nombre_staff = _safe_text(s.get("nombre"))
            maestro = info_map.get(nombre_staff) or info_map_lower.get(nombre_staff.lower(), {})
            staff_id = _safe_text(maestro.get("id")) or _safe_text(s.get("id"))
            staff_promo = _safe_text(maestro.get("promo")) or _safe_text(s.get("promo"))
            estado = _normalizar_estado_asistencia(s.get("estado"))

            staff_item = {
                **s,
                "nombre": nombre_staff,
                "id": staff_id,
                "promo": staff_promo,
                "estado": estado,
            }
            staff_rows.append(staff_item)

            if estado in {"asistio", "excusa", "no"}:
                promo_key = staff_promo or "Sin promoción"
                if promo_key not in resumen_promocion:
                    resumen_promocion[promo_key] = {"asistio": 0, "excusa": 0, "no": 0}
                resumen_promocion[promo_key][estado] += 1

        eventos_resumen.append({
            "evento": {**ev, "staff": staff_rows},
            "resumen": resumen_evento_staff({**ev, "staff": staff_rows}),
            "asistieron": [s for s in staff_rows if s.get("estado") == "asistio"],
            "excusas": [s for s in staff_rows if s.get("estado") == "excusa"],
            "no_fueron": [s for s in staff_rows if s.get("estado") == "no"],
            "promociones_texto": ", ".join([str(p) for p in ev.get("promociones", []) if _safe_text(p)]),
        })

    resumen_promocion_items = sorted(
        [{"promo": k, **v} for k, v in resumen_promocion.items()],
        key=lambda x: x["promo"],
    )

    return render_template(
        "staff.html",
        active_tab="staff",
        horas=HORAS,
        promociones=promociones,
        promociones_sel=promociones_sel,
        estudiantes_por_promo=estudiantes_por_promo,
        mes_actual=mes_actual,
        mes_prev=_shift_month(mes_actual, -1),
        mes_next=_shift_month(mes_actual, 1),
        dia_seleccionado=dia_seleccionado,
        calendario=calendario,
        eventos_resumen=eventos_resumen,
        resumen_promocion_items=resumen_promocion_items,
        staff_catalogo=staff_catalogo,
    )


@app.route("/staff/export.csv")
def export_staff_csv():
    events = normalizar_eventos_staff(cargar_eventos_staff())
    filas = _eventos_a_filas_csv(events)
    cols = [
        "event_id",
        "nombre",
        "fecha",
        "hora_inicio",
        "hora_fin",
        "promociones",
        "creado_en",
        "comentarios",
        "staff_nombre",
        "staff_id",
        "staff_promo",
        "staff_estado",
        "staff_nota",
    ]
    df_out = pd.DataFrame(filas, columns=cols)
    if df_out.empty:
        df_out = pd.DataFrame(columns=cols)

    csv_bytes = BytesIO()
    df_out.to_csv(csv_bytes, index=False, encoding="utf-8-sig")
    csv_bytes.seek(0)
    file_name = f"staff_eventos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return send_file(
        csv_bytes,
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=file_name,
    )


@app.route("/api/connection", methods=["GET", "POST"])
def api_connection():
    return jsonify({
        "ok": True,
        "status": "open",
        "service": "dashboard",
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    })


@app.route("/api/staff/events", methods=["GET", "POST"])
def api_staff_events():
    if not _sync_authorized():
        return _json_error("No autorizado", 401)

    if request.method == "GET":
        events = normalizar_eventos_staff(cargar_eventos_staff())
        return jsonify({
            "ok": True,
            "status": "open",
            "count": len(events),
            "events": events,
        })

    payload = request.get_json(silent=True)
    if payload is None:
        return _json_error("JSON requerido")

    incoming_events = _payload_a_eventos_staff(payload)
    if not incoming_events:
        return _json_error("No se recibieron eventos validos")

    replace = bool(payload.get("replace"))
    current_events = [] if replace else cargar_eventos_staff()
    merged_events = _merge_eventos_staff(current_events, incoming_events)
    guardar_eventos_staff(merged_events)

    return jsonify({
        "ok": True,
        "status": "open",
        "received": len(incoming_events),
        "count": len(merged_events),
    })


@app.route("/api/horario")
def api_horario():
    nombre = request.args.get("nombre")
    if not nombre:
        return jsonify({"error": "nombre requerido"}), 400
    df = cargar()
    df_est = df[df["Nombre_Estudiante"] == nombre]
    if df_est.empty:
        return jsonify([])

    maestro = get_student_info_by_names([nombre]).get(nombre, {})
    codigo = _safe_text(maestro.get("ID")) or "--"
    correo = _safe_text(maestro.get("CORREO"))
    contacto = _safe_text(maestro.get("CONTACTO"))

    # Solo incluir filas con horas válidas para la visualización
    df_est_valid = df_est[df_est["_horas_validas"] == True]

    materias_unicas = df_est_valid["Materia"].unique().tolist()
    color_map = {m: COLORES[i % len(COLORES)] for i, m in enumerate(materias_unicas)}

    res = []
    for _, row in df_est_valid.iterrows():
        res.append({
            "dia": row["Dia"],
            "inicio": row["Hora_Inicio"],
            "fin": row["Hora_Fin"],
            "materia": str(row["Materia"]),
            "docente": str(row.get("Docente", "")) if pd.notna(row.get("Docente", "")) else "",
            "codigo": codigo,
            "correo": correo,
            "contacto": contacto,
            "color": color_map.get(row["Materia"], COLORES[0]),
        })

    return jsonify(res)


with app.app_context():
    ensure_students_db()
    ensure_services_db()


if __name__ == "__main__":
    app.run(debug=True)
