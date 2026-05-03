import os
import json
import uuid
import calendar
import requests
from io import BytesIO
from urllib.parse import urlencode
from flask import Flask, render_template, request, jsonify, redirect, url_for, send_file
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from datetime import datetime, timedelta
from threading import Lock

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ruta_csv = os.path.join(BASE_DIR, "horarios_extraidos.csv")

# Configuración opcional para Excel en la nube
# URL fijada por defecto (puedes sobrescribirla con la variable de entorno EXCEL_URL)
EXCEL_URL = os.environ.get("EXCEL_URL", "https://universidaddelacosta-my.sharepoint.com/:x:/g/personal/sbarriosb_cuc_edu_co/IQCQInUk0TAsRKREO6BIYHEWAYTOW10Tw65VVjKnMc63Xkw?e=pZiwUW")  # URL pública o pre-signed para descargar el .xlsx
EXCEL_UPLOAD_URL = os.environ.get("EXCEL_UPLOAD_URL")  # URL para subir (PUT) el .xlsx actualizado (opcional)
CLOUD_SHEET_GENERAL = os.environ.get("CLOUD_SHEET_GENERAL", "General")
CLOUD_SHEET_EVENTS = os.environ.get("CLOUD_SHEET_EVENTS", "STAFF EVENTOS 2026")
STAFF_EVENTS_CSV = os.path.join(BASE_DIR, "staff_eventos.csv")
STAFF_EVENTS_JSON_LEGACY = os.path.join(BASE_DIR, "staff_eventos.json")


def _build_database_uri():
    raw = (os.environ.get("DATABASE_URL") or "").strip()
    if not raw:
        if (os.environ.get("RENDER") or "").lower() == "true":
            raise RuntimeError(
                "DATABASE_URL es obligatorio en Render para evitar guardar datos "
                "en SQLite efimero."
            )
        return f"sqlite:///{os.path.join(BASE_DIR, 'app.db')}"
    if raw.startswith("postgres://"):
        return raw.replace("postgres://", "postgresql+psycopg://", 1)
    if raw.startswith("postgresql://"):
        return raw.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw


app.config["SQLALCHEMY_DATABASE_URI"] = _build_database_uri()
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


class StaffEvent(db.Model):
    __tablename__ = "staff_events"

    id = db.Column(db.String(64), primary_key=True)
    nombre = db.Column(db.String(255), nullable=False, default="Evento sin nombre")
    fecha = db.Column(db.String(10), nullable=False, index=True)
    hora_inicio = db.Column(db.String(8), nullable=True, default="")
    hora_fin = db.Column(db.String(8), nullable=True, default="")
    promociones = db.Column(db.Text, nullable=False, default="")
    creado_en = db.Column(db.String(32), nullable=True, default="")
    staff = db.relationship(
        "StaffEventMember",
        backref="event",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class StaffEventMember(db.Model):
    __tablename__ = "staff_event_members"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    event_id = db.Column(
        db.String(64),
        db.ForeignKey("staff_events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    nombre = db.Column(db.String(255), nullable=False)
    staff_id_value = db.Column("staff_id", db.String(64), nullable=True, default="")
    promo = db.Column(db.String(64), nullable=True, default="")
    estado = db.Column(db.String(16), nullable=False, default="pendiente")
    nota = db.Column(db.Text, nullable=True, default="")


_staff_tables_ready = False


def ensure_staff_tables():
    global _staff_tables_ready
    if _staff_tables_ready:
        return
    db.create_all()
    _staff_tables_ready = True


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
        inicio_dt = pd.to_datetime(
            df.get("Hora_Inicio", "").astype(str).str.strip().str.upper(),
            format="%I:%M %p", errors="coerce"
        )
        fin_dt = pd.to_datetime(
            df.get("Hora_Fin", "").astype(str).str.strip().str.upper(),
            format="%I:%M %p", errors="coerce"
        )

        df["_horas_validas"] = inicio_dt.notna() & fin_dt.notna()

        # Mantener columnas legibles para la UI y añadir columnas numéricas para comparar
        df["Hora_Inicio"] = inicio_dt.dt.strftime("%H:%M")
        df["Hora_Fin"] = fin_dt.dt.strftime("%H:%M")

        df["Hora_Inicio_min"] = (inicio_dt.dt.hour * 60 + inicio_dt.dt.minute)
        df["Hora_Fin_min"] = (fin_dt.dt.hour * 60 + fin_dt.dt.minute)

        df["Dia"] = df.get("Dia", "").astype(str).str.strip().str.upper()

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


def read_cloud_general_df():
    """Lee la hoja 'General' del Excel en la nube y la devuelve como DataFrame.
    Si no está disponible retorna DataFrame vacío.
    """
    b = download_excel_bytes()
    if b is None:
        return pd.DataFrame()
    try:
        df = pd.read_excel(b, sheet_name=CLOUD_SHEET_GENERAL, dtype=str)
        return df
    except Exception as e:
        app.logger.warning("Error leyendo hoja '%s' del Excel: %s", CLOUD_SHEET_GENERAL, e)
        return pd.DataFrame()


def _find_column(df, candidates):
    """Busca en df una columna que coincida con cualquiera de 'candidates' (lista de nombres posibles).
    Retorna el nombre de columna encontrado o None.
    """
    if df is None or df.columns is None:
        return None
    cols = list(df.columns)
    # coincidencia exacta (case-insensitive)
    lowered = {str(c).strip().lower(): c for c in cols}
    for cand in candidates:
        if cand is None:
            continue
        key = cand.strip().lower()
        if key in lowered:
            return lowered[key]
    # buscar por inclusión
    for col in cols:
        col_l = str(col).strip().lower()
        for cand in candidates:
            if cand and cand.strip().lower() in col_l:
                return col
    return None


def get_student_info_by_names(names):
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
        index_map[nm.lower()] = row

    for n in names:
        key = str(n).strip()
        row = None
        # búsqueda exacta
        if key.lower() in index_map:
            row = index_map[key.lower()]
        else:
            # búsqueda por inclusión (parcial)
            for k, r in index_map.items():
                if key.lower() in k or k in key.lower():
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
    """Retorna un dict {Nombre_Estudiante: {id, promo}} con el primer registro de cada estudiante."""
    info = {}
    for _, row in df.drop_duplicates(subset="Nombre_Estudiante").iterrows():
        nombre = row["Nombre_Estudiante"]
        try:
            doc = str(int(row["ID_Estudiante"]))
        except (ValueError, TypeError):
            doc = str(row["ID_Estudiante"]) if pd.notna(row["ID_Estudiante"]) else ""
        promo = str(row["Promocion"]) if pd.notna(row.get("Promocion", None)) else ""
        info[nombre] = {"id": doc, "promo": promo}
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


def _leer_eventos_legacy_en_disco():
    if os.path.exists(STAFF_EVENTS_CSV):
        try:
            df_csv = pd.read_csv(STAFF_EVENTS_CSV, dtype=str, keep_default_na=False)
            return _filas_csv_a_eventos(df_csv)
        except Exception as e:
            app.logger.warning("No se pudo leer %s: %s", STAFF_EVENTS_CSV, e)

    if os.path.exists(STAFF_EVENTS_JSON_LEGACY):
        try:
            with open(STAFF_EVENTS_JSON_LEGACY, "r", encoding="utf-8") as f:
                payload = json.load(f)
            events = payload.get("events", []) if isinstance(payload, dict) else []
            return normalizar_eventos_staff(events)
        except Exception as e:
            app.logger.warning("No se pudo leer %s: %s", STAFF_EVENTS_JSON_LEGACY, e)

    return []


def _event_model_to_dict(ev_model):
    staff_rows = sorted(ev_model.staff, key=lambda s: (s.nombre or "").lower())
    return {
        "id": _safe_text(ev_model.id),
        "nombre": _safe_text(ev_model.nombre) or "Evento sin nombre",
        "fecha": _safe_text(ev_model.fecha),
        "hora_inicio": _safe_text(ev_model.hora_inicio),
        "hora_fin": _safe_text(ev_model.hora_fin),
        "promociones": _split_promociones(ev_model.promociones),
        "staff": [
            {
                "nombre": _safe_text(st.nombre),
                "id": _safe_text(st.staff_id_value),
                "promo": _safe_text(st.promo),
                "estado": _normalizar_estado_asistencia(st.estado),
                "nota": _safe_text(st.nota),
            }
            for st in staff_rows
            if _safe_text(st.nombre)
        ],
        "creado_en": _safe_text(ev_model.creado_en),
    }


def _migrar_legacy_a_db_si_aplica():
    ensure_staff_tables()
    if StaffEvent.query.first() is not None:
        return
    legacy_events = _leer_eventos_legacy_en_disco()
    if legacy_events:
        guardar_eventos_staff(legacy_events)
        app.logger.info("Migrados %s eventos legacy a base de datos.", len(legacy_events))


def cargar_eventos_staff():
    ensure_staff_tables()
    _migrar_legacy_a_db_si_aplica()
    rows = StaffEvent.query.order_by(StaffEvent.fecha, StaffEvent.hora_inicio, StaffEvent.nombre).all()
    return [_event_model_to_dict(ev) for ev in rows]


def guardar_eventos_staff(events):
    ensure_staff_tables()
    events = normalizar_eventos_staff(events)

    existing = {ev.id: ev for ev in StaffEvent.query.all()}
    incoming_ids = {_safe_text(ev.get("id")) for ev in events if _safe_text(ev.get("id"))}

    for ev_id, ev_model in existing.items():
        if ev_id not in incoming_ids:
            db.session.delete(ev_model)

    for ev in events:
        ev_id = _safe_text(ev.get("id")) or uuid.uuid4().hex
        ev_model = existing.get(ev_id)
        if ev_model is None:
            ev_model = StaffEvent(id=ev_id)
            db.session.add(ev_model)

        ev_model.nombre = _safe_text(ev.get("nombre")) or "Evento sin nombre"
        ev_model.fecha = _safe_text(ev.get("fecha"))
        ev_model.hora_inicio = _safe_text(ev.get("hora_inicio"))
        ev_model.hora_fin = _safe_text(ev.get("hora_fin"))
        ev_model.promociones = "|".join([_safe_text(p) for p in ev.get("promociones", []) if _safe_text(p)])
        ev_model.creado_en = _safe_text(ev.get("creado_en"))

        ev_model.staff.clear()
        for st in ev.get("staff", []):
            nombre_staff = _safe_text(st.get("nombre"))
            if not nombre_staff:
                continue
            ev_model.staff.append(
                StaffEventMember(
                    nombre=nombre_staff,
                    staff_id_value=_safe_text(st.get("id")),
                    promo=_safe_text(st.get("promo")),
                    estado=_normalizar_estado_asistencia(st.get("estado")),
                    nota=_safe_text(st.get("nota")),
                )
            )

    db.session.commit()


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

        if sel_estudiante:
            clases = df_filtered[df_filtered["Nombre_Estudiante"].isin(sel_estudiante)]
            materias_unicas = clases["Materia"].unique().tolist()
            color_map = {m: COLORES[i % len(COLORES)] for i, m in enumerate(materias_unicas)}

            for _, row in clases.iterrows():
                # Saltar filas sin horas válidas para el horario visual
                if not row.get("_horas_validas", False):
                    continue
                try:
                    codigo = str(int(row["Codigo_Clase"]))
                except (ValueError, TypeError):
                    codigo = str(row["Codigo_Clase"]) if pd.notna(row["Codigo_Clase"]) else "--"
                
                nom_comp = str(row["Nombre_Estudiante"]).split()
                nombre_corto = nom_comp[0] if nom_comp else ""
                sufijo_nombre = f" ({nombre_corto})" if len(sel_estudiante) > 1 else ""
                
                horario.append({
                    "dia": row["Dia"],
                        "inicio": row["Hora_Inicio"],
                    "fin": row["Hora_Fin"],
                    "materia": str(row["Materia"]) + sufijo_nombre,
                    "codigo": codigo,
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
                promociones_edit = [p.strip() for p in promociones_text.split(",") if p.strip()]
                staff_names = request.form.getlist("staff_name")
                staff_estados = request.form.getlist("staff_estado")

                ev["nombre"] = nombre_evento
                ev["fecha"] = fecha
                ev["hora_inicio"] = hora_inicio
                ev["hora_fin"] = hora_fin
                ev["promociones"] = promociones_edit

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
            staff_id = _safe_text(s.get("id")) or _safe_text(maestro.get("id"))
            staff_promo = _safe_text(s.get("promo")) or _safe_text(maestro.get("promo"))
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


@app.route("/api/horario")
def api_horario():
    nombre = request.args.get("nombre")
    if not nombre:
        return jsonify({"error": "nombre requerido"}), 400
    df = cargar()
    df_est = df[df["Nombre_Estudiante"] == nombre]
    if df_est.empty:
        return jsonify([])

    # Solo incluir filas con horas válidas para la visualización
    df_est_valid = df_est[df_est["_horas_validas"] == True]

    materias_unicas = df_est_valid["Materia"].unique().tolist()
    color_map = {m: COLORES[i % len(COLORES)] for i, m in enumerate(materias_unicas)}

    res = []
    for _, row in df_est_valid.iterrows():
        try:
            codigo = str(int(row["Codigo_Clase"]))
        except (ValueError, TypeError):
            codigo = str(row["Codigo_Clase"]) if pd.notna(row["Codigo_Clase"]) else "--"
        res.append({
            "dia": row["Dia"],
            "inicio": row["Hora_Inicio"],
            "fin": row["Hora_Fin"],
            "materia": str(row["Materia"]),
            "codigo": codigo,
            "color": color_map.get(row["Materia"], COLORES[0]),
        })

    return jsonify(res)


with app.app_context():
    ensure_staff_tables()


if __name__ == "__main__":
    app.run(debug=True)
