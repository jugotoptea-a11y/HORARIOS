import os
import requests
from io import BytesIO
from urllib.parse import quote_plus
from flask import Flask, render_template, request, jsonify, send_file
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
# Se conservan funciones de lectura (si EXCEL_URL está configurada) y la generación de eventos
# ahora sólo produce y devuelve un archivo nuevo al solicitar `/crear_evento`.


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


@app.route('/crear_evento', methods=['POST'])
def crear_evento():
    estudiantes = request.form.getlist('estudiantes')
    if not estudiantes:
        return jsonify({'error': 'no hay estudiantes seleccionados'}), 400

    nombre_evento = request.form.get('nombre_evento', '')
    dia = request.form.get('dia', '')
    fecha = request.form.get('fecha', '')
    hora = request.form.get('hora', '')

    # Convertir fecha de YYYY-MM-DD a formato amigable
    fecha_formateada = ""
    if fecha:
        try:
            dt = datetime.strptime(fecha, "%Y-%m-%d")
            meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
            fecha_formateada = f"{dt.day} de {meses[dt.month - 1]}"
        except Exception:
            fecha_formateada = fecha

    dia_formateado = (dia.capitalize() + ", ") if dia else ""
    dia_completo = f"{dia_formateado}{fecha_formateada}" if fecha_formateada else dia.capitalize()

    # Formatear la hora: soporta "H:MM AM/PM - H:MM AM/PM" o valor simple "H:MM AM/PM"
    if " - " in hora:
        partes_hora = hora.split(" - ", 1)
        hora_formateada = " - ".join(
            p.strip().replace("AM", "am").replace("PM", "pm") for p in partes_hora
        )
    else:
        hora_formateada = hora.strip().replace("AM", "am").replace("PM", "pm")

    # Obtener ID de cada estudiante directamente del CSV local
    df_local = cargar()
    _info_map = construir_info_estudiantes(df_local)  # {nombre: {id, promo}}
    id_map = {k: v["id"] for k, v in _info_map.items()}

    filas = []
    for est in estudiantes:
        fila = {
            'Día': dia_completo,
            'Hora': hora_formateada,
            'Becados': est,
            'ID': id_map.get(est, ''),
        }
        filas.append(fila)

    new_df = pd.DataFrame(filas)

    # Generar Excel en memoria y devolverlo como descarga (no se edita/actualiza ningún archivo remoto)
    out = BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer:
        new_df.to_excel(writer, sheet_name=CLOUD_SHEET_EVENTS, index=False)
    out.seek(0)
    filename = f"eventos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    # Usamos download_name (Flask >=2.0). Si tu versión no lo soporta, reemplaza por 'attachment_filename'.
    return send_file(out,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True,
                     download_name=filename)


if __name__ == "__main__":
    app.run(debug=True)