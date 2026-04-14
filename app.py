import os
from flask import Flask, render_template, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
from threading import Lock

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ruta_csv = os.path.join(BASE_DIR, "horarios_extraidos.csv")

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
    """Retorna un dict {Nombre_Estudiante: ID_Estudiante} con el primer registro de cada estudiante."""
    info = {}
    for _, row in df.drop_duplicates(subset="Nombre_Estudiante").iterrows():
        nombre = row["Nombre_Estudiante"]
        try:
            doc = str(int(row["ID_Estudiante"]))
        except (ValueError, TypeError):
            doc = str(row["ID_Estudiante"]) if pd.notna(row["ID_Estudiante"]) else ""
        info[nombre] = doc
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

        # Construir mapa nombre -> documento para los disponibles
        info_map = construir_info_estudiantes(df_filtered)
        disponibles_info = {nombre: info_map.get(nombre, "") for nombre in disponibles}

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


if __name__ == "__main__":
    app.run(debug=True)