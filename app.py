import os
from flask import Flask, render_template, request
import pandas as pd
from datetime import datetime, timedelta

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ruta_csv = os.path.join(BASE_DIR, "horarios_extraidos.csv")


def generar_horas():

    inicio = datetime.strptime("06:00", "%H:%M")
    fin = datetime.strptime("22:00", "%H:%M")

    horas = []

    while inicio <= fin:

        horas.append(inicio.strftime("%I:%M %p"))
        inicio += timedelta(minutes=30)

    return horas


def convertir_24(hora):
    # Si la hora está vacía, retornar None para manejarlo después
    if not hora or not hora.strip():
        return None
    
    return datetime.strptime(hora.strip().upper(), "%I:%M %p").strftime("%H:%M")


def cargar():

    df = pd.read_csv(ruta_csv, encoding="utf-8-sig")

    df["Hora_Inicio"] = pd.to_datetime(
        df["Hora_Inicio"].astype(str).str.strip().str.upper(),
        format="%I:%M %p", errors="coerce"
    ).dt.strftime("%H:%M")

    df["Hora_Fin"] = pd.to_datetime(
        df["Hora_Fin"].astype(str).str.strip().str.upper(),
        format="%I:%M %p", errors="coerce"
    ).dt.strftime("%H:%M")

    # Marcar filas con horas inválidas pero NO eliminarlas,
    # para que los estudiantes sigan apareciendo en el dropdown
    df["_horas_validas"] = df["Hora_Inicio"].notna() & df["Hora_Fin"].notna()

    df["Dia"] = df["Dia"].astype(str).str.strip().str.upper()

    return df


def buscar_disponibles(df, dias, inicio, fin, estudiante):

    inicio = convertir_24(inicio)
    fin = convertir_24(fin)
    
    # Si no hay hora inicio/fin, usar valores por defecto
    if inicio is None:
        inicio = "06:00"
    if fin is None:
        fin = "22:00"

    if estudiante != "":
        df = df[df["Nombre_Estudiante"].str.contains(estudiante, case=False)]

    todos = set(df["Nombre_Estudiante"])

    # Solo usar filas con horas válidas para determinar quién está ocupado
    df_valido = df[df["_horas_validas"] == True]

    ocupados = df_valido[
        (df_valido["Dia"].isin(dias)) &
        ~((df_valido["Hora_Fin"] <= inicio) | (df_valido["Hora_Inicio"] >= fin))
    ]

    ocupados_set = set(ocupados["Nombre_Estudiante"])

    libres = sorted(todos - ocupados_set)

    return libres


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

    horas = generar_horas()

    orden_dias = ["LUNES", "MARTES", "MIERCOLES", "JUEVES", "VIERNES", "SABADO"]
    dias_csv = df["Dia"].dropna().str.upper().unique().tolist()
    dias = [d for d in orden_dias if d in dias_csv]

    # Extraer promociones únicas
    promociones = sorted(df["Promocion"].dropna().unique().tolist(), key=str, reverse=True)

    disponibles = []
    disponibles_info = {}  # {nombre: id_documento}
    horario = []
    sel_estudiante = ""
    sel_dias = []
    sel_inicio = ""
    sel_fin = ""
    sel_promociones = []
    estudiantes = []

    if request.method == "POST":

        sel_promociones_raw = request.form.getlist("promociones")  # Obtener lista de promociones
        # Detectar si se seleccionó "TODAS" o si no hay selección
        todas_seleccionadas = "TODAS" in sel_promociones_raw
        # Filtrar solo promociones reales (excluir el valor especial "TODAS" y vacíos)
        sel_promociones = [p for p in sel_promociones_raw if p and p != "TODAS"]

        sel_dias = request.form.getlist("dias")  # Obtener lista de días
        sel_inicio = request.form["inicio"]
        sel_fin = request.form["fin"]
        sel_estudiante = request.form["estudiante"]

        # Filtrar por promociones (si hay seleccionadas y NO es "Todas")
        if sel_promociones and not todas_seleccionadas:
            df_filtered = df[df["Promocion"].astype(str).isin([str(p) for p in sel_promociones])]
        else:
            df_filtered = df
            # Si es "Todas", limpiar sel_promociones para que quede vacío (sin checkmarks individuales)
            if todas_seleccionadas:
                sel_promociones = []
        estudiantes = sorted(df_filtered["Nombre_Estudiante"].dropna().unique().tolist())

        # Buscar disponibles - intersección entre todos los días seleccionados
        # El estudiante debe ser libre en TODOS los días, no solo en alguno
        if sel_dias:
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
            clases = df_filtered[df_filtered["Nombre_Estudiante"] == sel_estudiante]
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
                horario.append({
                    "dia": row["Dia"],
                    "inicio": row["Hora_Inicio"],
                    "fin": row["Hora_Fin"],
                    "materia": str(row["Materia"]),
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
    )


if __name__ == "__main__":
    app.run(debug=True)