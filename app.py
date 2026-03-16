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

    df = df.dropna(subset=["Hora_Inicio", "Hora_Fin"])

    df["Dia"] = df["Dia"].astype(str).str.strip().str.upper()

    return df


def buscar_disponibles(df, dias, inicio, fin, estudiante):

    inicio = convertir_24(inicio)
    fin = convertir_24(fin)

    if estudiante != "":
        df = df[df["Nombre_Estudiante"].str.contains(estudiante, case=False)]

    todos = set(df["Nombre_Estudiante"])

    ocupados = df[
        (df["Dia"].isin(dias)) &
        ~((df["Hora_Fin"] <= inicio) | (df["Hora_Inicio"] >= fin))
    ]

    ocupados_set = set(ocupados["Nombre_Estudiante"])

    libres = sorted(todos - ocupados_set)

    return libres


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

    estudiantes = sorted(df["Nombre_Estudiante"].dropna().unique().tolist())

    disponibles = []
    horario = []
    sel_estudiante = ""
    sel_dia = ""
    sel_inicio = ""
    sel_fin = ""

    if request.method == "POST":

        sel_dia = request.form["dias"]
        sel_inicio = request.form["inicio"]
        sel_fin = request.form["fin"]
        sel_estudiante = request.form["estudiante"]

        disponibles = buscar_disponibles(df, [sel_dia], sel_inicio, sel_fin, sel_estudiante)

        if sel_estudiante:
            clases = df[df["Nombre_Estudiante"] == sel_estudiante]
            materias_unicas = clases["Materia"].unique().tolist()
            color_map = {m: COLORES[i % len(COLORES)] for i, m in enumerate(materias_unicas)}

            for _, row in clases.iterrows():
                horario.append({
                    "dia": row["Dia"],
                    "inicio": row["Hora_Inicio"],
                    "fin": row["Hora_Fin"],
                    "materia": str(row["Materia"]),
                    "codigo": str(int(row["Codigo_Clase"])),
                    "color": color_map.get(row["Materia"], "#3a7afe"),
                })

    return render_template(
        "index.html",
        horas=horas,
        dias=dias,
        estudiantes=estudiantes,
        disponibles=disponibles,
        horario=horario,
        dias_semana=dias,
        sel_estudiante=sel_estudiante,
        sel_dia=sel_dia,
        sel_inicio=sel_inicio,
        sel_fin=sel_fin,
    )


if __name__ == "__main__":
    app.run(debug=True)