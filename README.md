# Dashboard de horarios y staff

Aplicacion Flask para consultar disponibilidad de estudiantes a partir de horarios extraidos de PDFs y para gestionar eventos/asistencia de staff.

## Que hace

- Busca estudiantes disponibles por promocion, dia y franja horaria.
- Permite consultar antidisponibilidad, es decir, estudiantes ocupados en una franja.
- Muestra el horario visual de uno o varios estudiantes seleccionados.
- Cruza los horarios con datos maestros de estudiantes desde `Información.xlsx` y `app.db`.
- Gestiona eventos de staff desde `/staff`, con estados de asistencia: pendiente, asistio, excusa y no asistio.
- Exporta los eventos de staff a CSV.
- Expone endpoints JSON para integraciones simples.

## Estructura principal

```text
.
|-- app.py                    # Aplicacion Flask principal
|-- extraer_horarios.py       # Script para convertir PDFs de horarios en CSV
|-- horarios_extraidos.csv    # Fuente principal de horarios procesados
|-- Información.xlsx          # Datos maestros de estudiantes
|-- staff_eventos.csv         # Respaldo local/migracion inicial de staff
|-- app.db                    # Base SQLite local para datos maestros de estudiantes
|-- requirements.txt          # Dependencias de la app web
|-- templates/
|   |-- index.html            # Vista de disponibilidad
|   `-- staff.html            # Vista de eventos/asistencia
`-- H/
    |-- 2023/
    |-- 2024/
    |-- 2025/
    `-- 2026/                 # PDFs por promocion para extraccion
```

## Requisitos

- Python 3.10 o superior.
- pip.
- Dependencias de `requirements.txt`.
- Para regenerar `horarios_extraidos.csv` desde PDFs tambien se necesita `pdfplumber`.

> Nota: actualmente `pdfplumber` se importa en `extraer_horarios.py`, pero no aparece en `requirements.txt`.

## Instalacion local

En Windows:

```powershell
cd C:\Users\sebas\Downloads\HORARIO
py -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
py -m pip install pdfplumber
```

Si tu instalacion usa `python` en vez de `py`, cambia los comandos por:

```powershell
python -m venv .venv
python -m pip install -r requirements.txt
python -m pip install pdfplumber
```

## Ejecutar la app

```powershell
python app.py
```

Luego abre:

- `http://127.0.0.1:5000/` para disponibilidad.
- `http://127.0.0.1:5000/staff` para eventos y asistencia.

## Flujo de datos

### Horarios

La app lee `horarios_extraidos.csv`. Este archivo contiene columnas como:

- `Promocion`
- `Periodo`
- `ID_Estudiante`
- `Nombre_Estudiante`
- `Dia`
- `Hora_Inicio`
- `Hora_Fin`
- `Codigo_Clase`
- `Materia`
- `Docente`

Para regenerarlo desde los PDFs en `H/<promocion>/`:

```powershell
python extraer_horarios.py
```

El script recorre las carpetas dentro de `H/`, procesa los PDFs y sobrescribe `horarios_extraidos.csv`.

### Datos maestros de estudiantes

`Información.xlsx` se sincroniza hacia la tabla `students` en `app.db` al iniciar la aplicacion. Esa base local es la persistencia de los datos maestros de estudiantes, incluso si existe `DATABASE_URL`. Se usan columnas detectadas de forma flexible, por ejemplo:

- documento o identificacion
- nombre
- promocion
- correo
- contacto
- municipio
- programa

Si el Excel no existe, la app intenta seguir usando los datos que ya esten en `app.db`.

### Eventos de staff

Los eventos se guardan en `DATABASE_URL` cuando esta variable esta configurada. Si no existe `DATABASE_URL`, la app usa `staff_eventos.csv` como respaldo local. En el primer arranque con `DATABASE_URL`, si la base de servicios esta vacia, se migran los eventos legacy encontrados en `staff_eventos.csv` o en tablas antiguas de staff dentro de `app.db`.

Desde `/staff` se pueden:

- Crear eventos.
- Filtrar por mes, dia y promocion.
- Agregar o quitar estudiantes del evento.
- Editar nombre, fecha, horas, promociones y comentarios.
- Cambiar el estado de asistencia.
- Descargar el CSV desde `/staff/export.csv`.

## Variables de entorno

| Variable | Uso |
| --- | --- |
| `DATABASE_URL` | URI de base de datos para staff y servicios. No se usa para guardar datos maestros de estudiantes. |
| `RENDER` | Indicador de entorno Render. La app ya no lo usa para decidir la persistencia de estudiantes. |
| `EXCEL_URL` | URL opcional para descargar un Excel remoto si se habilita lectura desde nube. |
| `STUDENTS_SHEET_NAME` | Nombre esperado de la hoja de estudiantes. Por defecto: `General`. |
| `DASHBOARD_SYNC_TOKEN` | Token para proteger `/api/staff/events`. |

## Endpoints

| Metodo | Ruta | Descripcion |
| --- | --- | --- |
| `GET`, `POST` | `/` | Busqueda de disponibilidad y antidisponibilidad. |
| `GET`, `POST` | `/staff` | Gestion de eventos y asistencia de staff. |
| `GET` | `/staff/export.csv` | Descarga de eventos/asistencia en CSV. |
| `GET`, `POST` | `/api/connection` | Health check simple. |
| `GET`, `POST` | `/api/staff/events` | Consulta o carga de eventos via JSON. Requiere token si `DASHBOARD_SYNC_TOKEN` esta configurado. |
| `GET` | `/api/horario?nombre=<nombre>` | Devuelve el horario JSON de un estudiante. |

Para autenticar `/api/staff/events`, envia el token en cualquiera de estas formas:

- Header `Authorization: Bearer <token>`
- Header `X-Dashboard-Token: <token>`
- Query string `?token=<token>`

## Despliegue

El proyecto incluye `gunicorn` y soporte para PostgreSQL mediante `psycopg`.

Comando sugerido:

```bash
gunicorn app:app
```

En Render u otro hosting similar configura:

- `DATABASE_URL` con una base PostgreSQL persistente para staff y servicios.
- `RENDER=true` solo si quieres identificar el entorno en configuraciones externas.
- `DASHBOARD_SYNC_TOKEN` si vas a usar el endpoint de sincronizacion de eventos.

## Notas de mantenimiento

- No edites manualmente `horarios_extraidos.csv` si puedes regenerarlo desde los PDFs.
- Antes de regenerar horarios, conserva una copia del CSV actual si contiene ajustes manuales.
- `app.db`, `.env`, `__pycache__/` y `*.pyc` estan ignorados por Git.
- `staff_eventos.csv` queda como respaldo local/migracion inicial si no hay `DATABASE_URL`.
- `staff_eventos.json` parece ser un formato legado.
