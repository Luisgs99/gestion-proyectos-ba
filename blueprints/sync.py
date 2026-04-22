"""
Sincronización con Google Sheets.

Cada programa puede tener configurada una URL de Google Sheet (CSV export público).
El sync hace UPSERT en la tabla proyectos basándose en el campo clave de cada programa.
"""
import unicodedata
import re
import requests
import io
import pandas as pd
from flask import (Blueprint, render_template, request, redirect,
                   url_for, session, flash, jsonify)
from database import query, execute, get_db
from helpers.auth import admin_required, editor_required, login_required

bp = Blueprint('sync', __name__)


# ─── Mapeo de columnas del Sheet → columnas de proyectos ─────────────────────
#
# Clave: nombre de columna del sheet normalizado (minúsculas, sin tildes, sin símbolos extra)
# Valor: nombre de columna en la tabla proyectos
#
# Para agregar Clínica Tecnológica, añadir su entrada en COLUMN_MAPS con clave 'CLINICA'.

def _norm(s):
    """Normaliza un string: minúsculas, sin tildes, espacios → '_', sin caracteres raros."""
    s = str(s).strip().lower()
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')  # quita diacríticos
    s = re.sub(r'[^a-z0-9\s]', '', s)   # quita todo excepto letras, números y espacios
    s = re.sub(r'\s+', ' ', s).strip()
    return s


COLUMN_MAPS = {
    'FITBA': {
        # sheet_col_normalizada        : col_proyectos
        'codigo'                       : 'codigo',
        'ano'                          : 'anio',
        'linea'                        : 'linea',
        'titulo del proyecto'          : 'nombre',
        'ib'                           : 'beneficiario',
        'ib 2'                         : 'ib2',
        'adoptante'                    : 'adoptante',
        'directora'                    : 'director',
        'uvt'                          : 'uvt',
        'municipio adoptante'          : 'municipio',
        'anr'                          : 'anr_monto',
        'anr actualizado indice mm'    : 'anr_indice_mm',
        'anr actualizado'              : 'anr_actualizado',
        'direccion'                    : 'direccion',
        'latitud'                      : 'latitud',
        'longitud'                     : 'longitud',
        'seccion'                      : 'seccion',
        'visitado'                     : 'visitado',
        'n de visitas'                 : 'n_visitas',
        'sector de actividad 1'        : 'sector_actividad_1',
        'sector de actividad 2'        : 'sector_actividad_2',
        'sector de actividad 3'        : 'sector_actividad_3',
        'estado de ejecucion'          : 'estado',
        'n reso'                       : 'n_reso',
        'n de investigadores'          : 'n_investigadores',
        'mujeres'                      : 'n_mujeres',
        'directora mujer'              : 'directora_mujer',
        'sector por tema proyecto'     : 'sector_tema',
        'sector 2 por tema proyecto'   : 'sector_tema_2',
        'referente'                    : 'referente',
        'expediente seguimiento'       : 'expediente_seguimiento',
        'expediente proyecto'          : 'expediente_proyecto',
        'ita recibido'                 : 'ita_recibido',
        'ita para subsanar'            : 'ita_subsanar',
        'ita subsanado recibido con correcion': 'ita_subsanado',
        'ita evaluacion lista'         : 'ita_evaluacion',
        'ita firmado'                  : 'ita_firmado',
        'itf presentado'               : 'itf_presentado',
        'itf para subsanar'            : 'itf_subsanar',
        'resumen'                      : 'resumen',
        'detalle'                      : 'detalle',
    },
    'CLINICA': {
        'id'                            : 'codigo',
        'cuit'                          : 'cuit',
        'nombre de la empresa'          : 'nombre',
        'especialista'                  : 'especialista',
        'municipio'                     : 'municipio',
        'rubro'                         : 'rubro',
        'estado empresa'                : 'estado_empresa',
        'situacion'                     : 'situacion_clinica',
        'ano'                           : 'anio',
        'periodo de facturacion'        : 'periodo_facturacion',
        'monto promedio por diagnostico': 'monto_diagnostico',
    },
    'CLIC': {
        'codigo'                            : 'codigo',
        'municipio'                         : 'municipio',
        'tipo'                              : 'linea',
        'ano'                               : 'anio',
        'anr'                               : 'anr_monto',
        'fuente de financiamiento'          : 'fuente_financiamiento',
        'tematica'                          : 'area_tematica',
        'ubicacion'                         : 'direccion',
        'universidad'                       : 'beneficiario',
        'contacto universidad'              : 'contacto_nombre',
        'contacto municipio'                : 'contacto_municipio',
        'inscriptos'                        : 'n_inscriptos',
        'iniciaron'                         : 'n_iniciaron',
        'finalizaron'                       : 'n_finalizaron',
        'estado del proyecto'               : 'estado',
        'nro de expte gdeba'                : 'expediente_proyecto',
        'fecha firma contrato transferencia': 'fecha_inicio',
        'fecha de puesta en marcha'         : 'fecha_puesta_marcha',
    },
    'ORBITA': {
        'linea'                        : 'linea',
        'nombre'                       : 'nombre',
        'ano de redaccion'             : 'anio_redaccion',
        'ano de publicacion'           : 'anio_publicacion',
        'institucion beneficiaria'     : 'ib2',
        'estado'                       : 'situacion_clinica',  # el dashboard lee situacion_clinica
        'anr'                          : 'anr_monto',
        'anr feb26'                    : 'anr_indice_mm',
    },
}

# Prefijo a anteponer al valor clave del sheet para formar el codigo en la DB.
# Ej: CLINICA tiene ID=1234 en el sheet → codigo='CLINICA-1234' en la DB.
CLAVE_PREFIX = {
    'CLINICA': 'CLINICA-',
}

# Columnas de tipo numérico
NUMERIC_COLS = {
    'anr_monto', 'anr_actualizado', 'n_investigadores', 'n_mujeres',
    'n_visitas', 'latitud', 'longitud', 'anio', 'monto_diagnostico',
    'anio_redaccion', 'anio_publicacion',
    'n_inscriptos', 'n_iniciaron', 'n_finalizaron',
}
# Columnas que deben almacenarse como entero
INTEGER_COLS = {
    'anio', 'n_investigadores', 'n_mujeres', 'n_visitas',
    'anio_redaccion', 'anio_publicacion',
    'n_inscriptos', 'n_iniciaron', 'n_finalizaron',
}
BOOL_COLS = {'directora_mujer', 'visitado'}
DATE_COLS = {
    'ita_recibido', 'ita_subsanar', 'ita_subsanado', 'ita_evaluacion', 'ita_firmado',
    'itf_presentado', 'itf_subsanar',
    'fecha_inicio', 'fecha_puesta_marcha',
}

# Mapeo de valores de texto del sheet → valores esperados en la DB
VALUE_MAPS = {
    'estado': {
        'finalizado'                   : 'finalizado',
        'en ejecucion'                 : 'activo',
        'activo'                       : 'activo',
        'en proceso de finalizacion'   : 'en_finalizacion',
        'en finalizacion'              : 'en_finalizacion',
        'suspendido'                   : 'suspendido',
        'rescindido'                   : 'suspendido',
        'en evaluacion'                : 'en_evaluacion',
        'en evaluacion tecnica'        : 'en_evaluacion',
        # ORBITA
        'publicado'                    : 'finalizado',
        'en elaboracion'               : 'activo',
        'en edicion'                   : 'en_edicion',
        'en revision'                  : 'en_revision',
        'proximo a publicarse'         : 'en_edicion',
        # CLIC
        'ejecutado'                    : 'finalizado',
        'rendido'                      : 'finalizado',
        'finalizado'                   : 'finalizado',
        'presentado'                   : 'en_evaluacion',
        'pagado'                       : 'activo',
    },
}


def _parse_number(s):
    """
    Parsea un número de cualquier formato regional o con símbolo de moneda.

    Maneja:
      - '$7,864,616'  → 7864616   (coma como miles, US)
      - '2,022'       → 2022      (coma como miles)
      - '1.500.000'   → 1500000   (punto como miles, europeo)
      - '1.500,50'    → 1500.5    (punto=miles, coma=decimal, europeo)
      - '1,234.56'    → 1234.56   (coma=miles, punto=decimal, US)
      - '500000'      → 500000    (sin separadores)
    """
    # Quitar símbolo de moneda, espacios, signo +
    s = re.sub(r'[\s$€£¥+]', '', s)
    if not s or s in ('-', '—'):
        return None

    commas = s.count(',')
    dots   = s.count('.')

    if commas > 0 and dots > 0:
        # Ambos: el último determina el decimal
        if s.rfind(',') > s.rfind('.'):
            # Coma es decimal → estilo europeo: 1.234,56
            s = s.replace('.', '').replace(',', '.')
        else:
            # Punto es decimal → estilo US: 1,234.56
            s = s.replace(',', '')
    elif commas > 0:
        # Solo comas: decimal o miles según dígitos tras la última coma
        digits_after = len(s.rsplit(',', 1)[-1])
        if digits_after <= 2:
            # Coma decimal: 1,5 → 1.5
            s = s.replace(',', '.')
        else:
            # Coma como miles: 2,022 → 2022 | 7,864,616 → 7864616
            s = s.replace(',', '')
    elif dots > 0:
        # Solo puntos: decimal o miles según cantidad y dígitos
        dot_parts = s.split('.')
        if len(dot_parts) == 2 and len(dot_parts[1]) <= 2:
            # Punto decimal: 1500.50 → 1500.5
            pass
        else:
            # Punto como miles: 1.500.000 o 2.022 → quitar puntos
            s = s.replace('.', '')

    return float(s)


def _cast(col, val):
    """Convierte un valor del sheet al tipo correcto para la DB."""
    if val is None or str(val).strip() in ('', 'nan', 'NaN', 'None', '-', '—', '#N/A', '#VALUE!'):
        return None
    val = str(val).strip()

    if col in BOOL_COLS:
        return 1 if val.lower() in ('si', 'sí', 's', 'yes', '1', 'true', 'x') else 0

    if col in NUMERIC_COLS:
        try:
            result = _parse_number(val)
            if result is None:
                return None
            if col in INTEGER_COLS:
                return int(round(result))
            return result
        except Exception:
            return None

    if col in DATE_COLS:
        from datetime import datetime
        for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%y', '%Y/%m/%d'):
            try:
                return datetime.strptime(val, fmt).strftime('%Y-%m-%d')
            except Exception:
                continue
        return val  # guarda el string si no reconoce el formato

    if col in VALUE_MAPS:
        key = _norm(val)
        mapped = VALUE_MAPS[col].get(key)
        if mapped is not None:
            return mapped
        # estado: valores desconocidos (texto libre, etc.) caen a 'activo'
        if col == 'estado':
            return 'activo'
        return val

    return val


def _fetch_csv(url):
    """Descarga el CSV desde la URL del sheet y devuelve un DataFrame.

    Soporta tres formatos de URL de Google Sheets:
      1. Publicar en la web:  /spreadsheets/d/e/PUBLISHED_ID/pub?output=csv&gid=N
      2. URL de edición:      /spreadsheets/d/SHEET_ID/edit#gid=N
      3. Export directo:      /spreadsheets/d/SHEET_ID/export?format=csv&gid=N
    """
    url = url.strip()
    gid_match = re.search(r'gid=(\d+)', url)
    gid = gid_match.group(1) if gid_match else '0'

    # Caso 1 — URL de "Publicar en la web": /d/e/PUBLISHED_ID/pub
    pub_match = re.search(r'/spreadsheets/d/e/([a-zA-Z0-9_-]+)/pub', url)
    if pub_match:
        published_id = pub_match.group(1)
        url = (f"https://docs.google.com/spreadsheets/d/e/"
               f"{published_id}/pub?output=csv&gid={gid}")

    # Caso 2 — URL de edición o visualización: /d/SHEET_ID/edit  o  /d/SHEET_ID/view
    elif re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)/(edit|view|htmlview)', url):
        sheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)/', url)
        if sheet_match:
            sheet_id = sheet_match.group(1)
            url = (f"https://docs.google.com/spreadsheets/d/"
                   f"{sheet_id}/export?format=csv&gid={gid}")

    # Caso 3 — Ya es una URL de export o CSV directo: se usa tal cual

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise ValueError(f"Error HTTP {e.response.status_code} al acceder al sheet. "
                         f"URL usada: {url}") from e
    except requests.RequestException as e:
        raise ValueError(f"No se pudo conectar al sheet: {e}. URL usada: {url}") from e

    try:
        return pd.read_csv(io.StringIO(resp.content.decode('utf-8')))
    except Exception as e:
        raise ValueError(f"El archivo descargado no es un CSV válido: {e}") from e


def _run_sync(programa, col_map, campo_clave='codigo'):
    """
    Ejecuta el sync para un programa dado.
    campo_clave: nombre de la columna en la DB usada para el upsert (ej: 'codigo', 'cuit').
    Devuelve (insertados, actualizados, errores, detalle_list).
    """
    config = query("SELECT * FROM sync_sheets_config WHERE programa_id=? AND activo=1",
                   (programa['id'],), one=True)
    if not config:
        return 0, 0, 1, ['Sin URL de sheet configurada para este programa.']

    df = _fetch_csv(config['sheet_url'])

    # Normalizar nombres de columnas del sheet
    df.columns = [_norm(c) for c in df.columns]

    # Determinar qué columna del sheet corresponde al campo clave de la DB
    # (busca la clave del col_map cuyo valor == campo_clave)
    sheet_key_norm = _norm(
        next((k for k, v in col_map.items() if v == campo_clave), campo_clave)
    )
    if sheet_key_norm not in df.columns:
        return 0, 0, 1, [
            f"No se encontró la columna clave '{campo_clave}' (buscada como '{sheet_key_norm}') "
            f"en el sheet. Columnas disponibles: {list(df.columns)}"
        ]

    insertados, actualizados, errores = 0, 0, 0
    detalle = []

    conn = get_db()
    try:
        for i, row in df.iterrows():
            try:
                val_clave = row.get(sheet_key_norm)

                if val_clave is None or str(val_clave).strip() in ('', 'nan', 'None'):
                    continue

                val_clave_raw = str(val_clave).strip()
                # Aplicar prefijo si corresponde (ej: '1234' → 'CLINICA-1234')
                prefijo = CLAVE_PREFIX.get(programa['codigo'], '')
                val_clave_db = prefijo + val_clave_raw

                # Construir el dict de columnas a insertar/actualizar
                datos = {}
                for col_sheet_norm, col_db in col_map.items():
                    match_col = next((c for c in df.columns if c == col_sheet_norm), None)
                    if match_col is None:
                        continue
                    datos[col_db] = _cast(col_db, row.get(match_col))

                if not datos:
                    continue

                # Asegurar que el campo clave en datos tenga el valor con prefijo
                datos[campo_clave] = val_clave_db

                # Nombre obligatorio: si viene vacío, usar el valor clave
                if not datos.get('nombre') or str(datos.get('nombre', '')).strip() in ('', 'nan'):
                    datos['nombre'] = val_clave_db

                # UPSERT — WHERE usa campo_clave con el valor prefijado
                existente = conn.execute(
                    f"SELECT id FROM proyectos WHERE programa_id=? AND {campo_clave}=?",
                    (programa['id'], val_clave_db)
                ).fetchone()

                if existente:
                    # UPDATE: todas las columnas del sheet excepto el campo clave
                    set_parts = [f"{col}=?" for col in datos if col != campo_clave]
                    vals = [datos[col] for col in datos if col != campo_clave]
                    if set_parts:
                        vals += [programa['id'], val_clave_db]
                        conn.execute(
                            f"UPDATE proyectos SET {', '.join(set_parts)}, updated_at=CURRENT_TIMESTAMP "
                            f"WHERE programa_id=? AND {campo_clave}=?",
                            vals
                        )
                    actualizados += 1
                else:
                    # INSERT
                    datos['programa_id'] = programa['id']
                    cols_str = ', '.join(datos.keys())
                    placeholders = ', '.join(['?'] * len(datos))
                    conn.execute(
                        f"INSERT INTO proyectos ({cols_str}) VALUES ({placeholders})",
                        list(datos.values())
                    )
                    insertados += 1

            except Exception as e:
                errores += 1
                detalle.append(f"Fila {i+2}: {e}")

        conn.commit()
    finally:
        conn.close()

    return insertados, actualizados, errores, detalle


# ─── Ponderadores IPC ─────────────────────────────────────────────────────────

def _parse_fecha_ipc(s):
    """Convierte una cadena a formato YYYY-MM, o None si no reconoce el formato."""
    s = str(s).strip()
    if not s or s in ('nan', 'None', ''):
        return None
    # YYYY-MM
    if re.match(r'^\d{4}-\d{2}$', s):
        return s
    # YYYY-MM-DD → tomar solo YYYY-MM
    m = re.match(r'^(\d{4}-\d{2})-\d{2}$', s)
    if m:
        return m.group(1)
    # D/MM/YYYY o DD/MM/YYYY (fecha completa → solo YYYY-MM)
    m = re.match(r'^\d{1,2}/(\d{1,2})/(\d{4})$', s)
    if m:
        return f"{m.group(2)}-{m.group(1).zfill(2)}"
    # MM/YYYY o M/YYYY
    m = re.match(r'^(\d{1,2})/(\d{4})$', s)
    if m:
        return f"{m.group(2)}-{m.group(1).zfill(2)}"
    # YYYY/MM
    m = re.match(r'^(\d{4})/(\d{2})$', s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    meses_es = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'ago': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12',
    }
    meses_en = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
    }
    # ene-24, ene-2024, ene 24, jan-2024, etc.
    m = re.match(r'^([a-záéíóú]{3})[-\s](\d{2,4})$', s.lower())
    if m:
        abbr = m.group(1)
        mes = meses_es.get(abbr) or meses_en.get(abbr)
        anio = m.group(2)
        if len(anio) == 2:
            anio = '20' + anio
        if mes:
            return f"{anio}-{mes}"
    return None


def _run_sync_ponderadores(url):
    """
    Descarga el sheet matricial de ponderadores IPC y hace UPSERT en ponderadores_ipc.
    Formato esperado: filas = fecha_desembolso, columnas = fecha_valuacion, celdas = ponderador.
    Devuelve (insertados, actualizados, errores, detalle).
    """
    df = _fetch_csv(url)

    fecha_desembolso_col = df.columns[0]
    fecha_valuacion_cols = df.columns[1:]

    # Parsear encabezados de columna → fechas de valuación válidas
    fv_map = {}   # col_original → YYYY-MM
    cols_no_reconocidas = []
    for col in fecha_valuacion_cols:
        fv = _parse_fecha_ipc(str(col))
        if fv:
            fv_map[col] = fv
        else:
            cols_no_reconocidas.append(str(col))

    insertados, actualizados, errores = 0, 0, 0
    detalle = []
    if cols_no_reconocidas:
        detalle.append(f"Columnas no reconocidas como fecha: {', '.join(cols_no_reconocidas)}")

    conn = get_db()
    try:
        for i, row in df.iterrows():
            fd = _parse_fecha_ipc(str(row[fecha_desembolso_col]))
            if not fd:
                continue

            for fv_col, fv in fv_map.items():
                val = str(row[fv_col]).strip()
                if val in ('', 'nan', 'None', '-', '—', '#N/A', '#VALUE!'):
                    continue

                try:
                    ponderador = float(val.replace(',', '.'))
                except Exception:
                    errores += 1
                    detalle.append(f"Fila {i+2}, col '{fv_col}': valor no numérico '{val}'")
                    continue

                existing = conn.execute(
                    "SELECT id FROM ponderadores_ipc WHERE fecha_desembolso=? AND fecha_valuacion=?",
                    (fd, fv)
                ).fetchone()

                if existing:
                    conn.execute(
                        "UPDATE ponderadores_ipc SET ponderador=? WHERE fecha_desembolso=? AND fecha_valuacion=?",
                        (ponderador, fd, fv)
                    )
                    actualizados += 1
                else:
                    conn.execute(
                        "INSERT INTO ponderadores_ipc (fecha_desembolso, fecha_valuacion, ponderador) VALUES (?,?,?)",
                        (fd, fv, ponderador)
                    )
                    insertados += 1

        conn.commit()

        # Actualizar ipc_ultima_fecha usando el máximo de los encabezados reconocidos,
        # independientemente de si todas las celdas de esa columna tenían valores.
        if fv_map:
            ultima = max(fv_map.values())
            conn.execute(
                "INSERT INTO configuracion (clave, valor) VALUES ('ipc_ultima_fecha', ?) "
                "ON CONFLICT(clave) DO UPDATE SET valor=excluded.valor",
                (ultima,)
            )
            conn.commit()

    finally:
        conn.close()

    return insertados, actualizados, errores, detalle


# ─── Rutas ────────────────────────────────────────────────────────────────────

@bp.route('/sync')
@login_required
def index():
    programas = query("SELECT * FROM programas WHERE activo=1 ORDER BY id")
    configs = {r['programa_id']: r for r in query("SELECT * FROM sync_sheets_config")}
    logs = query("""
        SELECT sl.*, pr.nombre as prog_nombre, u.nombre as user_nombre, u.apellido as user_apellido
        FROM sync_log sl
        LEFT JOIN programas pr ON sl.programa_id = pr.id
        LEFT JOIN users u ON sl.ejecutado_por = u.id
        ORDER BY sl.created_at DESC LIMIT 30
    """)
    ipc_cfg = {r['clave']: r['valor'] for r in
               query("SELECT clave, valor FROM configuracion WHERE clave LIKE 'ipc_%'")}
    return render_template('sync/index.html',
                           programas=programas, configs=configs, logs=logs,
                           supported=list(COLUMN_MAPS.keys()),
                           ipc_cfg=ipc_cfg)


@bp.route('/sync/config', methods=['POST'])
@admin_required
def config_guardar():
    programa_id = request.form.get('programa_id')
    sheet_url   = request.form.get('sheet_url', '').strip()
    campo_clave = request.form.get('campo_clave', 'codigo').strip() or 'codigo'

    if not programa_id or not sheet_url:
        flash('Programa y URL son obligatorios.', 'danger')
        return redirect(url_for('sync.index'))

    existing = query("SELECT id FROM sync_sheets_config WHERE programa_id=?", (programa_id,), one=True)
    if existing:
        execute("UPDATE sync_sheets_config SET sheet_url=?, campo_clave=?, activo=1 WHERE programa_id=?",
                (sheet_url, campo_clave, programa_id))
    else:
        execute("INSERT INTO sync_sheets_config (programa_id, sheet_url, campo_clave) VALUES (?,?,?)",
                (programa_id, sheet_url, campo_clave))

    flash('Configuración guardada.', 'success')
    return redirect(url_for('sync.index'))


@bp.route('/sync/run/<int:programa_id>', methods=['POST'])
@editor_required
def run(programa_id):
    programa = query("SELECT * FROM programas WHERE id=?", (programa_id,), one=True)
    if not programa:
        flash('Programa no encontrado.', 'danger')
        return redirect(url_for('sync.index'))

    col_map = COLUMN_MAPS.get(programa['codigo'])
    if not col_map:
        flash(f'No hay mapeo de columnas definido para {programa["codigo"]} todavía.', 'warning')
        return redirect(url_for('sync.index'))

    config = query("SELECT campo_clave FROM sync_sheets_config WHERE programa_id=?",
                   (programa_id,), one=True)
    campo_clave = config['campo_clave'] if config else 'codigo'

    try:
        ins, act, err, detalle = _run_sync(programa, col_map, campo_clave)
        execute("""INSERT INTO sync_log (programa_id, insertados, actualizados, errores, detalle, ejecutado_por)
                   VALUES (?,?,?,?,?,?)""",
                (programa_id, ins, act, err, '\n'.join(detalle) if detalle else None, session['user_id']))
        execute("UPDATE sync_sheets_config SET ultima_sync=CURRENT_TIMESTAMP WHERE programa_id=?",
                (programa_id,))

        msg = f'{programa["codigo"]}: {ins} insertados, {act} actualizados.'
        if err:
            msg += f' {err} errores.'
        flash(msg, 'success' if err == 0 else 'warning')
    except Exception as e:
        execute("""INSERT INTO sync_log (programa_id, insertados, actualizados, errores, detalle, ejecutado_por)
                   VALUES (?,0,0,1,?,?)""",
                (programa_id, str(e), session['user_id']))
        flash(f'Error al sincronizar: {e}', 'danger')

    return redirect(url_for('sync.index'))


@bp.route('/sync/config/ponderadores', methods=['POST'])
@admin_required
def ponderadores_config_guardar():
    url = request.form.get('sheet_url', '').strip()
    if not url:
        flash('La URL es obligatoria.', 'danger')
        return redirect(url_for('sync.index'))
    execute(
        "INSERT INTO configuracion (clave, valor) VALUES ('ipc_ponderadores_url', ?) "
        "ON CONFLICT(clave) DO UPDATE SET valor=excluded.valor",
        (url,)
    )
    flash('URL de ponderadores guardada.', 'success')
    return redirect(url_for('sync.index'))


@bp.route('/sync/run/ponderadores', methods=['POST'])
@editor_required
def run_ponderadores():
    cfg = query("SELECT valor FROM configuracion WHERE clave='ipc_ponderadores_url'", one=True)
    if not cfg:
        flash('No hay URL de ponderadores configurada.', 'danger')
        return redirect(url_for('sync.index'))

    try:
        ins, act, err, detalle = _run_sync_ponderadores(cfg['valor'])
        execute(
            "INSERT INTO configuracion (clave, valor) VALUES ('ipc_ponderadores_ultima_sync', CURRENT_TIMESTAMP) "
            "ON CONFLICT(clave) DO UPDATE SET valor=CURRENT_TIMESTAMP"
        )
        msg = f'Ponderadores IPC: {ins} insertados, {act} actualizados.'
        if err:
            msg += f' {err} errores: ' + ' | '.join(detalle[:3])
        flash(msg, 'success' if err == 0 else 'warning')
    except Exception as e:
        flash(f'Error al sincronizar ponderadores: {e}', 'danger')

    return redirect(url_for('sync.index'))
