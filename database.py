import sqlite3
import os
from werkzeug.security import generate_password_hash

DB_PATH = os.path.join(os.path.dirname(__file__), 'proyectos_ba.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        apellido TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        rol TEXT NOT NULL DEFAULT 'agente',
        activo INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS programas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT UNIQUE NOT NULL,
        nombre TEXT NOT NULL,
        descripcion TEXT,
        color TEXT DEFAULT '#1565C0',
        icono TEXT DEFAULT 'folder',
        activo INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS proyectos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        programa_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        codigo TEXT,
        descripcion TEXT,
        beneficiario TEXT,
        tipo_beneficiario TEXT DEFAULT 'universidad',
        adoptante TEXT,
        tipo_adoptante TEXT DEFAULT 'empresa',
        anr_monto REAL DEFAULT 0,
        estado TEXT DEFAULT 'activo',
        fecha_inicio DATE,
        fecha_fin_prevista DATE,
        fecha_fin_real DATE,
        agente_id INTEGER,
        municipio TEXT,
        localidad TEXT,
        area_tematica TEXT,
        contacto_nombre TEXT,
        contacto_email TEXT,
        contacto_telefono TEXT,
        porcentaje_avance INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (programa_id) REFERENCES programas(id),
        FOREIGN KEY (agente_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS hitos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        programa_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        descripcion TEXT,
        orden INTEGER DEFAULT 0,
        tipo TEXT DEFAULT 'hito',
        FOREIGN KEY (programa_id) REFERENCES programas(id)
    );

    CREATE TABLE IF NOT EXISTS avances_hitos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proyecto_id INTEGER NOT NULL,
        hito_id INTEGER NOT NULL,
        estado TEXT DEFAULT 'pendiente',
        fecha_prevista DATE,
        fecha_real DATE,
        porcentaje INTEGER DEFAULT 0,
        observaciones TEXT,
        registrado_por INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(proyecto_id, hito_id),
        FOREIGN KEY (proyecto_id) REFERENCES proyectos(id),
        FOREIGN KEY (hito_id) REFERENCES hitos(id),
        FOREIGN KEY (registrado_por) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS novedades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proyecto_id INTEGER NOT NULL,
        titulo TEXT NOT NULL,
        descripcion TEXT,
        tipo TEXT DEFAULT 'novedad',
        fecha DATE DEFAULT CURRENT_DATE,
        registrado_por INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proyecto_id) REFERENCES proyectos(id),
        FOREIGN KEY (registrado_por) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS importaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        programa_id INTEGER,
        filename TEXT,
        registros_importados INTEGER DEFAULT 0,
        registros_error INTEGER DEFAULT 0,
        log_detalle TEXT,
        importado_por INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (programa_id) REFERENCES programas(id),
        FOREIGN KEY (importado_por) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS asignaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agente_id INTEGER NOT NULL,
        proyecto_id INTEGER NOT NULL,
        UNIQUE(agente_id, proyecto_id),
        FOREIGN KEY (agente_id) REFERENCES users(id),
        FOREIGN KEY (proyecto_id) REFERENCES proyectos(id)
    );
    """)

    # Seed programs
    programas = [
        ('FITBA', 'FITBA', 'Fondo de Innovación Tecnológica de Buenos Aires', '#006AC1', 'star'),
        ('ORBITA', 'ORBITA', 'Programa ORBITA', '#7B1FA2', 'rocket'),
        ('CLIC', 'CLIC', 'Centros Locales de Innovación y Cultura', '#00897B', 'lightbulb'),
        ('CLINICA', 'Clínica Tecnológica', 'Programa de Clínica Tecnológica', '#E65100', 'medical_services'),
        ('FONICS', 'FONICS', 'Fondo de Innovación para Científicos', '#1B5E20', 'science'),
    ]
    for p in programas:
        cur.execute("INSERT OR IGNORE INTO programas (codigo, nombre, descripcion, color, icono) VALUES (?,?,?,?,?)", p)

    # Seed FITBA hitos
    fitba_id = cur.execute("SELECT id FROM programas WHERE codigo='FITBA'").fetchone()['id']
    fonics_id = cur.execute("SELECT id FROM programas WHERE codigo='FONICS'").fetchone()['id']
    clic_id = cur.execute("SELECT id FROM programas WHERE codigo='CLIC'").fetchone()['id']
    clinica_id = cur.execute("SELECT id FROM programas WHERE codigo='CLINICA'").fetchone()['id']
    orbita_id = cur.execute("SELECT id FROM programas WHERE codigo='ORBITA'").fetchone()['id']

    fitba_hitos = [
        (fitba_id, 'Presentación de propuesta', 'Presentación formal del proyecto', 1),
        (fitba_id, 'Aprobación técnica', 'Evaluación y aprobación por comité técnico', 2),
        (fitba_id, 'Firma de convenio', 'Firma del convenio entre partes', 3),
        (fitba_id, 'Primer desembolso ANR', 'Transferencia del primer tramo del ANR', 4),
        (fitba_id, 'Primer informe de avance', 'Presentación del 1er informe técnico-financiero', 5),
        (fitba_id, 'Segundo desembolso ANR', 'Transferencia del segundo tramo del ANR', 6),
        (fitba_id, 'Segundo informe de avance', 'Presentación del 2do informe técnico-financiero', 7),
        (fitba_id, 'Tercer desembolso ANR', 'Transferencia del tercer tramo del ANR', 8),
        (fitba_id, 'Informe final técnico', 'Entrega del informe técnico final', 9),
        (fitba_id, 'Informe final financiero', 'Rendición de cuentas final', 10),
        (fitba_id, 'Cierre administrativo', 'Cierre formal del proyecto', 11),
    ]
    fonics_hitos = [
        (fonics_id, 'Presentación y evaluación', '', 1),
        (fonics_id, 'Aprobación y convenio', '', 2),
        (fonics_id, 'Desembolso único ANR', '', 3),
        (fonics_id, 'Informe de avance', '', 4),
        (fonics_id, 'Informe final', '', 5),
        (fonics_id, 'Cierre', '', 6),
    ]
    clic_hitos = [
        (clic_id, 'Diagnóstico territorial', '', 1),
        (clic_id, 'Apertura del Centro', '', 2),
        (clic_id, 'Primera actividad', '', 3),
        (clic_id, 'Informe semestral', '', 4),
        (clic_id, 'Evaluación anual', '', 5),
    ]
    clinica_hitos = [
        (clinica_id, 'Diagnóstico inicial', '', 1),
        (clinica_id, 'Plan de trabajo', '', 2),
        (clinica_id, 'Implementación', '', 3),
        (clinica_id, 'Informe final', '', 4),
    ]
    orbita_hitos = [
        (orbita_id, 'Selección de cohorte', '', 1),
        (orbita_id, 'Inicio de programa', '', 2),
        (orbita_id, 'Primer hito formativo', '', 3),
        (orbita_id, 'Mentorías', '', 4),
        (orbita_id, 'Demo Day', '', 5),
        (orbita_id, 'Cierre de cohorte', '', 6),
    ]
    all_hitos = fitba_hitos + fonics_hitos + clic_hitos + clinica_hitos + orbita_hitos
    for h in all_hitos:
        cur.execute("INSERT OR IGNORE INTO hitos (programa_id, nombre, descripcion, orden) SELECT ?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM hitos WHERE programa_id=? AND nombre=?)",
                    (h[0], h[1], h[2], h[3], h[0], h[1]))

    # ── Organigrama ──────────────────────────────────────────────────────────
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS org_unidades (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre  TEXT NOT NULL,
        tipo    TEXT NOT NULL DEFAULT 'l3',   -- l1, l2, l3, staff
        pos_x   INTEGER DEFAULT 0,
        pos_y   INTEGER DEFAULT 0,
        ancho   INTEGER DEFAULT 200,
        orden   INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS org_personas (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        unidad_id  INTEGER NOT NULL,
        nombre     TEXT NOT NULL,
        cargo      TEXT,
        subtipo    TEXT,                      -- NULL, 'label', 'consultor'
        orden      INTEGER DEFAULT 0,
        activo     INTEGER DEFAULT 1,
        FOREIGN KEY (unidad_id) REFERENCES org_unidades(id)
    );

    -- ── IPC / actualización monetaria ────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS configuracion (
        clave TEXT PRIMARY KEY,
        valor TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS ipc_config (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        programa_id     INTEGER UNIQUE NOT NULL,
        campo_anio      TEXT NOT NULL DEFAULT 'anio',   -- columna de proyectos que porta el año de desembolso
        anio_offset     INTEGER DEFAULT 0,               -- desfase en años (ej: +1 para FITBA feb año+1)
        mes_desembolso  TEXT NOT NULL DEFAULT '06',      -- MM
        fecha_expr_sql  TEXT,                            -- expresión SQL custom (reemplaza campo_anio+offset+mes)
        campo_monto     TEXT NOT NULL DEFAULT 'anr_monto', -- campo monetario a actualizar por IPC
        FOREIGN KEY (programa_id) REFERENCES programas(id)
    );

    CREATE TABLE IF NOT EXISTS ponderadores_ipc (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha_desembolso TEXT NOT NULL,   -- YYYY-MM
        fecha_valuacion  TEXT NOT NULL,   -- YYYY-MM
        ponderador       REAL NOT NULL,
        UNIQUE (fecha_desembolso, fecha_valuacion)
    );

    -- ── Filtros dinámicos por programa ───────────────────────────────────────
    CREATE TABLE IF NOT EXISTS filtros_config (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        programa_id INTEGER NOT NULL,
        field_key   TEXT NOT NULL,       -- nombre de columna en proyectos
        label       TEXT NOT NULL,       -- etiqueta visible al usuario
        filter_type TEXT NOT NULL DEFAULT 'select',  -- select | text | boolean
        enabled     INTEGER DEFAULT 0,
        orden       INTEGER DEFAULT 0,
        UNIQUE (programa_id, field_key),
        FOREIGN KEY (programa_id) REFERENCES programas(id)
    );

    -- ── Clínica Tecnológica: convenios de financiamiento ─────────────────────
    CREATE TABLE IF NOT EXISTS convenios_financiamiento (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        programa_id INTEGER NOT NULL,
        anio        INTEGER,
        financiador TEXT,
        descripcion TEXT,
        monto       REAL DEFAULT 0,
        FOREIGN KEY (programa_id) REFERENCES programas(id)
    );

    -- ── Coordenadas para mapas ───────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS municipio_coords (
        municipio TEXT PRIMARY KEY,
        lat       REAL NOT NULL,
        lng       REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS ib_coords (
        beneficiario_key TEXT PRIMARY KEY,
        lat              REAL NOT NULL,
        lng              REAL NOT NULL
    );

    -- ── Instituciones ────────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS instituciones (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre          TEXT NOT NULL,
        nombre_corto    TEXT,
        tipo            TEXT DEFAULT 'universidad',
        cuit            TEXT,
        municipio       TEXT,
        localidad       TEXT,
        provincia       TEXT DEFAULT 'Buenos Aires',
        website         TEXT,
        descripcion     TEXT,
        estado_vinculo  TEXT DEFAULT 'activo',
        notas_vinculo   TEXT,
        created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS institucion_contactos (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        institucion_id  INTEGER NOT NULL,
        nombre          TEXT NOT NULL,
        cargo           TEXT,
        email           TEXT,
        telefono        TEXT,
        notas           TEXT,
        activo          INTEGER DEFAULT 1,
        created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (institucion_id) REFERENCES instituciones(id)
    );

    CREATE TABLE IF NOT EXISTS institucion_documentos (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        institucion_id  INTEGER NOT NULL,
        tipo_doc        TEXT DEFAULT 'otro',
        nombre_doc      TEXT NOT NULL,
        filename        TEXT NOT NULL,
        descripcion     TEXT,
        fecha_doc       DATE,
        subido_por      INTEGER,
        created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (institucion_id) REFERENCES instituciones(id),
        FOREIGN KEY (subido_por) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS institucion_novedades (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        institucion_id  INTEGER NOT NULL,
        titulo          TEXT NOT NULL,
        cuerpo          TEXT,
        tipo            TEXT DEFAULT 'novedad',
        fecha           DATE DEFAULT CURRENT_DATE,
        registrado_por  INTEGER,
        created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (institucion_id) REFERENCES instituciones(id),
        FOREIGN KEY (registrado_por) REFERENCES users(id)
    );

    -- ── Sincronización con Google Sheets ────────────────────────────────────
    CREATE TABLE IF NOT EXISTS sync_sheets_config (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        programa_id     INTEGER UNIQUE NOT NULL,
        sheet_url       TEXT NOT NULL,
        campo_clave     TEXT NOT NULL DEFAULT 'codigo',
        activo          INTEGER DEFAULT 1,
        ultima_sync     DATETIME,
        FOREIGN KEY (programa_id) REFERENCES programas(id)
    );

    CREATE TABLE IF NOT EXISTS sync_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        programa_id     INTEGER NOT NULL,
        insertados      INTEGER DEFAULT 0,
        actualizados    INTEGER DEFAULT 0,
        errores         INTEGER DEFAULT 0,
        detalle         TEXT,
        ejecutado_por   INTEGER,
        created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (programa_id) REFERENCES programas(id),
        FOREIGN KEY (ejecutado_por) REFERENCES users(id)
    );
    """)

    # ── Columnas incrementales en hitos ─────────────────────────────────────
    existing_hito_cols = {row[1] for row in cur.execute("PRAGMA table_info(hitos)").fetchall()}
    for col, tipo in [('etapa', 'TEXT'), ('anio_desde', 'INTEGER'), ('anio_hasta', 'INTEGER')]:
        if col not in existing_hito_cols:
            cur.execute(f"ALTER TABLE hitos ADD COLUMN {col} {tipo} DEFAULT NULL")

    # ── Hitos FITBA: marcar viejos, insertar nuevos 2024+ ────────────────────
    fitba_id_row = cur.execute("SELECT id FROM programas WHERE codigo='FITBA'").fetchone()
    if fitba_id_row:
        fitba_id = fitba_id_row['id'] if hasattr(fitba_id_row, 'keys') else fitba_id_row[0]
        # Los hitos legacy (sin anio_desde) ya no aplican a ninguna convocatoria activa
        cur.execute("""UPDATE hitos SET anio_hasta=2021
                       WHERE programa_id=? AND anio_desde IS NULL AND anio_hasta IS NULL""",
                    (fitba_id,))

        # Hitos FITBA 2024+: etapa 1 "Rendición avance"
        nuevos_ra = [
            (fitba_id, 'Llegó Rendición Avance',       'Rendición avance', 1,  2024),
            (fitba_id, 'Observación enviada',            'Rendición avance', 2,  2024),
            (fitba_id, 'Observación recibida',           'Rendición avance', 3,  2024),
            (fitba_id, 'OK rendición para evaluación',   'Rendición avance', 4,  2024),
            (fitba_id, 'Evaluación firmada',             'Rendición avance', 5,  2024),
            (fitba_id, 'Informe contable',               'Rendición avance', 6,  2024),
            (fitba_id, 'Conformidad SSCTI RA',           'Rendición avance', 7,  2024),
            (fitba_id, 'Comunicación',                   'Rendición avance', 8,  2024),
            # etapa 2 "Rendición final"
            (fitba_id, 'Recepción ITF/RF',              'Rendición final',  9,  2024),
            (fitba_id, 'PV Observaciones',              'Rendición final',  10, 2024),
            (fitba_id, 'Recepción subsanación',         'Rendición final',  11, 2024),
            (fitba_id, 'OK direcciones',                'Rendición final',  12, 2024),
            (fitba_id, 'Evaluación lista',              'Rendición final',  13, 2024),
            (fitba_id, 'Rendición Final cierre contable','Rendición final', 14, 2024),
            (fitba_id, 'RESO',                          'Rendición final',  15, 2024),
            (fitba_id, 'COMUNICACIÓN',                  'Rendición final',  16, 2024),
        ]
        for prog_id, nombre, etapa, orden, anio_d in nuevos_ra:
            cur.execute("""INSERT INTO hitos (programa_id, nombre, etapa, orden, anio_desde)
                           SELECT ?,?,?,?,? WHERE NOT EXISTS
                           (SELECT 1 FROM hitos WHERE programa_id=? AND nombre=? AND anio_desde=?)""",
                        (prog_id, nombre, etapa, orden, anio_d, prog_id, nombre, anio_d))

        # Pre-crear avances_hitos para proyectos 2024/2025 ya existentes
        nuevos_ids = [r[0] for r in cur.execute(
            "SELECT id FROM hitos WHERE programa_id=? AND anio_desde=2024", (fitba_id,)).fetchall()]
        proyectos_24_25 = cur.execute(
            "SELECT id FROM proyectos WHERE programa_id=? AND anio IN (2024,2025)", (fitba_id,)).fetchall()
        for prow in proyectos_24_25:
            proy_id = prow[0] if not hasattr(prow, 'keys') else prow['id']
            for hito_id in nuevos_ids:
                cur.execute("""INSERT OR IGNORE INTO avances_hitos
                               (proyecto_id, hito_id, estado) VALUES (?,?,'pendiente')""",
                            (proy_id, hito_id))

    # ── Columnas incrementales en proyectos ─────────────────────────────────
    existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(proyectos)").fetchall()}
    nuevas_cols = [
        ("direccion",          "TEXT"),
        ("referente",          "TEXT"),
        ("expediente_proyecto","TEXT"),
        ("itf_presentado",     "DATE"),
        ("itf_subsanar",       "DATE"),
        ("resumen",            "TEXT"),
        ("detalle",            "TEXT"),
        ("anr_indice_mm",      "TEXT"),
        # ORBITA / compartidas
        ("linea",              "TEXT"),
        ("ib2",                "TEXT"),
        ("anio_redaccion",     "INTEGER"),
        ("anio_publicacion",   "INTEGER"),
        # CLIC
        ("fuente_financiamiento", "TEXT"),
        ("contacto_municipio",    "TEXT"),
        ("n_inscriptos",          "INTEGER"),
        ("n_iniciaron",           "INTEGER"),
        ("n_finalizaron",         "INTEGER"),
        ("fecha_puesta_marcha",   "DATE"),
    ]
    for col, tipo in nuevas_cols:
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE proyectos ADD COLUMN {col} {tipo}")

    # ── Columnas incrementales en ipc_config ─────────────────────────────────
    existing_ipc_cols = {row[1] for row in cur.execute("PRAGMA table_info(ipc_config)").fetchall()}
    if 'campo_monto' not in existing_ipc_cols:
        cur.execute("ALTER TABLE ipc_config ADD COLUMN campo_monto TEXT NOT NULL DEFAULT 'anr_monto'")

    # Migración: asegurar campo_monto correcto para CLINICA
    clinica_id_row = cur.execute("SELECT id FROM programas WHERE codigo='CLINICA'").fetchone()
    if clinica_id_row:
        clin_id = clinica_id_row['id'] if hasattr(clinica_id_row, 'keys') else clinica_id_row[0]
        cur.execute("""UPDATE ipc_config SET campo_monto='monto_diagnostico'
                       WHERE programa_id=? AND campo_monto='anr_monto'""", (clin_id,))

    # Seed configuracion IPC (fecha de valuación por defecto)
    cur.execute("INSERT OR IGNORE INTO configuracion (clave, valor) VALUES ('ipc_ultima_fecha', '2026-02')")
    cur.execute("INSERT OR IGNORE INTO configuracion (clave, valor) VALUES ('ipc_primera_fecha', '2019-01')")

    # Seed admin user
    admin_hash = generate_password_hash('admin123')
    cur.execute("""INSERT OR IGNORE INTO users (nombre, apellido, email, password_hash, rol)
                   VALUES ('Admin', 'Sistema', 'admin@subsecretaria.gba.gov.ar', ?, 'admin')""", (admin_hash,))

    viewer_hash = generate_password_hash('subsec123')
    cur.execute("""INSERT OR IGNORE INTO users (nombre, apellido, email, password_hash, rol)
                   VALUES ('Subsecretario', 'GBA', 'subsecretario@gba.gov.ar', ?, 'visualizador')""", (viewer_hash,))

    agent_hash = generate_password_hash('agente123')
    cur.execute("""INSERT OR IGNORE INTO users (nombre, apellido, email, password_hash, rol)
                   VALUES ('Agente', 'Demo', 'agente@gba.gov.ar', ?, 'agente')""", (agent_hash,))

    conn.commit()
    conn.close()
    print("✓ Base de datos inicializada")

def query(sql, args=(), one=False):
    conn = get_db()
    cur = conn.execute(sql, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv

def execute(sql, args=()):
    conn = get_db()
    cur = conn.execute(sql, args)
    conn.commit()
    last_id = cur.lastrowid
    conn.close()
    return last_id

def execute_many(sql, args_list):
    conn = get_db()
    conn.executemany(sql, args_list)
    conn.commit()
    conn.close()
