import json
from flask import Blueprint, render_template, request, redirect, url_for, flash
from database import query, get_db
from helpers.auth import login_required, can_edit

bp = Blueprint('municipios', __name__)

# Condición para detectar si p.adoptante es este municipio (caso no-CLIC)
_ADOP_MATCH = """(
    TRIM(LOWER(p.adoptante)) = LOWER(?)
    OR LOWER(p.adoptante) LIKE 'municipalidad de ' || LOWER(?)
    OR LOWER(p.adoptante) LIKE 'municipio de ' || LOWER(?)
    OR LOWER(p.adoptante) LIKE 'municipalidad ' || LOWER(?)
)"""


@bp.route('/municipios/<nombre>')
@login_required
def detail(nombre):
    tab = request.args.get('tab', 'info')

    muni = query(
        "SELECT * FROM municipios WHERE LOWER(nombre)=LOWER(?)", (nombre,), one=True
    )

    # CLIC: el campo p.municipio identifica al municipio adoptante
    clic = query("""
        SELECT p.id, p.nombre, p.codigo, p.estado, p.anr_monto, p.anr_actualizado,
               p.fecha_inicio, p.linea, p.area_tematica,
               p.n_inscriptos, p.n_iniciaron, p.n_finalizaron,
               pr.codigo AS prog, pr.color, pr.nombre AS prog_nombre
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE pr.codigo='CLIC' AND UPPER(p.linea)='CLIC'
          AND LOWER(p.municipio) = LOWER(?)
        ORDER BY p.estado, p.nombre
    """, (nombre,))

    # Otros programas donde el municipio figura como adoptante en p.adoptante
    adoptante = query(f"""
        SELECT p.id, p.nombre, p.codigo, p.estado, p.anr_monto, p.anr_actualizado,
               p.adoptante, p.municipio,
               pr.codigo AS prog, pr.color, pr.nombre AS prog_nombre
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE pr.codigo NOT IN ('CLIC', 'CLINICA')
          AND {_ADOP_MATCH}
        ORDER BY pr.codigo, p.estado, p.nombre
    """, (nombre, nombre, nombre, nombre))

    # FITBA: empresas ubicadas en el municipio (adoptante = empresa, no municipio)
    fitba_empresas = query(f"""
        SELECT p.id, p.nombre, p.codigo, p.estado, p.anr_monto, p.anr_actualizado,
               p.adoptante, p.sector_actividad_1,
               pr.codigo AS prog, pr.color
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE pr.codigo='FITBA' AND LOWER(p.municipio) = LOWER(?)
          AND NOT {_ADOP_MATCH}
        ORDER BY p.estado, p.nombre
    """, (nombre, nombre, nombre, nombre, nombre))

    # Clínica: empresas en el municipio
    clinica_empresas = query("""
        SELECT p.id, p.nombre, p.codigo, p.estado,
               COALESCE(p.monto_diagnostico, p.anr_monto, 0) AS monto,
               p.rubro, pr.codigo AS prog, pr.color
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE pr.codigo='CLINICA' AND LOWER(p.municipio) = LOWER(?)
        ORDER BY p.estado, p.nombre
    """, (nombre,))

    # Universidades del sistema ubicadas en el municipio
    universidades = query("""
        SELECT nombre, nombre_corto, website
        FROM instituciones
        WHERE tipo='universidad' AND LOWER(municipio) = LOWER(?)
        ORDER BY nombre
    """, (nombre,))

    coords = query(
        "SELECT lat, lng FROM municipio_coords WHERE LOWER(municipio) = LOWER(?)",
        (nombre,), one=True
    )

    clic_l    = [dict(r) for r in clic]
    adop_l    = [dict(r) for r in adoptante]
    fitba_l   = [dict(r) for r in fitba_empresas]
    clinica_l = [dict(r) for r in clinica_empresas]
    univ_l    = [dict(r) for r in universidades]

    # Datos para Chart.js: proyectos por programa
    prog_data = {}
    for p in clic_l:
        prog_data.setdefault('CLIC', {'count': 0, 'color': p.get('color', '#00897B')})['count'] += 1
    for p in adop_l:
        k = p['prog']
        prog_data.setdefault(k, {'count': 0, 'color': p.get('color', '#666')})['count'] += 1
    for p in fitba_l:
        prog_data.setdefault('FITBA (empresas)', {'count': 0, 'color': p.get('color', '#006AC1')})['count'] += 1
    for p in clinica_l:
        prog_data.setdefault('Clínica', {'count': 0, 'color': p.get('color', '#E65100')})['count'] += 1

    # Datos para Chart.js: proyectos por estado
    estado_data = {}
    for p in clic_l + adop_l + fitba_l + clinica_l:
        est = p.get('estado') or 'sin estado'
        estado_data[est] = estado_data.get(est, 0) + 1

    ESTADO_COLORS = {
        'activo': '#00897B', 'en_curso': '#1565C0', 'finalizado': '#43A047',
        'suspendido': '#E65100', 'cancelado': '#B71C1C', 'pendiente': '#F9A825',
    }

    anr_adoptante = int(sum((p.get('anr_monto') or 0) for p in clic_l + adop_l))
    anr_empresas  = int(
        sum((p.get('anr_monto') or 0) for p in fitba_l) +
        sum((p.get('monto')    or 0) for p in clinica_l)
    )

    return render_template(
        'municipios/detail.html',
        nombre=nombre,
        muni=dict(muni) if muni else None,
        tab=tab,
        clic=clic_l,
        adoptante=adop_l,
        fitba_empresas=fitba_l,
        clinica_empresas=clinica_l,
        universidades=univ_l,
        coords=dict(coords) if coords else None,
        chart_prog=json.dumps(prog_data),
        chart_estados=json.dumps(estado_data),
        estado_colors=json.dumps(ESTADO_COLORS),
        anr_adoptante=anr_adoptante,
        anr_empresas=anr_empresas,
    )


@bp.route('/municipios/<nombre>/editar', methods=['POST'])
@login_required
def editar(nombre):
    if not can_edit():
        flash('Sin permisos de edición.', 'warning')
        return redirect(url_for('municipios.detail', nombre=nombre))

    def _int(key):
        v = request.form.get(key, '').strip().replace('.', '').replace(',', '')
        return int(v) if v.isdigit() else None

    def _float(key):
        v = request.form.get(key, '').strip().replace(',', '.')
        try:
            return float(v) if v else None
        except ValueError:
            return None

    def _text(key):
        return request.form.get(key, '').strip() or None

    # densidad se recalcula automáticamente si hay población y superficie
    pob = _int('poblacion')
    sup = _float('superficie')
    densidad = round(pob / sup) if pob and sup and sup > 0 else None

    data = {
        'intendente':        _text('intendente'),
        'partido_politico':  _text('partido_politico'),
        'interna':           _text('interna'),
        'poblacion':         pob,
        'seccion_electoral': _text('seccion_electoral'),
        'ciudad_cabecera':   _text('ciudad_cabecera'),
        'superficie':        sup,
        'densidad':          densidad,
        'concejales':        _int('concejales'),
        'gba':               1 if request.form.get('gba') == '1' else 0,
        'universidades':     _text('universidades'),
        'notas':             _text('notas'),
    }

    db = get_db()
    existing = db.execute(
        "SELECT id FROM municipios WHERE LOWER(nombre)=LOWER(?)", (nombre,)
    ).fetchone()
    if existing:
        db.execute("""
            UPDATE municipios
               SET intendente=?, partido_politico=?, interna=?,
                   poblacion=?, seccion_electoral=?, ciudad_cabecera=?,
                   superficie=?, densidad=?, concejales=?, gba=?,
                   universidades=?, notas=?,
                   updated_at=CURRENT_TIMESTAMP
             WHERE LOWER(nombre)=LOWER(?)""",
            (data['intendente'], data['partido_politico'], data['interna'],
             data['poblacion'], data['seccion_electoral'], data['ciudad_cabecera'],
             data['superficie'], data['densidad'], data['concejales'], data['gba'],
             data['universidades'], data['notas'], nombre))
    else:
        db.execute("""
            INSERT INTO municipios
                (nombre, intendente, partido_politico, interna,
                 poblacion, seccion_electoral, ciudad_cabecera,
                 superficie, densidad, concejales, gba,
                 universidades, notas)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (nombre, data['intendente'], data['partido_politico'], data['interna'],
             data['poblacion'], data['seccion_electoral'], data['ciudad_cabecera'],
             data['superficie'], data['densidad'], data['concejales'], data['gba'],
             data['universidades'], data['notas']))
    db.commit()
    db.close()
    flash('Datos del municipio actualizados.', 'success')
    return redirect(url_for('municipios.detail', nombre=nombre, tab='info'))
