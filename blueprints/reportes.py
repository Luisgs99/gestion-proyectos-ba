import io
from collections import OrderedDict
from datetime import datetime
import pandas as pd
from flask import Blueprint, render_template, request, send_file, jsonify
from database import query
from helpers.auth import login_required
from helpers.ipc import (get_ipc_config, get_ipc_rule, build_ipc_join,
                         ipc_anr_expr, ipc_fecha_desemb_label)

bp = Blueprint('reportes', __name__)

# ─── Metadata de columnas exportables/filtrables ─────────────────────────────
# ftype: 'text' | 'estado' | 'dynamic' | 'anio' | 'rango_monto' | 'rango_numero'
# progs: None = todos los programas; lista = solo esos códigos

COLUMNAS_META = OrderedDict([
    ('nombre',             {'label': 'Nombre del proyecto',    'filterable': True,  'ftype': 'text',         'default': True,  'progs': None}),
    ('codigo',             {'label': 'Código',                 'filterable': True,  'ftype': 'text',         'default': True,  'progs': None}),
    ('estado',             {'label': 'Estado',                 'filterable': True,  'ftype': 'estado',       'default': True,  'progs': None}),
    ('municipio',          {'label': 'Municipio',              'filterable': True,  'ftype': 'dynamic',      'default': True,  'progs': None}),
    ('localidad',          {'label': 'Localidad',              'filterable': False, 'ftype': None,           'default': False, 'progs': None}),
    ('anio',               {'label': 'Año',                    'filterable': True,  'ftype': 'anio',         'default': True,  'progs': ['FITBA','FONICS','ORBITA','CLIC']}),
    ('anio_aprobacion',    {'label': 'Año de aprobación',      'filterable': True,  'ftype': 'anio',         'default': False, 'progs': ['FITBA','FONICS']}),
    ('beneficiario',       {'label': 'Beneficiario',           'filterable': True,  'ftype': 'text',         'default': True,  'progs': None}),
    ('ib2',                {'label': 'IB (institución)',       'filterable': True,  'ftype': 'text',         'default': True,  'progs': ['FITBA','FONICS']}),
    ('adoptante',          {'label': 'Adoptante (empresa)',    'filterable': True,  'ftype': 'text',         'default': True,  'progs': ['FITBA','CLIC','CLINICA']}),
    ('anr_monto',          {'label': 'ANR nominal ($)',        'filterable': True,  'ftype': 'rango_monto',  'default': True,  'progs': None}),
    ('linea',              {'label': 'Línea',                  'filterable': True,  'ftype': 'dynamic',      'default': True,  'progs': ['FITBA','FONICS']}),
    ('seccion',            {'label': 'Sección',                'filterable': True,  'ftype': 'dynamic',      'default': False, 'progs': ['FITBA']}),
    ('sector_tema',        {'label': 'Sector / Tema',          'filterable': True,  'ftype': 'dynamic',      'default': False, 'progs': ['FITBA']}),
    ('sector_actividad_1', {'label': 'Actividad principal',    'filterable': True,  'ftype': 'dynamic',      'default': False, 'progs': ['FITBA']}),
    ('area_tematica',      {'label': 'Área temática',          'filterable': True,  'ftype': 'dynamic',      'default': True,  'progs': ['CLIC','CLINICA']}),
    ('uvt',                {'label': 'UVT',                    'filterable': True,  'ftype': 'text',         'default': False, 'progs': ['FITBA','FONICS']}),
    ('director',           {'label': 'Director',               'filterable': True,  'ftype': 'text',         'default': False, 'progs': ['FONICS']}),
    ('n_investigadores',   {'label': 'N° investigadores',      'filterable': True,  'ftype': 'rango_numero', 'default': False, 'progs': ['FONICS']}),
    ('n_mujeres',          {'label': 'Mujeres en equipo',      'filterable': True,  'ftype': 'rango_numero', 'default': False, 'progs': ['FONICS']}),
    ('porcentaje_avance',  {'label': 'Avance (%)',             'filterable': True,  'ftype': 'rango_numero', 'default': True,  'progs': None}),
    ('situacion_clinica',  {'label': 'Situación clínica',      'filterable': True,  'ftype': 'dynamic',      'default': True,  'progs': ['CLINICA']}),
    ('especialista',       {'label': 'Especialista',           'filterable': False, 'ftype': None,           'default': False, 'progs': ['CLINICA']}),
    ('rubro',              {'label': 'Rubro',                  'filterable': True,  'ftype': 'dynamic',      'default': True,  'progs': ['CLINICA']}),
    ('monto_diagnostico',  {'label': 'Monto diagnóstico ($)',  'filterable': True,  'ftype': 'rango_monto',  'default': True,  'progs': ['CLINICA']}),
    ('periodo_facturacion',{'label': 'Período facturación',    'filterable': False, 'ftype': None,           'default': False, 'progs': ['FITBA']}),
    ('tipo_beneficiario',  {'label': 'Tipo beneficiario',      'filterable': True,  'ftype': 'dynamic',      'default': False, 'progs': None}),
    ('tipo_adoptante',     {'label': 'Tipo adoptante',         'filterable': True,  'ftype': 'dynamic',      'default': False, 'progs': None}),
    ('fecha_inicio',       {'label': 'Fecha inicio',           'filterable': False, 'ftype': None,           'default': False, 'progs': None}),
    ('fecha_fin_prevista', {'label': 'Fecha fin prevista',     'filterable': False, 'ftype': None,           'default': False, 'progs': None}),
    # Computadas via JOIN
    ('programa',           {'label': 'Programa',               'filterable': False, 'ftype': None,           'default': True,  'progs': None, 'computed': True}),
    ('agente',             {'label': 'Agente responsable',     'filterable': False, 'ftype': None,           'default': True,  'progs': None, 'computed': True}),
])

ESTADOS_LABELS = {
    'activo': 'Activo', 'finalizado': 'Finalizado',
    'suspendido': 'Suspendido', 'en_evaluacion': 'En evaluación',
}


# ─── Helpers internos ─────────────────────────────────────────────────────────

def _prog_codigo(programa_id):
    if not programa_id:
        return None
    row = query("SELECT codigo FROM programas WHERE id=?", (programa_id,), one=True)
    return row['codigo'] if row else None


def _apply_dynamic_filters(sql, args, prog_codigo):
    """Lee f_FIELD y f_FIELD_desde/hasta/min/max de request.args y los agrega al SQL."""
    for key, meta in COLUMNAS_META.items():
        if not meta.get('filterable') or meta.get('computed'):
            continue
        if prog_codigo and meta.get('progs') and prog_codigo not in meta['progs']:
            continue
        ftype = meta.get('ftype')

        if ftype in ('text', 'dynamic', 'estado'):
            val = request.args.get(f'f_{key}', '').strip()
            if val:
                sql += f' AND p.{key}=?'
                args.append(val)

        elif ftype == 'anio':
            desde = request.args.get(f'f_{key}_desde', '').strip()
            hasta = request.args.get(f'f_{key}_hasta', '').strip()
            if desde:
                sql += f' AND CAST(p.{key} AS INTEGER)>=?'
                args.append(int(desde))
            if hasta:
                sql += f' AND CAST(p.{key} AS INTEGER)<=?'
                args.append(int(hasta))

        elif ftype in ('rango_monto', 'rango_numero'):
            minv = request.args.get(f'f_{key}_min', '').strip()
            maxv = request.args.get(f'f_{key}_max', '').strip()
            cast = float if ftype == 'rango_monto' else int
            if minv:
                sql += f' AND p.{key}>=?'
                args.append(cast(minv))
            if maxv:
                sql += f' AND p.{key}<=?'
                args.append(cast(maxv))

    return sql, args


def _fmt_monto(v):
    if v is None:
        return '—'
    if v >= 1_000_000_000:
        return f'${v/1e9:.2f} MM'
    if v >= 1_000_000:
        return f'${v/1e6:.1f}M'
    if v >= 1_000:
        return f'${v/1e3:.0f}K'
    return f'${v:,.0f}'


def _generar_resumen_ejecutivo(kpis, por_programa, fecha_str):
    """Genera el párrafo de resumen ejecutivo para el informe."""
    total = kpis.get('total', 0)
    activos = kpis.get('activos', 0)
    finalizados = kpis.get('finalizados', 0)
    municipios = kpis.get('municipios', 0)
    anr = kpis.get('anr_nominal', 0)
    anr_real = kpis.get('anr_real', 0)

    pct_activos = round(100 * activos / total) if total else 0
    progs_activos = [p for p in por_programa if p.get('total', 0) > 0]
    progs_str = ', '.join(f"<strong>{p['codigo']}</strong>" for p in progs_activos[:4])
    if len(progs_activos) > 4:
        progs_str += f" y {len(progs_activos)-4} más"

    partes = [
        f"Al <strong>{fecha_str}</strong>, el portafolio de innovación de la Subsecretaría "
        f"cuenta con <strong>{total} proyectos</strong> registrados, "
        f"de los cuales <strong>{activos} se encuentran activos</strong> "
        f"({pct_activos}%) y <strong>{finalizados} han finalizado</strong>. "
    ]
    if municipios:
        partes.append(
            f"La intervención alcanza <strong>{municipios} municipios</strong> de la provincia. "
        )
    if anr:
        partes.append(
            f"El compromiso ANR nominal acumulado asciende a <strong>{_fmt_monto(anr)}</strong>"
        )
        if anr_real and anr_real != anr:
            partes.append(
                f", equivalente a <strong>{_fmt_monto(anr_real)} a valores actualizados</strong> (IPC). "
            )
        else:
            partes.append('. ')
    if progs_activos:
        partes.append(f"Los programas en ejecución son {progs_str}.")

    return ''.join(partes)


# ─── Rutas ────────────────────────────────────────────────────────────────────

@bp.route('/reportes')
@login_required
def index():
    programas = query("SELECT * FROM programas WHERE activo=1 ORDER BY id")
    ipc_meta = query("SELECT clave, valor FROM configuracion WHERE clave LIKE 'ipc_%'")
    ipc_fecha_val = {r['clave']: r['valor'] for r in ipc_meta}.get('ipc_ultima_fecha', '')

    municipios = [r['municipio'] for r in query(
        "SELECT DISTINCT municipio FROM proyectos "
        "WHERE municipio IS NOT NULL AND municipio != '' ORDER BY municipio"
    )]
    instituciones = [r['ib2'] for r in query(
        "SELECT DISTINCT ib2 FROM proyectos "
        "WHERE ib2 IS NOT NULL AND ib2 != '' ORDER BY ib2"
    )]

    return render_template('reports/index.html',
                           programas=programas,
                           ipc_fecha_val=ipc_fecha_val,
                           municipios=municipios,
                           instituciones=instituciones)


# ─── API: columnas y opciones de filtro para un programa ─────────────────────

@bp.route('/api/reportes/columnas')
@login_required
def api_columnas():
    programa_id = request.args.get('programa_id', '').strip()
    prog_codigo = _prog_codigo(programa_id) if programa_id else None

    cols = []
    for key, meta in COLUMNAS_META.items():
        # Filtrar por programa
        if prog_codigo and meta.get('progs') and prog_codigo not in meta['progs']:
            continue

        col = {
            'key':        key,
            'label':      meta['label'],
            'filterable': meta.get('filterable', False),
            'ftype':      meta.get('ftype'),
            'default':    meta.get('default', False),
            'computed':   meta.get('computed', False),
        }

        ftype = meta.get('ftype')

        # Opciones para selects dinámicos
        if ftype == 'dynamic' and meta.get('filterable'):
            where_parts = [f'{key} IS NOT NULL', f"{key} != ''"]
            q_args = []
            if programa_id:
                where_parts.append('programa_id=?')
                q_args.append(programa_id)
            opts = query(
                f"SELECT DISTINCT {key} FROM proyectos WHERE {' AND '.join(where_parts)} ORDER BY {key}",
                q_args
            )
            col['options'] = [r[key] for r in opts]

        # Rango para campos año
        if ftype == 'anio' and programa_id:
            r = query(
                f"SELECT MIN(CAST({key} AS INTEGER)) as mn, MAX(CAST({key} AS INTEGER)) as mx "
                f"FROM proyectos WHERE programa_id=? AND {key} IS NOT NULL",
                (programa_id,), one=True
            )
            if r and r['mn']:
                col['rango'] = {'min': r['mn'], 'max': r['mx']}

        # Rango para montos
        if ftype == 'rango_monto' and programa_id:
            r = query(
                f"SELECT MIN({key}) as mn, MAX({key}) as mx "
                f"FROM proyectos WHERE programa_id=? AND {key} > 0",
                (programa_id,), one=True
            )
            if r and r['mn']:
                col['rango'] = {'min': int(r['mn']), 'max': int(r['mx'])}

        cols.append(col)

    return jsonify({'columnas': cols, 'prog_codigo': prog_codigo})


# ─── API: datos agregados para informe y análisis ────────────────────────────

@bp.route('/api/reportes/datos')
@login_required
def api_datos():
    # Filtros recibidos
    programa_ids  = request.args.getlist('programa_id')
    estados_sel   = request.args.getlist('estado')
    anio_desde    = request.args.get('anio_desde', '').strip()
    anio_hasta    = request.args.get('anio_hasta', '').strip()
    municipio_sel = request.args.get('municipio', '').strip()
    ib2_sel       = request.args.get('ib2', '').strip()
    dimension     = request.args.get('dimension', '')  # campo a analizar en tab Análisis

    # Construir WHERE base
    where = ['1=1']
    args  = []
    if programa_ids:
        placeholders = ','.join('?' * len(programa_ids))
        where.append(f'p.programa_id IN ({placeholders})')
        args.extend(programa_ids)
    if estados_sel:
        placeholders = ','.join('?' * len(estados_sel))
        where.append(f'p.estado IN ({placeholders})')
        args.extend(estados_sel)
    if anio_desde:
        where.append('CAST(COALESCE(p.anio, p.anio_aprobacion, 0) AS INTEGER)>=?')
        args.append(int(anio_desde))
    if anio_hasta:
        where.append('CAST(COALESCE(p.anio, p.anio_aprobacion, 0) AS INTEGER)<=?')
        args.append(int(anio_hasta))
    if municipio_sel:
        where.append('p.municipio=?')
        args.append(municipio_sel)
    if ib2_sel:
        where.append('p.ib2=?')
        args.append(ib2_sel)

    w = ' AND '.join(where)

    # KPIs globales
    kpis_row = query(
        f"SELECT COUNT(*) as total,"
        f" SUM(CASE WHEN p.estado='activo' THEN 1 ELSE 0 END) as activos,"
        f" SUM(CASE WHEN p.estado='finalizado' THEN 1 ELSE 0 END) as finalizados,"
        f" SUM(CASE WHEN p.estado='suspendido' THEN 1 ELSE 0 END) as suspendidos,"
        f" COUNT(DISTINCT p.municipio) as municipios,"
        f" COALESCE(SUM(p.anr_monto),0) as anr_nominal,"
        f" COALESCE(AVG(p.porcentaje_avance),0) as avance_prom"
        f" FROM proyectos p JOIN programas pr ON p.programa_id=pr.id"
        f" WHERE {w}", args, one=True
    )
    kpis = dict(kpis_row) if kpis_row else {}

    # ANR real (IPC) – se intenta para cada programa
    ipc_meta = query("SELECT clave, valor FROM configuracion WHERE clave LIKE 'ipc_%'")
    ipc_fecha_val = {r['clave']: r['valor'] for r in ipc_meta}.get('ipc_ultima_fecha', '')
    anr_real_total = 0
    anr_real_by_prog = {}
    programas_todos = query("SELECT * FROM programas WHERE activo=1")
    for p in programas_todos:
        if programa_ids and str(p['id']) not in programa_ids:
            continue
        ipc_join, has_ipc = build_ipc_join(None, ipc_fecha_val, programa_id=p['id'], alias='proy')
        if has_ipc:
            real_expr = ipc_anr_expr(has_ipc, alias='proy')
            extra_where = [f'proy.programa_id={p["id"]}']
            if estados_sel:
                ph = ','.join('?' * len(estados_sel))
                extra_where.append(f"proy.estado IN ({ph})")
            r = query(
                f"SELECT COALESCE(SUM({real_expr}),0) as s"
                f" FROM proyectos proy {ipc_join}"
                f" WHERE {' AND '.join(extra_where)}",
                estados_sel if estados_sel else []
            , one=True)
            val = r['s'] if r else 0
        else:
            r = query(
                f"SELECT COALESCE(SUM(anr_monto),0) as s FROM proyectos"
                f" WHERE programa_id=?", (p['id'],), one=True
            )
            val = r['s'] if r else 0
        anr_real_total += val
        anr_real_by_prog[p['id']] = round(val)
    kpis['anr_real'] = round(anr_real_total)

    # Por programa
    por_programa = query(
        f"SELECT pr.id, pr.nombre, pr.codigo, pr.color,"
        f" COUNT(p.id) as total,"
        f" SUM(CASE WHEN p.estado='activo' THEN 1 ELSE 0 END) as activos,"
        f" SUM(CASE WHEN p.estado='finalizado' THEN 1 ELSE 0 END) as finalizados,"
        f" SUM(CASE WHEN p.estado='suspendido' THEN 1 ELSE 0 END) as suspendidos,"
        f" COALESCE(SUM(p.anr_monto),0) as anr_nominal"
        f" FROM programas pr LEFT JOIN proyectos p ON p.programa_id=pr.id AND {w}"
        f" WHERE pr.activo=1 GROUP BY pr.id ORDER BY total DESC",
        args
    )
    por_programa_list = [dict(r) for r in por_programa]
    for prog_dict in por_programa_list:
        prog_dict['anr_real'] = anr_real_by_prog.get(prog_dict['id'], 0)

    # Por estado
    por_estado = query(
        f"SELECT p.estado, COUNT(*) as n"
        f" FROM proyectos p JOIN programas pr ON p.programa_id=pr.id"
        f" WHERE {w} GROUP BY p.estado ORDER BY n DESC", args
    )

    # Por municipio (top 15)
    por_municipio = query(
        f"SELECT p.municipio, COUNT(*) as n, COALESCE(SUM(p.anr_monto),0) as anr"
        f" FROM proyectos p JOIN programas pr ON p.programa_id=pr.id"
        f" WHERE {w} AND p.municipio IS NOT NULL AND p.municipio != ''"
        f" GROUP BY p.municipio ORDER BY n DESC LIMIT 15", args
    )

    # Evolución anual
    por_anio = query(
        f"SELECT COALESCE(p.anio, p.anio_aprobacion) as anio,"
        f" COUNT(*) as n, COALESCE(SUM(p.anr_monto),0) as anr"
        f" FROM proyectos p JOIN programas pr ON p.programa_id=pr.id"
        f" WHERE {w} AND COALESCE(p.anio, p.anio_aprobacion) IS NOT NULL"
        f" GROUP BY 1 ORDER BY 1", args
    )

    # Avance promedio por programa
    avance_prog = query(
        f"SELECT pr.codigo, pr.color, COALESCE(AVG(p.porcentaje_avance),0) as avance"
        f" FROM programas pr LEFT JOIN proyectos p ON p.programa_id=pr.id AND {w}"
        f" WHERE pr.activo=1 GROUP BY pr.id", args
    )

    # Análisis por dimensión libre
    dimension_data = {}
    DIMENSIONES_VALIDAS = {
        'municipio', 'linea', 'area_tematica', 'sector_tema',
        'sector_actividad_1', 'tipo_beneficiario', 'tipo_adoptante',
        'seccion', 'rubro', 'situacion_clinica',
        'anio', 'anio_aprobacion', 'estado',
    }
    if dimension and dimension in DIMENSIONES_VALIDAS:
        dim_field = f'COALESCE(p.{dimension},p.anio_aprobacion)' if dimension == 'anio' else f'p.{dimension}'
        rows = query(
            f"SELECT {dim_field} as dim_val,"
            f" COUNT(*) as n,"
            f" COALESCE(SUM(p.anr_monto),0) as anr,"
            f" COALESCE(AVG(p.porcentaje_avance),0) as avance_prom"
            f" FROM proyectos p JOIN programas pr ON p.programa_id=pr.id"
            f" WHERE {w} AND {dim_field} IS NOT NULL AND {dim_field}!=''"
            f" GROUP BY 1 ORDER BY n DESC LIMIT 20", args
        )
        labels_dim = [r['dim_val'] for r in rows]

        # ANR real por valor de dimensión (agrega por programa con IPC propio)
        anr_real_dim = {lbl: 0.0 for lbl in labels_dim}
        dim_field_proy = dim_field.replace('p.', 'proy.')
        for prog in programas_todos:
            if programa_ids and str(prog['id']) not in programa_ids:
                continue
            ipc_join_d, has_ipc_d = build_ipc_join(None, ipc_fecha_val, programa_id=prog['id'], alias='proy')
            real_expr_d = ipc_anr_expr(has_ipc_d, alias='proy')
            prog_where_d = [f'proy.programa_id={prog["id"]}',
                            f'{dim_field_proy} IS NOT NULL', f"{dim_field_proy}!=''"]
            extra_args_d = []
            if estados_sel:
                prog_where_d.append(f"proy.estado IN ({','.join('?'*len(estados_sel))})")
                extra_args_d.extend(estados_sel)
            if anio_desde:
                prog_where_d.append(f'CAST(COALESCE(proy.anio,proy.anio_aprobacion,0) AS INTEGER)>={anio_desde}')
            if anio_hasta:
                prog_where_d.append(f'CAST(COALESCE(proy.anio,proy.anio_aprobacion,0) AS INTEGER)<={anio_hasta}')
            if municipio_sel:
                prog_where_d.append('proy.municipio=?')
                extra_args_d.append(municipio_sel)
            if ib2_sel:
                prog_where_d.append('proy.ib2=?')
                extra_args_d.append(ib2_sel)
            r_real_d = query(
                f"SELECT {dim_field_proy} as dim_val, COALESCE(SUM({real_expr_d}),0) as anr_real"
                f" FROM proyectos proy {ipc_join_d}"
                f" WHERE {' AND '.join(prog_where_d)} GROUP BY 1",
                extra_args_d
            )
            for rr in r_real_d:
                if rr['dim_val'] in anr_real_dim:
                    anr_real_dim[rr['dim_val']] += rr['anr_real']

        dimension_data = {
            'campo':           dimension,
            'label':           COLUMNAS_META.get(dimension, {}).get('label', dimension),
            'labels':          labels_dim,
            'values_n':        [r['n'] for r in rows],
            'values_anr':      [round(r['anr']) for r in rows],
            'values_anr_real': [round(anr_real_dim.get(lbl, 0)) for lbl in labels_dim],
            'values_avance':   [round(r['avance_prom']) for r in rows],
        }

    # Resumen ejecutivo
    hoy = datetime.now().strftime('%d/%m/%Y')
    resumen = _generar_resumen_ejecutivo(kpis, por_programa_list, hoy)

    return jsonify({
        'kpis':        kpis,
        'por_programa': por_programa_list,
        'por_estado':  [dict(r) for r in por_estado],
        'por_municipio': [dict(r) for r in por_municipio],
        'por_anio':    [dict(r) for r in por_anio],
        'avance_prog': [dict(r) for r in avance_prog],
        'dimension':   dimension_data,
        'ipc_fecha':   ipc_fecha_val,
        'resumen':     resumen,
        'generado':    hoy,
    })


# ─── Export Excel dinámico ────────────────────────────────────────────────────

@bp.route('/reportes/exportar')
@login_required
def exportar():
    programa_id = request.args.get('programa_id', '').strip()
    prog_codigo = _prog_codigo(programa_id) if programa_id else None

    # Columnas a incluir (si no vienen, usar las default)
    cols_param = request.args.getlist('cols')
    if not cols_param:
        cols_param = [k for k, m in COLUMNAS_META.items()
                      if m.get('default') and
                      (not prog_codigo or not m.get('progs') or prog_codigo in m['progs'])]

    # Construir SELECT
    select_parts = []
    col_labels   = {}
    for col in cols_param:
        if col not in COLUMNAS_META:
            continue
        meta = COLUMNAS_META[col]
        if col == 'programa':
            select_parts.append("pr.nombre as _prog")
            col_labels['_prog'] = 'Programa'
        elif col == 'agente':
            select_parts.append("u.nombre || ' ' || u.apellido as _agente")
            col_labels['_agente'] = 'Agente'
        else:
            select_parts.append(f'p.{col}')
            col_labels[col] = meta['label']

    if not select_parts:
        select_parts = ['p.nombre', 'p.estado', 'pr.nombre as _prog']
        col_labels = {'nombre': 'Nombre', 'estado': 'Estado', '_prog': 'Programa'}

    sql = (f"SELECT {', '.join(select_parts)}"
           " FROM proyectos p"
           " JOIN programas pr ON p.programa_id=pr.id"
           " LEFT JOIN users u ON p.agente_id=u.id"
           " WHERE 1=1")
    args = []

    if programa_id:
        sql += ' AND p.programa_id=?'
        args.append(programa_id)

    sql, args = _apply_dynamic_filters(sql, args, prog_codigo)
    sql += ' ORDER BY pr.nombre, p.nombre'

    proyectos = query(sql, args)
    df = pd.DataFrame([dict(p) for p in proyectos])

    if not df.empty:
        rename = {c: col_labels.get(c, COLUMNAS_META.get(c, {}).get('label', c)) for c in df.columns}
        df = df.rename(columns=rename)
        df = df.dropna(axis=1, how='all')

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        sheet = 'Proyectos' if not df.empty else 'Sin datos'
        df_out = df if not df.empty else pd.DataFrame({'Sin resultados para los filtros aplicados': []})
        df_out.to_excel(writer, sheet_name=sheet, index=False)
        wb = writer.book
        ws = writer.sheets[sheet]

        hdr_fmt = wb.add_format({
            'bold': True, 'bg_color': '#1E325C',
            'font_color': 'white', 'border': 1, 'text_wrap': True,
        })
        num_fmt  = wb.add_format({'num_format': '#,##0', 'border': 1})
        pct_fmt  = wb.add_format({'num_format': '0"%"', 'border': 1})
        cell_fmt = wb.add_format({'border': 1})

        MONEY_LABELS = {'ANR nominal ($)', 'Monto diagnóstico ($)'}
        PCT_LABELS   = {'Avance (%)'}

        for ci, col in enumerate(df_out.columns):
            ws.write(0, ci, col, hdr_fmt)
            w = max(len(str(col)) + 4, 14)
            ws.set_column(ci, ci, w)

        for ri, row in df_out.iterrows():
            for ci, val in enumerate(row):
                col = df_out.columns[ci]
                fmt = num_fmt if col in MONEY_LABELS else pct_fmt if col in PCT_LABELS else cell_fmt
                ws.write(ri + 1, ci, val, fmt)

        # Hoja de metadata
        ws_meta = wb.add_worksheet('Filtros aplicados')
        mf = wb.add_format({'bold': True})
        ws_meta.write(0, 0, 'Parámetro', mf);  ws_meta.write(0, 1, 'Valor')
        ws_meta.write(1, 0, 'Programa', mf)
        ws_meta.write(1, 1, prog_codigo or 'Todos')
        row_idx = 2
        for key, meta in COLUMNAS_META.items():
            if not meta.get('filterable') or meta.get('computed'):
                continue
            ftype = meta.get('ftype')
            val = ''
            if ftype in ('text', 'dynamic', 'estado'):
                val = request.args.get(f'f_{key}', '')
            elif ftype == 'anio':
                d = request.args.get(f'f_{key}_desde', '')
                h = request.args.get(f'f_{key}_hasta', '')
                if d or h:
                    val = f"{d or '...'} – {h or '...'}"
            elif ftype in ('rango_monto', 'rango_numero'):
                mn = request.args.get(f'f_{key}_min', '')
                mx = request.args.get(f'f_{key}_max', '')
                if mn or mx:
                    val = f"{mn or '...'} – {mx or '...'}"
            if val:
                ws_meta.write(row_idx, 0, meta['label'], mf)
                ws_meta.write(row_idx, 1, val)
                row_idx += 1
        ws_meta.write(row_idx, 0, 'Generado', mf)
        ws_meta.write(row_idx, 1, datetime.now().strftime('%d/%m/%Y %H:%M'))
        ws_meta.set_column(0, 0, 22)
        ws_meta.set_column(1, 1, 35)

    output.seek(0)
    ts = datetime.now().strftime('%Y%m%d_%H%M')
    prog_tag = f'_{prog_codigo}' if prog_codigo else ''
    fname = f'reporte{prog_tag}_{ts}.xlsx'
    return send_file(output, download_name=fname, as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# ─── Herramienta IPC (sin cambios) ────────────────────────────────────────────

@bp.route('/herramientas/ipc', methods=['GET', 'POST'])
@login_required
def ipc():
    ultima_fecha, primera_fecha = get_ipc_config()
    periodos_disp = [r['fecha_desembolso'] for r in
        query("SELECT DISTINCT fecha_desembolso FROM ponderadores_ipc ORDER BY fecha_desembolso")]
    programas = query("SELECT * FROM programas WHERE activo=1 ORDER BY id")

    ipc_rules = {}
    for p in programas:
        rule = get_ipc_rule(p['id'])
        ipc_rules[p['id']] = {
            'tiene_regla': bool(rule),
            'label':       ipc_fecha_desemb_label(p['id']) if rule else None,
        }

    resultados, params_form, errores = [], {}, []
    total_nominal = total_real = 0

    if request.method == 'POST':
        programa_id = request.form.get('programa_id', '')
        fecha_val   = request.form.get('fecha_valuacion', ultima_fecha)
        modo_desemb = request.form.get('modo_desembolso', 'auto')
        mes_fijo    = request.form.get('mes_fijo', '')

        params_form = dict(request.form)
        if not programa_id: errores.append('Selecciona un programa.')
        if not fecha_val:   errores.append('Selecciona la fecha de valuación.')

        if not errores:
            rule = get_ipc_rule(int(programa_id))
            proy_list = query(
                "SELECT id, codigo, nombre, anio, anio_redaccion, anio_aprobacion,"
                " periodo_facturacion, anr_monto, monto_diagnostico, municipio,"
                " beneficiario, ib2, adoptante, estado, linea"
                " FROM proyectos"
                " WHERE programa_id=? AND (anr_monto > 0 OR monto_diagnostico > 0)"
                " ORDER BY codigo", (programa_id,))

            meses_es = {'Ene': '01', 'Feb': '02', 'Mar': '03', 'Abr': '04',
                        'May': '05', 'Jun': '06', 'Jul': '07', 'Ago': '08',
                        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dic': '12'}

            for p in proy_list:
                monto_nom = p['anr_monto'] or p['monto_diagnostico'] or 0
                if not monto_nom:
                    continue

                fecha_desemb = None
                if modo_desemb == 'fijo':
                    fecha_desemb = mes_fijo or None
                elif modo_desemb == 'periodo_facturacion':
                    pf = p['periodo_facturacion']
                    if pf:
                        parts = pf.split()
                        if len(parts) == 2:
                            fecha_desemb = f"{parts[1]}-{meses_es.get(parts[0], '01')}"
                else:
                    if rule:
                        campo    = rule['campo_anio']
                        anio_val = p[campo]
                        if anio_val:
                            fecha_desemb = f"{int(anio_val)+rule['anio_offset']}-{rule['mes_desembolso']}"
                    else:
                        anio_val = p['anio'] or p['anio_aprobacion']
                        if anio_val:
                            fecha_desemb = f"{int(anio_val)}-06"

                ib_label = p['ib2'] or p['beneficiario'] or ''

                if not fecha_desemb:
                    resultados.append({'codigo': p['codigo'], 'nombre': p['nombre'],
                        'monto_nominal': monto_nom, 'fecha_desembolso': '---',
                        'ponderador': None, 'monto_real': None,
                        'error': 'Sin fecha de desembolso',
                        'municipio': p['municipio'], 'beneficiario': ib_label,
                        'estado': p['estado'], 'linea': p['linea']})
                    continue

                pond_row = query(
                    "SELECT ponderador FROM ponderadores_ipc WHERE fecha_desembolso=? AND fecha_valuacion=?",
                    (fecha_desemb, fecha_val), one=True)

                if not pond_row:
                    resultados.append({'codigo': p['codigo'], 'nombre': p['nombre'],
                        'monto_nominal': monto_nom, 'fecha_desembolso': fecha_desemb,
                        'ponderador': None, 'monto_real': None,
                        'error': f'Sin ponderador ({fecha_desemb} a {fecha_val})',
                        'municipio': p['municipio'], 'beneficiario': ib_label,
                        'estado': p['estado'], 'linea': p['linea']})
                    continue

                ponderador   = pond_row['ponderador']
                monto_real   = round(monto_nom * ponderador)
                total_nominal += monto_nom
                total_real    += monto_real
                resultados.append({'codigo': p['codigo'], 'nombre': p['nombre'],
                    'monto_nominal': monto_nom, 'fecha_desembolso': fecha_desemb,
                    'ponderador': ponderador, 'monto_real': monto_real, 'error': None,
                    'municipio': p['municipio'], 'beneficiario': ib_label,
                    'estado': p['estado'], 'linea': p['linea']})

    return render_template('reports/ipc.html',
        programas=programas, ipc_rules=ipc_rules,
        periodos_disp=periodos_disp, ultima_fecha=ultima_fecha, primera_fecha=primera_fecha,
        resultados=resultados, params_form=params_form,
        errores=errores, total_nominal=total_nominal, total_real=total_real)


@bp.route('/herramientas/ipc/exportar', methods=['POST'])
@login_required
def ipc_exportar():
    programa_id = request.form.get('programa_id', '')
    fecha_val   = request.form.get('fecha_valuacion', '2026-02')
    modo_desemb = request.form.get('modo_desembolso', 'auto')
    mes_fijo    = request.form.get('mes_fijo', '')

    prog = query("SELECT * FROM programas WHERE id=?", (programa_id,), one=True)
    rule = get_ipc_rule(int(programa_id)) if programa_id else None

    proy_list = query(
        "SELECT codigo, nombre, anio, anio_redaccion, anio_aprobacion, periodo_facturacion,"
        " anr_monto, monto_diagnostico, municipio, beneficiario, ib2,"
        " adoptante, estado, linea, seccion, uvt"
        " FROM proyectos"
        " WHERE programa_id=? AND (anr_monto > 0 OR monto_diagnostico > 0)"
        " ORDER BY codigo", (programa_id,))

    meses_es = {'Ene': '01', 'Feb': '02', 'Mar': '03', 'Abr': '04',
                'May': '05', 'Jun': '06', 'Jul': '07', 'Ago': '08',
                'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dic': '12'}
    rows = []
    for p in proy_list:
        monto_nom = p['anr_monto'] or p['monto_diagnostico'] or 0
        if not monto_nom:
            continue

        fecha_desemb = None
        if modo_desemb == 'fijo':
            fecha_desemb = mes_fijo
        elif modo_desemb == 'periodo_facturacion':
            pf = p['periodo_facturacion']
            if pf:
                parts = pf.split()
                if len(parts) == 2:
                    fecha_desemb = f"{parts[1]}-{meses_es.get(parts[0], '01')}"
        else:
            if rule:
                anio_val = p[rule['campo_anio']]
                if anio_val:
                    fecha_desemb = f"{int(anio_val)+rule['anio_offset']}-{rule['mes_desembolso']}"
            else:
                anio_val = p['anio'] or p['anio_aprobacion']
                if anio_val:
                    fecha_desemb = f"{int(anio_val)}-06"

        ponderador = monto_real = None
        if fecha_desemb:
            pr = query("SELECT ponderador FROM ponderadores_ipc WHERE fecha_desembolso=? AND fecha_valuacion=?",
                       (fecha_desemb, fecha_val), one=True)
            if pr:
                ponderador = pr['ponderador']
                monto_real = round(monto_nom * ponderador)

        rows.append({
            'Codigo':        p['codigo'],
            'Nombre':        p['nombre'],
            'Beneficiario':  p['ib2'] or p['beneficiario'] or p['adoptante'] or '',
            'Municipio':     p['municipio'] or '',
            'Linea':         p['linea'] or '',
            'Estado':        p['estado'] or '',
            'Mes desembolso':         fecha_desemb or 'Sin datos',
            'Monto nominal ($)':      monto_nom,
            'Ponderador IPC':         ponderador or '',
            f'Monto actualizado {fecha_val} ($)': monto_real or '',
            'Variacion (x)': round(ponderador, 4) if ponderador else '',
        })

    df = pd.DataFrame(rows)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='IPC', index=False)
        wb_xl = writer.book
        ws_xl = writer.sheets['IPC']
        hdr = wb_xl.add_format({'bold': True, 'bg_color': '#1E325C', 'font_color': '#FFFFFF', 'border': 1})
        mon = wb_xl.add_format({'num_format': '#,##0', 'border': 1})
        pon = wb_xl.add_format({'num_format': '0.000', 'border': 1, 'bg_color': '#E0F7FA'})
        rea = wb_xl.add_format({'num_format': '#,##0', 'bold': True, 'bg_color': '#E8F5E9', 'border': 1})
        nor = wb_xl.add_format({'border': 1})
        tf  = wb_xl.add_format({'bold': True, 'bg_color': '#1E325C', 'font_color': '#FFFFFF', 'num_format': '#,##0', 'border': 1})
        for i, col in enumerate(df.columns):
            ws_xl.write(0, i, col, hdr)
            ws_xl.set_column(i, i, max(len(str(col)) + 4, 14))
        for ri, row in df.iterrows():
            for ci, val in enumerate(row):
                col = df.columns[ci]
                fmt = mon if 'nominal' in col else pon if 'Ponderador' in col else rea if 'actualizado' in col else nor
                ws_xl.write(ri + 1, ci, val, fmt)
        last = len(df) + 1
        ws_xl.write(last, 0, 'TOTAL', tf)
        ws_xl.write(last, 7, df['Monto nominal ($)'].sum(), tf)
        rc = f'Monto actualizado {fecha_val} ($)'
        if rc in df.columns:
            ws_xl.write(last, 9, pd.to_numeric(df[rc], errors='coerce').sum(), tf)
        ws_meta = wb_xl.add_worksheet('Parametros')
        mf = wb_xl.add_format({'bold': True})
        ws_meta.write(0, 0, 'Programa',        mf); ws_meta.write(0, 1, prog['nombre'] if prog else programa_id)
        ws_meta.write(1, 0, 'Fecha valuacion',  mf); ws_meta.write(1, 1, fecha_val)
        lbl = ipc_fecha_desemb_label(int(programa_id)) if programa_id else 'Manual'
        ws_meta.write(2, 0, 'Regla desembolso', mf); ws_meta.write(2, 1, lbl or 'Manual')
        ws_meta.write(3, 0, 'Indice',           mf); ws_meta.write(3, 1, 'IPC INDEC')
        ws_meta.write(4, 0, 'Generado',         mf); ws_meta.write(4, 1, datetime.now().strftime('%d/%m/%Y %H:%M'))
        ws_meta.set_column(0, 0, 20); ws_meta.set_column(1, 1, 30)

    output.seek(0)
    prog_codigo = prog['codigo'] if prog else 'todos'
    fname = f"ipc_{prog_codigo}_{fecha_val.replace('-', '')}_{datetime.now().strftime('%Y%m%d')}.xlsx"
    return send_file(output, download_name=fname, as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
