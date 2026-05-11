import io
import os
import re
import uuid
import pandas as pd
from datetime import date, datetime
from werkzeug.utils import secure_filename
from flask import (Blueprint, render_template, request, redirect,
                   url_for, session, flash, send_file, current_app, jsonify)
from database import query, execute, get_db
from helpers.auth import login_required, editor_required, can_edit
from helpers.ipc import get_ipc_config

bp = Blueprint('instituciones', __name__)

def _word_in(term, text):
    """Checks whole-word match to avoid UNM matching UNMDP, etc."""
    return bool(re.search(r'\b' + re.escape(term) + r'\b', text))

TIPOS_DOC = ['convenio', 'estatuto', 'resolucion', 'nota', 'acta', 'otro']
TIPOS_NOVEDAD = ['reunion', 'comunicacion', 'acuerdo', 'conflicto', 'novedad']
TIPOS_INST = ['universidad', 'uvt', 'conicet', 'hospital', 'empresa', 'municipio', 'ong', 'otro']
ESTADOS_VINCULO = ['activo', 'en_gestion', 'pendiente', 'inactivo']


def _docs_folder():
    folder = os.path.join(current_app.config['UPLOAD_FOLDER'], 'instituciones')
    os.makedirs(folder, exist_ok=True)
    return folder


# ─── Listado ──────────────────────────────────────────────────────────────────
@bp.route('/instituciones')
@login_required
def listado():
    q = request.args.get('q', '').strip()
    tipo = request.args.get('tipo', '')
    estado = request.args.get('estado', '')

    ipc_fecha_val, _ = get_ipc_config()

    # Build IPC join for CLINICA (same pattern as adoptantes)
    rule_clinica = query("""SELECT ic.* FROM ipc_config ic
                            JOIN programas pg ON ic.programa_id = pg.id
                            WHERE pg.codigo='CLINICA'""", one=True)
    if rule_clinica:
        campo = rule_clinica['campo_anio']
        offset = rule_clinica['anio_offset']
        mes = rule_clinica['mes_desembolso']
        fecha_expr = (f"(CAST(p.{campo}+{offset} AS TEXT) || '-{mes}')"
                      if offset else
                      f"(CAST(p.{campo} AS TEXT) || '-{mes}')")
        clinica_ipc_join = (
            f" LEFT JOIN ponderadores_ipc ipc_c"
            f" ON pr.codigo='CLINICA'"
            f" AND ipc_c.fecha_desembolso=({fecha_expr})"
            f" AND ipc_c.fecha_valuacion='{ipc_fecha_val}'"
            f" AND p.{campo} IS NOT NULL"
        )
        anr_real_expr = (
            "CASE WHEN pr.codigo='CLINICA'"
            " THEN COALESCE(p.monto_diagnostico*COALESCE(ipc_c.ponderador,1),p.monto_diagnostico,0)"
            " ELSE COALESCE(p.anr_actualizado,p.anr_monto,0) END"
        )
    else:
        clinica_ipc_join = ""
        anr_real_expr = "COALESCE(p.anr_actualizado,p.anr_monto,0)"

    anr_nom_expr = (
        "CASE WHEN pr.codigo='CLINICA' THEN COALESCE(p.monto_diagnostico,0)"
        " ELSE COALESCE(p.anr_monto,0) END"
    )

    # All projects with ANR figures (for Python-side text matching)
    proyectos_all = [dict(r) for r in query(f"""
        SELECT p.id, p.beneficiario, p.ib2, p.uvt,
               {anr_nom_expr} AS anr_nom,
               {anr_real_expr} AS anr_real,
               pr.codigo AS prog_codigo, pr.color AS prog_color
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        {clinica_ipc_join}
    """)]

    # Main instituciones query — only contact/doc counts (no project join to avoid cartesian product)
    sql = """
        SELECT i.*,
               COUNT(DISTINCT ic.id) AS n_contactos,
               COUNT(DISTINCT id2.id) AS n_documentos
        FROM instituciones i
        LEFT JOIN institucion_contactos ic ON ic.institucion_id = i.id AND ic.activo = 1
        LEFT JOIN institucion_documentos id2 ON id2.institucion_id = i.id
        WHERE 1=1
    """
    params = []
    if q:
        sql += " AND (i.nombre LIKE ? OR i.nombre_corto LIKE ?)"
        params += [f'%{q}%', f'%{q}%']
    if tipo:
        sql += " AND i.tipo = ?"
        params.append(tipo)
    if estado:
        sql += " AND i.estado_vinculo = ?"
        params.append(estado)
    sql += " GROUP BY i.id ORDER BY i.nombre"

    instituciones_raw = query(sql, params)

    # Enrich each institution with project stats via text matching (mirrors detail view logic)
    instituciones = []
    for inst in instituciones_raw:
        d = dict(inst)
        n_l  = (d['nombre'] or '').lower().strip()
        nc_l = (d['nombre_corto'] or '').lower().strip()
        nombres = {n for n in [n_l, nc_l] if len(n) >= 2}

        n_proy = 0
        tot_nom = 0.0
        tot_real = 0.0
        prog_set = set()
        color_map = {}

        is_uvt = d.get('tipo') == 'uvt'
        for p in proyectos_all:
            if p['prog_codigo'] == 'FITBA' and is_uvt:
                ib_text = (p['uvt'] or '').lower()
            elif p['prog_codigo'] == 'FITBA':
                ib_text = (p['ib2'] or '').lower()
            else:
                ib_text = (p['beneficiario'] or '').lower()
            for n in nombres:
                if _word_in(n, ib_text):
                    n_proy   += 1
                    tot_nom  += p['anr_nom']  or 0
                    tot_real += p['anr_real'] or 0
                    prog_set.add(p['prog_codigo'])
                    color_map[p['prog_codigo']] = p['prog_color']
                    break

        d['n_proyectos']    = n_proy
        d['total_anr']      = tot_nom
        d['total_anr_real'] = tot_real
        d['programas'] = ','.join(sorted(prog_set))
        d['colores']   = ','.join(color_map.get(c, '') for c in sorted(prog_set))
        instituciones.append(d)

    instituciones.sort(key=lambda x: (-x['n_proyectos'], (x['nombre'] or '').lower()))

    con_proy = [i for i in instituciones if i['n_proyectos'] > 0]
    kpi = {
        'total':           len(instituciones),
        'con_proyectos':   len(con_proy),
        'total_proyectos': sum(i['n_proyectos'] for i in con_proy),
        'total_anr':       sum(i['total_anr']      for i in con_proy),
        'total_anr_real':  sum(i['total_anr_real'] for i in con_proy),
    }

    top_by_proyectos = sorted(con_proy, key=lambda x: -x['n_proyectos'])[:15]
    top_by_anr_nom   = sorted(con_proy, key=lambda x: -x['total_anr'])[:15]
    top_by_anr_real  = sorted(con_proy, key=lambda x: -x['total_anr_real'])[:15]

    ibs          = [i for i in instituciones if i.get('tipo') != 'uvt']
    uvts         = [i for i in instituciones if i.get('tipo') == 'uvt']
    ib_con_proy  = [i for i in ibs  if i['n_proyectos'] > 0]
    uvt_con_proy = [i for i in uvts if i['n_proyectos'] > 0]

    kpi_ib = {
        'total':           len(ibs),
        'con_proyectos':   len(ib_con_proy),
        'total_proyectos': sum(i['n_proyectos']    for i in ib_con_proy),
        'total_anr':       sum(i['total_anr']      for i in ib_con_proy),
        'total_anr_real':  sum(i['total_anr_real'] for i in ib_con_proy),
    }
    kpi_uvt = {
        'total':           len(uvts),
        'con_proyectos':   len(uvt_con_proy),
        'total_proyectos': sum(i['n_proyectos']    for i in uvt_con_proy),
        'total_anr':       sum(i['total_anr']      for i in uvt_con_proy),
        'total_anr_real':  sum(i['total_anr_real'] for i in uvt_con_proy),
    }

    top_ib_by_proyectos  = sorted(ib_con_proy,  key=lambda x: -x['n_proyectos'])[:15]
    top_ib_by_anr_nom    = sorted(ib_con_proy,  key=lambda x: -x['total_anr'])[:15]
    top_ib_by_anr_real   = sorted(ib_con_proy,  key=lambda x: -x['total_anr_real'])[:15]
    top_uvt_by_proyectos = sorted(uvt_con_proy, key=lambda x: -x['n_proyectos'])[:15]
    top_uvt_by_anr_nom   = sorted(uvt_con_proy, key=lambda x: -x['total_anr'])[:15]
    top_uvt_by_anr_real  = sorted(uvt_con_proy, key=lambda x: -x['total_anr_real'])[:15]

    # IB: cuántas IBs participan en cada programa
    ib_por_programa = {}
    for inst in ibs:
        progs = (inst.get('programas') or '').split(',')
        cols  = (inst.get('colores')   or '').split(',')
        for j, prog in enumerate(progs):
            prog = prog.strip()
            if not prog:
                continue
            col = cols[j].strip() if j < len(cols) else '#666'
            if prog not in ib_por_programa:
                ib_por_programa[prog] = {'codigo': prog, 'color': col, 'n_ibs': 0}
            ib_por_programa[prog]['n_ibs'] += 1
    ib_por_programa = sorted(ib_por_programa.values(), key=lambda x: -x['n_ibs'])

    # UVT: estadísticas desde FITBA (tasa de éxito, calificación)
    fitba_uvt_rows = query("""
        SELECT p.uvt, p.estado, COALESCE(p.anr_monto, 0) AS anr
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE pr.codigo = 'FITBA' AND p.uvt IS NOT NULL AND TRIM(p.uvt) != ''
    """) or []

    fitba_by_uvt = {}
    for r in fitba_uvt_rows:
        name = (r['uvt'] or '').strip()
        if not name:
            continue
        if name not in fitba_by_uvt:
            fitba_by_uvt[name] = {'total': 0, 'activos': 0, 'anr': 0.0}
        fitba_by_uvt[name]['total']  += 1
        fitba_by_uvt[name]['anr']    += float(r['anr'] or 0)
        if (r['estado'] or '') == 'activo':
            fitba_by_uvt[name]['activos'] += 1

    uvt_stats = []
    for inst in uvts:
        nombre = (inst.get('nombre') or '').strip()
        nc     = (inst.get('nombre_corto') or '').strip()
        stats  = fitba_by_uvt.get(nombre) or fitba_by_uvt.get(nc)
        if not stats:
            for nm in [n for n in [nombre.lower(), nc.lower()] if n]:
                for k, v in fitba_by_uvt.items():
                    if _word_in(nm, k.lower()):
                        stats = v
                        break
                if stats:
                    break
        total   = stats['total']   if stats else 0
        activos = stats['activos'] if stats else 0
        anr     = stats['anr']     if stats else 0.0
        tasa    = activos / total  if total > 0 else None
        uvt_stats.append({
            'id':             inst['id'],
            'nombre':         inst['nombre'],
            'nombre_corto':   inst.get('nombre_corto'),
            'municipio':      inst.get('municipio'),
            'estado_vinculo': inst.get('estado_vinculo'),
            'total_fitba':    total,
            'activos_fitba':  activos,
            'tasa_exito':     tasa,
            'anr_fitba':      anr,
        })

    max_total_uvt = max((s['total_fitba'] for s in uvt_stats), default=1) or 1
    max_anr_uvt   = max((s['anr_fitba']   for s in uvt_stats), default=1) or 1
    for s in uvt_stats:
        if s['total_fitba'] == 0:
            s['score'] = None
            s['score_label'] = None
        else:
            tasa  = s['tasa_exito'] or 0
            vol   = s['total_fitba'] / max_total_uvt
            anr_n = s['anr_fitba']   / max_anr_uvt
            raw   = tasa * 5 + vol * 3 + anr_n * 2   # 0–10
            s['score']       = round(raw, 1)
            s['score_label'] = 'Alta' if raw >= 7 else ('Media' if raw >= 4 else 'Baja')
    uvt_stats.sort(key=lambda x: (-(x['score'] or -1), (x['nombre'] or '').lower()))

    uvts_con_fitba = [s for s in uvt_stats if s['total_fitba'] > 0]
    uvts_con_tasa  = [s for s in uvts_con_fitba if s['tasa_exito'] is not None]
    kpi_uvt_extra  = {
        'con_fitba':     len(uvts_con_fitba),
        'total_fitba':   sum(s['total_fitba'] for s in uvts_con_fitba),
        'tasa_promedio': (sum(s['tasa_exito'] for s in uvts_con_tasa) / len(uvts_con_tasa)
                          if uvts_con_tasa else 0),
        'calificadas':   len([s for s in uvt_stats if s['score'] is not None]),
    }

    return render_template('instituciones/list.html',
                           instituciones=instituciones,
                           tipos=TIPOS_INST, estados=ESTADOS_VINCULO,
                           q=q, tipo=tipo, estado=estado,
                           kpi=kpi, kpi_ib=kpi_ib, kpi_uvt=kpi_uvt, kpi_uvt_extra=kpi_uvt_extra,
                           top_by_proyectos=top_by_proyectos,
                           top_by_anr_nom=top_by_anr_nom,
                           top_by_anr_real=top_by_anr_real,
                           top_ib_by_proyectos=top_ib_by_proyectos,
                           top_ib_by_anr_nom=top_ib_by_anr_nom,
                           top_ib_by_anr_real=top_ib_by_anr_real,
                           top_uvt_by_proyectos=top_uvt_by_proyectos,
                           top_uvt_by_anr_nom=top_uvt_by_anr_nom,
                           top_uvt_by_anr_real=top_uvt_by_anr_real,
                           ib_por_programa=ib_por_programa,
                           uvt_stats=uvt_stats,
                           ipc_fecha_val=ipc_fecha_val)


# ─── Detalle ──────────────────────────────────────────────────────────────────
@bp.route('/instituciones/<int:iid>')
@login_required
def detail(iid):
    inst = query("SELECT * FROM instituciones WHERE id=?", (iid,), one=True)
    if not inst:
        flash('Institución no encontrada.', 'danger')
        return redirect(url_for('instituciones.listado'))

    contactos = query("""
        SELECT ic.*, u.nombre as reg_nombre FROM institucion_contactos ic
        LEFT JOIN users u ON ic.created_at IS NOT NULL
        WHERE ic.institucion_id = ? AND ic.activo = 1 ORDER BY ic.nombre
    """, (iid,))

    documentos = query("""
        SELECT id2.*, u.nombre as subido_por_nombre, u.apellido as subido_por_apellido
        FROM institucion_documentos id2
        LEFT JOIN users u ON id2.subido_por = u.id
        WHERE id2.institucion_id = ? ORDER BY id2.created_at DESC
    """, (iid,))

    novedades = query("""
        SELECT n.*, u.nombre as reg_nombre, u.apellido as reg_apellido
        FROM institucion_novedades n
        LEFT JOIN users u ON n.registrado_por = u.id
        WHERE n.institucion_id = ? ORDER BY n.fecha DESC, n.created_at DESC
    """, (iid,))

    # Build IPC expressions for real ANR (same pattern as list())
    ipc_fecha_val, _ = get_ipc_config()
    rule_clinica = query("""SELECT ic.* FROM ipc_config ic
                            JOIN programas pg ON ic.programa_id = pg.id
                            WHERE pg.codigo='CLINICA'""", one=True)
    if rule_clinica:
        campo  = rule_clinica['campo_anio']
        offset = rule_clinica['anio_offset']
        mes    = rule_clinica['mes_desembolso']
        fecha_expr = (f"(CAST(p.{campo}+{offset} AS TEXT) || '-{mes}')"
                      if offset else
                      f"(CAST(p.{campo} AS TEXT) || '-{mes}')")
        clinica_ipc_join = (
            f" LEFT JOIN ponderadores_ipc ipc_c"
            f" ON pr.codigo='CLINICA'"
            f" AND ipc_c.fecha_desembolso=({fecha_expr})"
            f" AND ipc_c.fecha_valuacion='{ipc_fecha_val}'"
            f" AND p.{campo} IS NOT NULL"
        )
        anr_real_expr = (
            "CASE WHEN pr.codigo='CLINICA'"
            " THEN COALESCE(p.monto_diagnostico*COALESCE(ipc_c.ponderador,1),p.monto_diagnostico,0)"
            " ELSE COALESCE(p.anr_actualizado,p.anr_monto,0) END"
        )
    else:
        clinica_ipc_join = ""
        anr_real_expr = "COALESCE(p.anr_actualizado,p.anr_monto,0)"

    anr_nom_expr = (
        "CASE WHEN pr.codigo='CLINICA' THEN COALESCE(p.monto_diagnostico,0)"
        " ELSE COALESCE(p.anr_monto,0) END"
    )

    # Proyectos vinculados (por nombre_corto o nombre en beneficiario / ib2 / uvt)
    terminos = [inst['nombre']]
    if inst['nombre_corto']:
        terminos.append(inst['nombre_corto'])
    proyectos_vinculados = []
    seen_ids = set()
    is_uvt = inst['tipo'] == 'uvt'
    fitba_match_field = 'p.uvt' if is_uvt else 'p.ib2'
    for t in terminos:
        t_lower = t.lower()
        rows = query(f"""
            SELECT p.id, p.nombre, p.codigo, p.estado, p.anio, p.anio_redaccion,
                   p.beneficiario, p.ib2, p.uvt,
                   {anr_nom_expr}  AS anr_nom,
                   {anr_real_expr} AS anr_real,
                   pr.nombre AS prog_nombre, pr.codigo AS prog_codigo, pr.color AS prog_color
            FROM proyectos p
            JOIN programas pr ON p.programa_id = pr.id
            {clinica_ipc_join}
            WHERE (pr.codigo != 'FITBA' AND p.beneficiario LIKE ?)
               OR (pr.codigo  = 'FITBA' AND {fitba_match_field} LIKE ?)
            ORDER BY p.anio IS NULL, p.anio DESC, p.nombre
        """, (f'%{t}%', f'%{t}%'))
        for r in rows:
            if r['prog_codigo'] == 'FITBA' and is_uvt:
                ib_text = (r['uvt'] or '').lower()
            elif r['prog_codigo'] == 'FITBA':
                ib_text = (r['ib2'] or '').lower()
            else:
                ib_text = (r['beneficiario'] or '').lower()
            if r['id'] not in seen_ids and _word_in(t_lower, ib_text):
                d = dict(r)
                if d['prog_codigo'] == 'ORBITA' and not d.get('anio') and d.get('anio_redaccion'):
                    d['anio'] = d['anio_redaccion']
                proyectos_vinculados.append(d)
                seen_ids.add(r['id'])

    # Aggregates for proyectos-tab charts
    by_programa = {}
    by_anio     = {}
    by_estado   = {}
    by_programa_act = {}
    by_anio_act     = {}
    for p in proyectos_vinculados:
        cod  = p['prog_codigo']
        anio = str(p['anio']) if p['anio'] else 'S/D'
        est  = p['estado'] or 'sin_estado'
        nom  = float(p['anr_nom']  or 0)
        real = float(p['anr_real'] or 0)

        if cod not in by_programa:
            by_programa[cod] = {'codigo': cod, 'color': p['prog_color'],
                                'n': 0, 'anr_nom': 0.0, 'anr_real': 0.0}
        by_programa[cod]['n']        += 1
        by_programa[cod]['anr_nom']  += nom
        by_programa[cod]['anr_real'] += real

        if anio not in by_anio:
            by_anio[anio] = {'anio': anio, 'n': 0, 'anr_nom': 0.0, 'anr_real': 0.0}
        by_anio[anio]['n']        += 1
        by_anio[anio]['anr_nom']  += nom
        by_anio[anio]['anr_real'] += real

        if est not in by_estado:
            by_estado[est] = {'estado': est, 'n': 0}
        by_estado[est]['n'] += 1

        if est == 'activo':
            if cod not in by_programa_act:
                by_programa_act[cod] = {'codigo': cod, 'color': p['prog_color'],
                                        'n': 0, 'anr_nom': 0.0, 'anr_real': 0.0}
            by_programa_act[cod]['n']        += 1
            by_programa_act[cod]['anr_nom']  += nom
            by_programa_act[cod]['anr_real'] += real

            if anio not in by_anio_act:
                by_anio_act[anio] = {'anio': anio, 'n': 0, 'anr_nom': 0.0, 'anr_real': 0.0}
            by_anio_act[anio]['n']        += 1
            by_anio_act[anio]['anr_nom']  += nom
            by_anio_act[anio]['anr_real'] += real

    activos_list = [p for p in proyectos_vinculados if (p['estado'] or '') == 'activo']
    kpi_proy = {
        'total':          len(proyectos_vinculados),
        'total_anr_nom':  sum(float(p['anr_nom']  or 0) for p in proyectos_vinculados),
        'total_anr_real': sum(float(p['anr_real'] or 0) for p in proyectos_vinculados),
        'n_programas':    len(by_programa),
        'n_anios':        len(by_anio),
        'total_act':          len(activos_list),
        'total_anr_nom_act':  sum(float(p['anr_nom']  or 0) for p in activos_list),
        'total_anr_real_act': sum(float(p['anr_real'] or 0) for p in activos_list),
        'n_programas_act':    len(by_programa_act),
        'n_anios_act':        len(by_anio_act),
    }

    tab = request.args.get('tab', 'info')
    return render_template('instituciones/detail.html',
                           inst=inst, contactos=contactos,
                           documentos=documentos, novedades=novedades,
                           proyectos_vinculados=proyectos_vinculados,
                           tipos_doc=TIPOS_DOC, tipos_novedad=TIPOS_NOVEDAD,
                           tab=tab,
                           today=date.today().isoformat(),
                           kpi_proy=kpi_proy,
                           by_programa=list(by_programa.values()),
                           by_anio=sorted(by_anio.values(), key=lambda x: x['anio']),
                           by_programa_act=list(by_programa_act.values()),
                           by_anio_act=sorted(by_anio_act.values(), key=lambda x: x['anio']),
                           by_estado=list(by_estado.values()),
                           ipc_fecha_val=ipc_fecha_val)


# ─── Exportar ─────────────────────────────────────────────────────────────────
@bp.route('/instituciones/exportar')
@login_required
def exportar():
    q = request.args.get('q', '').strip()
    tipo = request.args.get('tipo', '')
    estado = request.args.get('estado', '')
    formato = request.args.get('formato', 'xlsx')

    sql = """
        SELECT i.nombre, i.nombre_corto, i.tipo, i.cuit,
               i.municipio, i.localidad, i.provincia,
               i.estado_vinculo, i.website,
               COUNT(DISTINCT ic.id) AS contactos,
               COUNT(DISTINCT id2.id) AS documentos
        FROM instituciones i
        LEFT JOIN institucion_contactos ic ON ic.institucion_id = i.id AND ic.activo = 1
        LEFT JOIN institucion_documentos id2 ON id2.institucion_id = i.id
        WHERE 1=1
    """
    params = []
    if q:
        sql += " AND (i.nombre LIKE ? OR i.nombre_corto LIKE ?)"
        params += [f'%{q}%', f'%{q}%']
    if tipo:
        sql += " AND i.tipo = ?"
        params.append(tipo)
    if estado:
        sql += " AND i.estado_vinculo = ?"
        params.append(estado)
    sql += " GROUP BY i.id ORDER BY i.nombre"

    rows = query(sql, params)

    col_labels = {
        'nombre': 'Institución', 'nombre_corto': 'Sigla',
        'tipo': 'Tipo', 'cuit': 'CUIT',
        'municipio': 'Municipio', 'localidad': 'Localidad',
        'provincia': 'Provincia', 'estado_vinculo': 'Estado vínculo',
        'website': 'Website', 'contactos': 'Contactos', 'documentos': 'Documentos',
    }

    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame(columns=list(col_labels))
    if not df.empty:
        df = df.rename(columns=col_labels)

    ts = datetime.now().strftime('%Y%m%d_%H%M')

    if formato == 'csv':
        output = io.BytesIO(df.to_csv(index=False).encode('utf-8-sig'))
        output.seek(0)
        return send_file(output, download_name=f'instituciones_{ts}.csv',
                         as_attachment=True, mimetype='text/csv; charset=utf-8')

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_out = df if not df.empty else pd.DataFrame({'Sin resultados': []})
        df_out.to_excel(writer, sheet_name='Instituciones', index=False)
        wb = writer.book
        ws = writer.sheets['Instituciones']
        hdr_fmt  = wb.add_format({'bold': True, 'bg_color': '#1E325C', 'font_color': 'white', 'border': 1})
        cell_fmt = wb.add_format({'border': 1})
        num_fmt  = wb.add_format({'num_format': '0', 'border': 1})
        for ci, col in enumerate(df_out.columns):
            ws.write(0, ci, col, hdr_fmt)
            ws.set_column(ci, ci, max(len(str(col)) + 4, 14))
        for ri, row in df_out.iterrows():
            for ci, val in enumerate(row):
                col = df_out.columns[ci]
                ws.write(ri + 1, ci, val, num_fmt if col in ('Contactos', 'Documentos') else cell_fmt)

    output.seek(0)
    return send_file(output, download_name=f'instituciones_{ts}.xlsx', as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# ─── Alta / Edición ───────────────────────────────────────────────────────────
@bp.route('/instituciones/nueva', methods=['GET', 'POST'])
@editor_required
def nueva():
    if request.method == 'POST':
        nombre = request.form.get('nombre', '').strip()
        if not nombre:
            flash('El nombre es obligatorio.', 'danger')
            return redirect(url_for('instituciones.nueva'))
        iid = execute("""
            INSERT INTO instituciones (nombre, nombre_corto, tipo, cuit, municipio,
                localidad, provincia, website, descripcion, estado_vinculo, notas_vinculo)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            nombre,
            request.form.get('nombre_corto', '').strip() or None,
            request.form.get('tipo', 'universidad'),
            request.form.get('cuit', '').strip() or None,
            request.form.get('municipio', '').strip() or None,
            request.form.get('localidad', '').strip() or None,
            request.form.get('provincia', 'Buenos Aires').strip() or 'Buenos Aires',
            request.form.get('website', '').strip() or None,
            request.form.get('descripcion', '').strip() or None,
            request.form.get('estado_vinculo', 'activo'),
            request.form.get('notas_vinculo', '').strip() or None,
        ))
        flash(f'Institución "{nombre}" creada correctamente.', 'success')
        return redirect(url_for('instituciones.detail', iid=iid))
    return render_template('instituciones/form.html',
                           inst=None, tipos=TIPOS_INST, estados=ESTADOS_VINCULO)


@bp.route('/instituciones/<int:iid>/editar', methods=['GET', 'POST'])
@editor_required
def editar(iid):
    inst = query("SELECT * FROM instituciones WHERE id=?", (iid,), one=True)
    if not inst:
        flash('Institución no encontrada.', 'danger')
        return redirect(url_for('instituciones.listado'))
    if request.method == 'POST':
        nombre = request.form.get('nombre', '').strip()
        if not nombre:
            flash('El nombre es obligatorio.', 'danger')
            return redirect(url_for('instituciones.editar', iid=iid))
        execute("""
            UPDATE instituciones SET nombre=?, nombre_corto=?, tipo=?, cuit=?,
                municipio=?, localidad=?, provincia=?, website=?, descripcion=?,
                estado_vinculo=?, notas_vinculo=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (
            nombre,
            request.form.get('nombre_corto', '').strip() or None,
            request.form.get('tipo', 'universidad'),
            request.form.get('cuit', '').strip() or None,
            request.form.get('municipio', '').strip() or None,
            request.form.get('localidad', '').strip() or None,
            request.form.get('provincia', 'Buenos Aires').strip() or 'Buenos Aires',
            request.form.get('website', '').strip() or None,
            request.form.get('descripcion', '').strip() or None,
            request.form.get('estado_vinculo', 'activo'),
            request.form.get('notas_vinculo', '').strip() or None,
            iid,
        ))
        flash('Institución actualizada.', 'success')
        return redirect(url_for('instituciones.detail', iid=iid))
    return render_template('instituciones/form.html',
                           inst=dict(inst), tipos=TIPOS_INST, estados=ESTADOS_VINCULO)


# ─── Importar desde Excel ─────────────────────────────────────────────────────
@bp.route('/instituciones/importar', methods=['POST'])
@editor_required
def importar():
    file = request.files.get('archivo')
    if not file:
        flash('Seleccioná un archivo Excel.', 'danger')
        return redirect(url_for('instituciones.listado'))
    try:
        df = pd.read_excel(file, engine='openpyxl')
        df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]
        importadas, existentes, errores = 0, 0, []

        for i, row in df.iterrows():
            try:
                nombre = str(row.get('nombre', '')).strip()
                if not nombre or nombre == 'nan':
                    continue
                existe = query("SELECT id FROM instituciones WHERE nombre=?", (nombre,), one=True)
                if existe:
                    existentes += 1
                    continue

                def _v(col, default=''):
                    val = str(row.get(col, default)).strip()
                    return None if val in ('', 'nan') else val

                execute("""
                    INSERT INTO instituciones (nombre, nombre_corto, tipo, cuit, municipio,
                        localidad, provincia, website, descripcion, estado_vinculo, notas_vinculo)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (nombre, _v('nombre_corto'), _v('tipo') or 'universidad',
                      _v('cuit'), _v('municipio'), _v('localidad'),
                      _v('provincia') or 'Buenos Aires', _v('website'),
                      _v('descripcion'), _v('estado_vinculo') or 'activo',
                      _v('notas_vinculo')))
                importadas += 1
            except Exception as e:
                errores.append(f"Fila {i+2}: {e}")

        msg = f'{importadas} instituciones importadas, {existentes} ya existentes.'
        if errores:
            msg += f' {len(errores)} errores: ' + '; '.join(errores[:3])
        flash(msg, 'success' if not errores else 'warning')
    except Exception as e:
        flash(f'Error al procesar el archivo: {e}', 'danger')
    return redirect(url_for('instituciones.listado'))


# ─── Sincronizar desde proyectos FITBA ────────────────────────────────────────
@bp.route('/instituciones/sync-fitba', methods=['POST'])
@editor_required
def sync_fitba():
    # Extrae valores únicos de ib2 en proyectos FITBA
    rows = query("""
        SELECT DISTINCT ib2 as nombre FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE pr.codigo = 'FITBA' AND p.ib2 IS NOT NULL AND p.ib2 != ''
        ORDER BY ib2
    """)
    nuevas, existentes = 0, 0
    for r in rows:
        nombre = r['nombre'].strip()
        if not nombre:
            continue
        existe = query("SELECT id FROM instituciones WHERE nombre=?", (nombre,), one=True)
        if existe:
            existentes += 1
        else:
            execute("INSERT INTO instituciones (nombre, tipo, estado_vinculo) VALUES (?,?,?)",
                    (nombre, 'universidad', 'activo'))
            nuevas += 1
    flash(f'Sincronización completada: {nuevas} instituciones nuevas, {existentes} ya registradas.', 'success')
    return redirect(url_for('instituciones.listado'))


@bp.route('/instituciones/sync-uvt-fitba', methods=['POST'])
@editor_required
def sync_uvt_fitba():
    rows = query("""
        SELECT DISTINCT uvt as nombre FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE pr.codigo = 'FITBA' AND p.uvt IS NOT NULL AND TRIM(p.uvt) != ''
        ORDER BY uvt
    """)
    nuevas, existentes = 0, 0
    for r in rows:
        nombre = r['nombre'].strip()
        if not nombre:
            continue
        if query("SELECT id FROM instituciones WHERE nombre=?", (nombre,), one=True):
            existentes += 1
        else:
            execute("INSERT INTO instituciones (nombre, tipo, estado_vinculo) VALUES (?,?,?)",
                    (nombre, 'uvt', 'activo'))
            nuevas += 1
    flash(f'Sincronización UVT completada: {nuevas} nuevas, {existentes} ya registradas.', 'success')
    return redirect(url_for('instituciones.listado'))


@bp.route('/instituciones/sync-proyectos', methods=['POST'])
@editor_required
def sync_proyectos():
    # CLINICA usa beneficiario para la empresa receptora, no para la IB → excluir
    rows = query("""
        SELECT DISTINCT p.beneficiario AS nombre
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE p.beneficiario IS NOT NULL AND TRIM(p.beneficiario) NOT IN ('', '-')
          AND pr.codigo != 'CLINICA'
        UNION
        SELECT DISTINCT p.ib2 AS nombre
        FROM proyectos p
        WHERE p.ib2 IS NOT NULL AND TRIM(p.ib2) NOT IN ('', '-')
        ORDER BY nombre
    """)
    nuevas, existentes = 0, 0
    for r in rows:
        nombre = (r['nombre'] or '').strip()
        if not nombre:
            continue
        if query("SELECT id FROM instituciones WHERE nombre=?", (nombre,), one=True):
            existentes += 1
        else:
            execute("INSERT INTO instituciones (nombre, tipo, estado_vinculo) VALUES (?,?,?)",
                    (nombre, 'universidad', 'activo'))
            nuevas += 1
    flash(f'Sincronización completada: {nuevas} instituciones nuevas importadas, {existentes} ya registradas.', 'success')
    return redirect(url_for('instituciones.listado'))


# ─── Documentos ───────────────────────────────────────────────────────────────
@bp.route('/instituciones/<int:iid>/documentos/subir', methods=['POST'])
@editor_required
def subir_doc(iid):
    inst = query("SELECT id FROM instituciones WHERE id=?", (iid,), one=True)
    if not inst:
        return redirect(url_for('instituciones.listado'))

    file = request.files.get('archivo')
    nombre_doc = request.form.get('nombre_doc', '').strip()
    if not file or not nombre_doc:
        flash('Nombre y archivo son obligatorios.', 'danger')
        return redirect(url_for('instituciones.detail', iid=iid, tab='documentos'))

    ext = os.path.splitext(secure_filename(file.filename))[1]
    stored_name = f"{uuid.uuid4().hex}{ext}"
    file.save(os.path.join(_docs_folder(), stored_name))

    execute("""
        INSERT INTO institucion_documentos
            (institucion_id, tipo_doc, nombre_doc, filename, descripcion, fecha_doc, subido_por)
        VALUES (?,?,?,?,?,?,?)
    """, (iid,
          request.form.get('tipo_doc', 'otro'),
          nombre_doc,
          stored_name,
          request.form.get('descripcion', '').strip() or None,
          request.form.get('fecha_doc') or None,
          session['user_id']))
    flash('Documento subido correctamente.', 'success')
    return redirect(url_for('instituciones.detail', iid=iid, tab='documentos'))


@bp.route('/instituciones/<int:iid>/documentos/<int:did>/ver')
@login_required
def ver_doc(iid, did):
    doc = query("SELECT * FROM institucion_documentos WHERE id=? AND institucion_id=?", (did, iid), one=True)
    if not doc:
        flash('Documento no encontrado.', 'danger')
        return redirect(url_for('instituciones.detail', iid=iid, tab='documentos'))
    path = os.path.join(_docs_folder(), doc['filename'])
    if not os.path.exists(path):
        flash('Archivo no encontrado en el servidor.', 'danger')
        return redirect(url_for('instituciones.detail', iid=iid, tab='documentos'))
    return send_file(path, as_attachment=False, mimetype='application/pdf')


@bp.route('/instituciones/<int:iid>/documentos/<int:did>/descargar')
@login_required
def descargar_doc(iid, did):
    doc = query("SELECT * FROM institucion_documentos WHERE id=? AND institucion_id=?", (did, iid), one=True)
    if not doc:
        flash('Documento no encontrado.', 'danger')
        return redirect(url_for('instituciones.detail', iid=iid, tab='documentos'))
    path = os.path.join(_docs_folder(), doc['filename'])
    if not os.path.exists(path):
        flash('Archivo no encontrado en el servidor.', 'danger')
        return redirect(url_for('instituciones.detail', iid=iid, tab='documentos'))
    ext = os.path.splitext(doc['filename'])[1]
    return send_file(path, as_attachment=True,
                     download_name=f"{doc['nombre_doc']}{ext}")


@bp.route('/instituciones/<int:iid>/documentos/<int:did>/eliminar', methods=['POST'])
@editor_required
def eliminar_doc(iid, did):
    doc = query("SELECT * FROM institucion_documentos WHERE id=? AND institucion_id=?", (did, iid), one=True)
    if doc:
        path = os.path.join(_docs_folder(), doc['filename'])
        if os.path.exists(path):
            os.remove(path)
        execute("DELETE FROM institucion_documentos WHERE id=?", (did,))
        flash('Documento eliminado.', 'success')
    return redirect(url_for('instituciones.detail', iid=iid, tab='documentos'))


# ─── Contactos ────────────────────────────────────────────────────────────────
@bp.route('/instituciones/<int:iid>/contactos/nuevo', methods=['POST'])
@editor_required
def nuevo_contacto(iid):
    nombre = request.form.get('nombre', '').strip()
    if not nombre:
        flash('El nombre es obligatorio.', 'danger')
        return redirect(url_for('instituciones.detail', iid=iid, tab='contactos'))
    execute("""
        INSERT INTO institucion_contactos
            (institucion_id, nombre, cargo, email, telefono, notas)
        VALUES (?,?,?,?,?,?)
    """, (iid, nombre,
          request.form.get('cargo', '').strip() or None,
          request.form.get('email', '').strip() or None,
          request.form.get('telefono', '').strip() or None,
          request.form.get('notas', '').strip() or None))
    flash('Contacto agregado.', 'success')
    return redirect(url_for('instituciones.detail', iid=iid, tab='contactos'))


@bp.route('/instituciones/<int:iid>/contactos/<int:cid>/eliminar', methods=['POST'])
@editor_required
def eliminar_contacto(iid, cid):
    execute("UPDATE institucion_contactos SET activo=0 WHERE id=? AND institucion_id=?", (cid, iid))
    flash('Contacto eliminado.', 'success')
    return redirect(url_for('instituciones.detail', iid=iid, tab='contactos'))


# ─── Novedades ────────────────────────────────────────────────────────────────
@bp.route('/instituciones/<int:iid>/novedades/nueva', methods=['POST'])
@editor_required
def nueva_novedad(iid):
    titulo = request.form.get('titulo', '').strip()
    if not titulo:
        flash('El título es obligatorio.', 'danger')
        return redirect(url_for('instituciones.detail', iid=iid, tab='novedades'))
    execute("""
        INSERT INTO institucion_novedades
            (institucion_id, titulo, cuerpo, tipo, fecha, registrado_por)
        VALUES (?,?,?,?,?,?)
    """, (iid, titulo,
          request.form.get('cuerpo', '').strip() or None,
          request.form.get('tipo', 'novedad'),
          request.form.get('fecha') or date.today().isoformat(),
          session['user_id']))
    flash('Novedad registrada.', 'success')
    return redirect(url_for('instituciones.detail', iid=iid, tab='novedades'))


@bp.route('/instituciones/<int:iid>/novedades/<int:nid>/eliminar', methods=['POST'])
@editor_required
def eliminar_novedad(iid, nid):
    execute("DELETE FROM institucion_novedades WHERE id=? AND institucion_id=?", (nid, iid))
    flash('Novedad eliminada.', 'success')
    return redirect(url_for('instituciones.detail', iid=iid, tab='novedades'))
