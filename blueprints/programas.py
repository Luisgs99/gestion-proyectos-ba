from collections import defaultdict
from flask import Blueprint, render_template, redirect, url_for, flash, session
from database import query
from helpers.auth import login_required
from helpers.ipc import build_ipc_join, get_ipc_config, ipc_anr_expr

bp = Blueprint('programas', __name__)


# ─── Listado ──────────────────────────────────────────────────────────────────
@bp.route('/programas')
@login_required
def list():
    programas = query("SELECT * FROM programas WHERE activo=1 ORDER BY id")
    return render_template('programs/list.html', programas=programas)


@bp.route('/fitba')
@login_required
def fitba():
    fitba = query("SELECT * FROM programas WHERE codigo='FITBA'", one=True)
    if not fitba:
        return redirect(url_for('programas.list'))
    return redirect(url_for('programas.detail', pid=fitba['id']))


# ─── Detalle genérico / dispatcher ────────────────────────────────────────────
@bp.route('/programas/<int:pid>')
@login_required
def detail(pid):
    programa = query("SELECT * FROM programas WHERE id=?", (pid,), one=True)
    if not programa:
        flash('Programa no encontrado.', 'danger')
        return redirect(url_for('programas.list'))

    if session.get('rol') == 'agente':
        proyectos = query("""
            SELECT p.*, u.nombre as agente_nombre, u.apellido as agente_apellido
            FROM proyectos p
            LEFT JOIN users u ON p.agente_id = u.id
            WHERE p.programa_id=?
              AND (p.agente_id=?
                   OR EXISTS (SELECT 1 FROM asignaciones a
                              WHERE a.proyecto_id=p.id AND a.agente_id=?))
            ORDER BY p.nombre
        """, (pid, session['user_id'], session['user_id']))
    else:
        proyectos = query("""
            SELECT p.*, u.nombre as agente_nombre, u.apellido as agente_apellido
            FROM proyectos p
            LEFT JOIN users u ON p.agente_id = u.id
            WHERE p.programa_id=? ORDER BY p.nombre
        """, (pid,))

    hitos = query("SELECT * FROM hitos WHERE programa_id=? ORDER BY orden", (pid,))

    if programa['codigo'] == 'FITBA':
        return _fitba_detail(pid, programa, proyectos, hitos)
    if programa['codigo'] == 'ORBITA':
        return _orbita_detail(pid, programa, proyectos, hitos)
    if programa['codigo'] == 'CLINICA':
        return _clinica_detail(pid, programa, proyectos, hitos)
    if programa['codigo'] == 'FONICS':
        return _fonics_detail(pid, programa, proyectos, hitos)
    if programa['codigo'] == 'CLIC':
        return _clic_detail(pid, programa, proyectos, hitos)

    # Fallback genérico
    total       = len(proyectos)
    activos     = sum(1 for p in proyectos if p['estado'] == 'activo')
    finalizados = sum(1 for p in proyectos if p['estado'] == 'finalizado')
    total_anr   = sum(p['anr_monto'] or 0 for p in proyectos)
    avg_avance  = (sum(p['porcentaje_avance'] or 0 for p in proyectos) / total) if total > 0 else 0

    avances_por_hito = {}
    for h in hitos:
        completados = query("""
            SELECT COUNT(*) as n FROM avances_hitos ah
            JOIN proyectos p ON ah.proyecto_id = p.id
            WHERE ah.hito_id=? AND p.programa_id=? AND ah.estado='completado'
        """, (h['id'], pid), one=True)['n']
        avances_por_hito[h['id']] = completados

    estados_dist = query("SELECT estado, COUNT(*) as n FROM proyectos WHERE programa_id=? GROUP BY estado", (pid,))
    por_municipio = query("""
        SELECT municipio, COUNT(*) as n FROM proyectos
        WHERE programa_id=? AND municipio IS NOT NULL AND municipio != ''
        GROUP BY municipio ORDER BY n DESC LIMIT 10
    """, (pid,))
    top_anr = query("""
        SELECT nombre, anr_monto FROM proyectos
        WHERE programa_id=? AND anr_monto > 0
        ORDER BY anr_monto DESC LIMIT 10
    """, (pid,))

    return render_template('programs/detail.html',
        programa=programa, proyectos=proyectos, hitos=hitos,
        total=total, activos=activos, finalizados=finalizados,
        total_anr=total_anr, avg_avance=avg_avance,
        avances_por_hito=avances_por_hito,
        estados_dist=[dict(r) for r in estados_dist],
        por_municipio=[dict(r) for r in por_municipio],
        top_anr=[dict(r) for r in top_anr])


# ─── Dashboard FITBA ──────────────────────────────────────────────────────────
def _fitba_detail(pid, programa, proyectos, hitos):
    total        = len(proyectos)
    finalizados  = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='finalizado'", (pid,), one=True)['n']
    activos      = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='activo'", (pid,), one=True)['n']
    en_fin       = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='en_finalizacion'", (pid,), one=True)['n']
    suspendidos  = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='suspendido'", (pid,), one=True)['n']
    total_anr    = query("SELECT COALESCE(SUM(anr_monto),0) as s FROM proyectos WHERE programa_id=?", (pid,), one=True)['s']
    total_anr_act= query("SELECT COALESCE(SUM(anr_actualizado),0) as s FROM proyectos WHERE programa_id=?", (pid,), one=True)['s']
    total_invest = query("SELECT COALESCE(SUM(n_investigadores),0) as s FROM proyectos WHERE programa_id=?", (pid,), one=True)['s']
    total_mujeres= query("SELECT COALESCE(SUM(n_mujeres),0) as s FROM proyectos WHERE programa_id=?", (pid,), one=True)['s']
    directoras   = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND directora_mujer=1", (pid,), one=True)['n']

    por_anio_linea = query("""
        SELECT anio, linea, COUNT(*) as n FROM proyectos
        WHERE programa_id=? AND anio IS NOT NULL AND linea IS NOT NULL
        GROUP BY anio, linea ORDER BY anio, linea
    """, (pid,))
    por_anio = query("""
        SELECT anio, COUNT(*) as n, COALESCE(SUM(anr_monto),0) as total_anr
        FROM proyectos WHERE programa_id=? AND anio IS NOT NULL
        GROUP BY anio ORDER BY anio
    """, (pid,))
    estados_dist = query("SELECT estado, COUNT(*) as n FROM proyectos WHERE programa_id=? GROUP BY estado ORDER BY n DESC", (pid,))
    por_linea = query("""
        SELECT linea, COUNT(*) as n, COALESCE(SUM(anr_monto),0) as total_anr
        FROM proyectos WHERE programa_id=? AND linea IS NOT NULL
        GROUP BY linea ORDER BY linea
    """, (pid,))
    por_municipio = query("""
        SELECT municipio, COUNT(*) as n FROM proyectos
        WHERE programa_id=? AND municipio IS NOT NULL AND municipio != ''
        GROUP BY municipio ORDER BY n DESC LIMIT 15
    """, (pid,))
    top_ib = query("""
        SELECT ib2 as beneficiario, COUNT(*) as n, COALESCE(SUM(anr_monto),0) as total_anr
        FROM proyectos WHERE programa_id=? AND ib2 IS NOT NULL
        GROUP BY ib2 ORDER BY n DESC LIMIT 15
    """, (pid,))

    sectores_act = query("""
        SELECT sector_actividad_1 as sector, COUNT(*) as n FROM proyectos
        WHERE programa_id=? AND sector_actividad_1 IS NOT NULL AND sector_actividad_1 != 'N/C'
        GROUP BY sector_actividad_1
        UNION ALL
        SELECT sector_actividad_2, COUNT(*) FROM proyectos
        WHERE programa_id=? AND sector_actividad_2 IS NOT NULL AND sector_actividad_2 != 'N/C'
        GROUP BY sector_actividad_2
        UNION ALL
        SELECT sector_actividad_3, COUNT(*) FROM proyectos
        WHERE programa_id=? AND sector_actividad_3 IS NOT NULL AND sector_actividad_3 != 'N/C'
        GROUP BY sector_actividad_3
    """, (pid, pid, pid))
    sector_agg = defaultdict(int)
    for r in sectores_act:
        sector_agg[r['sector']] += r['n']
    sectores_sorted = [{'sector': k, 'n': v} for k, v in sorted(sector_agg.items(), key=lambda x: -x[1])[:12]]

    sector_tema = query("""
        SELECT sector_tema, COUNT(*) as n FROM proyectos
        WHERE programa_id=? AND sector_tema IS NOT NULL
        GROUP BY sector_tema ORDER BY n DESC
    """, (pid,))
    por_seccion = query("""
        SELECT seccion, COUNT(*) as n FROM proyectos
        WHERE programa_id=? AND seccion IS NOT NULL
        GROUP BY seccion ORDER BY seccion
    """, (pid,))

    ipc_fecha_val, _ = get_ipc_config()
    IPC_JOIN, _has_ipc = build_ipc_join(None, ipc_fecha_val, programa_id=pid)

    total_anr_real = query(
        f"SELECT COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)),0) as s "
        f"FROM proyectos p {IPC_JOIN} WHERE p.programa_id=?", (pid,), one=True
    )['s']

    top_ib_ipc = query(f"""
        SELECT p.ib2 as beneficiario,
               COUNT(*) as n,
               COALESCE(SUM(p.anr_monto),0) as total_anr,
               COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)),0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.ib2 IS NOT NULL AND p.anio IS NOT NULL
        GROUP BY p.ib2 ORDER BY total_real DESC LIMIT 15
    """, (pid,))

    por_municipio_ipc = query(f"""
        SELECT p.municipio,
               COUNT(*) as n,
               COALESCE(SUM(p.anr_monto),0) as total_anr,
               COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)),0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.municipio IS NOT NULL
              AND p.municipio != '' AND p.anio IS NOT NULL
        GROUP BY p.municipio ORDER BY total_real DESC LIMIT 15
    """, (pid,))

    por_anio_ipc = query(f"""
        SELECT p.anio,
               COUNT(*) as n,
               COALESCE(SUM(p.anr_monto),0) as total_anr,
               COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)),0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.anio IS NOT NULL
        GROUP BY p.anio ORDER BY p.anio
    """, (pid,))

    por_linea_ipc = query(f"""
        SELECT p.linea,
               COUNT(*) as n,
               COALESCE(SUM(p.anr_monto),0) as total_anr,
               COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)),0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.linea IS NOT NULL AND p.anio IS NOT NULL
        GROUP BY p.linea ORDER BY p.linea
    """, (pid,))

    proyectos_con_real = query(f"""
        SELECT p.id, p.codigo, p.nombre, p.anio, p.beneficiario, p.adoptante,
               p.municipio, p.linea, p.seccion, p.anr_monto, p.estado,
               p.situacion_clinica, p.porcentaje_avance,
               COALESCE(p.anr_monto * COALESCE(ipc.ponderador,1), p.anr_monto) as anr_real,
               ipc.ponderador,
               '—' as fecha_desemb
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.anio IS NOT NULL
        ORDER BY p.codigo
    """, (pid,))

    mapa_municipios = query(f"""
        SELECT p.municipio, COUNT(*) as n,
               COALESCE(SUM(p.anr_monto),0) as total_nom,
               COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)),0) as total_real,
               mc.lat, mc.lng
        FROM proyectos p {IPC_JOIN}
        JOIN municipio_coords mc ON mc.municipio = p.municipio
        WHERE p.programa_id=?
        GROUP BY p.municipio ORDER BY n DESC
    """, (pid,))

    # Datos por convocatoria para el filtro del mapa
    mapa_por_anio = {}
    for _anio in [2022, 2023, 2024, 2025]:
        rows_anio = query(f"""
            SELECT p.municipio, COUNT(*) as n,
                   COALESCE(SUM(p.anr_monto),0) as total_nom,
                   COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)),0) as total_real,
                   mc.lat, mc.lng
            FROM proyectos p {IPC_JOIN}
            JOIN municipio_coords mc ON mc.municipio = p.municipio
            WHERE p.programa_id=? AND p.anio=?
            GROUP BY p.municipio ORDER BY n DESC
        """, (pid, _anio))
        mapa_por_anio[str(_anio)] = [dict(r) for r in rows_anio]

    mapa_ib = query("""
        SELECT p.ib2 as beneficiario, COUNT(*) as n,
               COALESCE(SUM(p.anr_monto),0) as total_nom,
               ic.lat, ic.lng
        FROM proyectos p
        JOIN ib_coords ic ON ic.beneficiario_key = p.ib2
        WHERE p.programa_id=?
        GROUP BY p.ib2 ORDER BY n DESC
    """, (pid,))

    con_coords = query("""
        SELECT nombre, beneficiario, adoptante, municipio, anr_monto, estado, latitud, longitud, linea
        FROM proyectos WHERE programa_id=? AND latitud IS NOT NULL AND longitud IS NOT NULL
        LIMIT 300
    """, (pid,))

    return render_template('programs/fitba_dashboard.html',
        programa=programa, proyectos=proyectos, hitos=hitos,
        total=total, activos=activos, finalizados=finalizados,
        en_fin=en_fin, suspendidos=suspendidos,
        total_anr=total_anr, total_anr_act=total_anr_act,
        total_anr_real=total_anr_real, ipc_fecha_val=ipc_fecha_val,
        total_invest=total_invest, total_mujeres=int(total_mujeres or 0),
        directoras=directoras,
        por_anio_linea=[dict(r) for r in por_anio_linea],
        por_anio=[dict(r) for r in por_anio],
        por_anio_ipc=[dict(r) for r in por_anio_ipc],
        estados_dist=[dict(r) for r in estados_dist],
        por_linea=[dict(r) for r in por_linea],
        por_linea_ipc=[dict(r) for r in por_linea_ipc],
        por_municipio=[dict(r) for r in por_municipio],
        por_municipio_ipc=[dict(r) for r in por_municipio_ipc],
        top_ib=[dict(r) for r in top_ib],
        top_ib_ipc=[dict(r) for r in top_ib_ipc],
        sectores_sorted=sectores_sorted,
        sector_tema=[dict(r) for r in sector_tema],
        por_seccion=[dict(r) for r in por_seccion],
        con_coords=[dict(r) for r in con_coords],
        proyectos_con_real=[dict(r) for r in proyectos_con_real],
        mapa_municipios=[dict(r) for r in mapa_municipios],
        mapa_por_anio=mapa_por_anio,
        mapa_ib=[dict(r) for r in mapa_ib],
    )


# ─── Dashboard ORBITA ─────────────────────────────────────────────────────────
def _orbita_detail(pid, programa, proyectos, hitos):
    total = len(proyectos)

    ipc_fecha_val, _ = get_ipc_config()
    IPC_JOIN, has_ipc = build_ipc_join(None, ipc_fecha_val, programa_id=pid)
    real_expr = ipc_anr_expr(has_ipc)

    publicados  = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND situacion_clinica='Publicado'", (pid,), one=True)['n']
    en_proceso  = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND situacion_clinica IS NOT NULL AND situacion_clinica != 'Publicado'", (pid,), one=True)['n']
    total_anr   = query("SELECT COALESCE(SUM(anr_monto),0) as s FROM proyectos WHERE programa_id=?", (pid,), one=True)['s']
    total_anr_real = query(
        f"SELECT COALESCE(SUM({real_expr}),0) as s FROM proyectos p {IPC_JOIN} WHERE p.programa_id=?",
        (pid,), one=True
    )['s']

    pipeline_ed = query("""
        SELECT situacion_clinica as estado_ed, COUNT(*) as n, COALESCE(SUM(anr_monto),0) as total_anr
        FROM proyectos WHERE programa_id=? AND situacion_clinica IS NOT NULL
        GROUP BY situacion_clinica ORDER BY COUNT(*) DESC
    """, (pid,))

    por_linea = query(f"""
        SELECT p.linea,
               COUNT(*) as n,
               SUM(CASE WHEN p.situacion_clinica='Publicado' THEN 1 ELSE 0 END) as publicados,
               COALESCE(SUM(p.anr_monto),0) as total_anr,
               COALESCE(SUM({real_expr}),0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.linea IS NOT NULL
        GROUP BY p.linea ORDER BY n DESC
    """, (pid,))

    por_ib = query(f"""
        SELECT p.ib2 as beneficiario, COUNT(*) as n,
               COALESCE(SUM(p.anr_monto),0) as total_anr,
               COALESCE(SUM({real_expr}),0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.ib2 IS NOT NULL
        GROUP BY p.ib2 ORDER BY n DESC
    """, (pid,))

    por_anio = query(f"""
        SELECT p.anio_redaccion as anio_red, p.anio_publicacion,
               COUNT(*) as n,
               SUM(CASE WHEN p.situacion_clinica='Publicado' THEN 1 ELSE 0 END) as publicados,
               COALESCE(SUM(p.anr_monto),0) as total_anr,
               COALESCE(SUM({real_expr}),0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.anio_redaccion IS NOT NULL
        GROUP BY p.anio_redaccion ORDER BY p.anio_redaccion
    """, (pid,))

    real_per_proy = {}
    if has_ipc:
        rows_real = query(
            "SELECT p.id, ROUND(p.anr_monto * COALESCE(ipc.ponderador, 1)) as ar "
            f"FROM proyectos p {IPC_JOIN} "
            "WHERE p.programa_id=? AND p.anr_monto > 0",
            (pid,))
        for r in rows_real:
            real_per_proy[r['id']] = r['ar']
    proyectos_enrich = []
    for p in proyectos:
        d = dict(p)
        d['anr_real'] = real_per_proy.get(p['id'], p['anr_monto'] or 0)
        proyectos_enrich.append(d)

    return render_template('programs/orbita_dashboard.html',
        programa=programa, proyectos=proyectos_enrich, hitos=hitos,
        total=total, publicados=publicados, en_proceso=en_proceso,
        total_anr=total_anr, total_anr_real=total_anr_real,
        ipc_fecha_val=ipc_fecha_val,
        pipeline_ed=[dict(r) for r in pipeline_ed],
        por_linea=[dict(r) for r in por_linea],
        por_ib=[dict(r) for r in por_ib],
        por_anio=[dict(r) for r in por_anio],
    )


# ─── Dashboard CLÍNICA TECNOLÓGICA ────────────────────────────────────────────
def _clinica_detail(pid, programa, proyectos, hitos):
    total        = len(proyectos)
    finalizados  = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='finalizado'", (pid,), one=True)['n']
    activos      = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='activo'", (pid,), one=True)['n']
    en_fin       = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='en_finalizacion'", (pid,), one=True)['n']
    suspendidos  = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='suspendido'", (pid,), one=True)['n']
    con_monto    = query("SELECT COUNT(*) as n, COALESCE(SUM(monto_diagnostico),0) as s FROM proyectos WHERE programa_id=? AND monto_diagnostico IS NOT NULL", (pid,), one=True)
    total_facturado = con_monto['s']
    monto_promedio  = total_facturado / con_monto['n'] if con_monto['n'] > 0 else 0

    ORDEN_SIT = [
        'Averiguando interés',
        '1.Derivado a Clínico',
        '2.Empresa en stand-by',
        '3.Visita programada',
        '4.Diagnóstico En Proceso',
        '5.Diagnóstico Finalizado',
        '2.Empresa no interesada',
        '0.Empresa no válida',
        '0.Municipio no priorizado',
    ]
    LABELS_SIT = {
        'Averiguando interés':       'Averiguando interés',
        '1.Derivado a Clínico':      'Derivado a Clínico',
        '2.Empresa en stand-by':     'En stand-by',
        '3.Visita programada':       'Visita programada',
        '4.Diagnóstico En Proceso':  'Diagnóstico en proceso',
        '5.Diagnóstico Finalizado':  'Diagnóstico finalizado',
        '2.Empresa no interesada':   'No interesada',
        '0.Empresa no válida':       'Empresa no válida',
        '0.Municipio no priorizado': 'Municipio no priorizado',
    }
    sit_raw = {r['situacion_clinica']: r['n'] for r in query(
        "SELECT situacion_clinica, COUNT(*) as n FROM proyectos WHERE programa_id=? AND situacion_clinica IS NOT NULL GROUP BY situacion_clinica", (pid,))}
    pipeline = [{'situacion': s, 'label': LABELS_SIT.get(s, s), 'n': sit_raw.get(s, 0)} for s in ORDEN_SIT]

    por_anio = query("""
        SELECT anio_aprobacion as anio, COUNT(*) as n,
               SUM(CASE WHEN estado='finalizado' THEN 1 ELSE 0 END) as finalizados,
               SUM(CASE WHEN estado='activo'     THEN 1 ELSE 0 END) as activos,
               SUM(CASE WHEN estado='suspendido' THEN 1 ELSE 0 END) as suspendidos
        FROM proyectos WHERE programa_id=? AND anio_aprobacion IS NOT NULL
        GROUP BY anio_aprobacion ORDER BY anio_aprobacion
    """, (pid,))

    por_municipio = query("""
        SELECT municipio, COUNT(*) as n,
               SUM(CASE WHEN estado='finalizado' THEN 1 ELSE 0 END) as finalizados
        FROM proyectos WHERE programa_id=? AND municipio IS NOT NULL
        GROUP BY municipio ORDER BY n DESC LIMIT 20
    """, (pid,))

    mapa_municipios_clinica = query("""
        SELECT municipio, COUNT(*) as n,
               SUM(CASE WHEN estado='finalizado' THEN 1 ELSE 0 END) as finalizados
        FROM proyectos WHERE programa_id=? AND municipio IS NOT NULL AND municipio != ''
        GROUP BY municipio ORDER BY finalizados DESC
    """, (pid,))

    rubros_raw = query("""
        SELECT rubro, COUNT(*) as n FROM proyectos
        WHERE programa_id=? AND rubro IS NOT NULL
        GROUP BY rubro ORDER BY n DESC LIMIT 18
    """, (pid,))

    por_especialista = query("""
        SELECT especialista,
               COUNT(*) as n,
               SUM(CASE WHEN estado='finalizado' THEN 1 ELSE 0 END) as finalizados,
               SUM(CASE WHEN estado='suspendido' THEN 1 ELSE 0 END) as bajas,
               COALESCE(AVG(CASE WHEN monto_diagnostico IS NOT NULL THEN monto_diagnostico END), 0) as monto_avg
        FROM proyectos
        WHERE programa_id=? AND especialista IS NOT NULL AND especialista NOT IN ('-')
        GROUP BY especialista ORDER BY n DESC LIMIT 20
    """, (pid,))

    estado_emp_dist = query("""
        SELECT estado_empresa, COUNT(*) as n
        FROM proyectos WHERE programa_id=? AND estado_empresa IS NOT NULL
        GROUP BY estado_empresa ORDER BY estado_empresa
    """, (pid,))

    por_periodo = query("""
        SELECT periodo_facturacion, COUNT(*) as n, SUM(monto_diagnostico) as total
        FROM proyectos WHERE programa_id=? AND periodo_facturacion IS NOT NULL
        GROUP BY periodo_facturacion ORDER BY periodo_facturacion DESC LIMIT 12
    """, (pid,))

    convenios = query("""
        SELECT id, anio, financiador, descripcion, monto
        FROM convenios_financiamiento WHERE programa_id=? ORDER BY anio, id
    """, (pid,))
    total_convenios = sum(c['monto'] for c in convenios) if convenios else 0

    ipc_fecha_val, _ = get_ipc_config()
    IPC_JOIN, has_ipc = build_ipc_join(None, ipc_fecha_val, programa_id=pid)
    real_expr = ipc_anr_expr(has_ipc, campo_monto='monto_diagnostico')

    total_real_row = query(
        "SELECT COALESCE(SUM(" + real_expr + "),0) as s FROM proyectos p " + IPC_JOIN +
        " WHERE p.programa_id=? AND p.monto_diagnostico IS NOT NULL",
        (pid,), one=True)
    total_facturado_real = total_real_row['s'] if total_real_row else 0

    convenios_enriched  = []
    total_convenios_real= 0
    for c in convenios:
        c = dict(c)
        pond_row = query(
            "SELECT ponderador FROM ponderadores_ipc WHERE fecha_desembolso=? AND fecha_valuacion=?",
            (f"{c['anio']}-06", ipc_fecha_val), one=True)
        c['ponderador'] = pond_row['ponderador'] if pond_row else 1.0
        c['monto_real'] = round(c['monto'] * c['ponderador'])
        total_convenios_real += c['monto_real']
        convenios_enriched.append(c)

    por_esp_real = query(
        "SELECT p.especialista, COUNT(*) as n,"
        " SUM(CASE WHEN p.estado='finalizado' THEN 1 ELSE 0 END) as finalizados,"
        " SUM(CASE WHEN p.estado='suspendido' THEN 1 ELSE 0 END) as bajas,"
        " COALESCE(AVG(CASE WHEN p.monto_diagnostico IS NOT NULL THEN p.monto_diagnostico END),0) as monto_avg,"
        " COALESCE(AVG(CASE WHEN p.monto_diagnostico IS NOT NULL THEN " + real_expr + " END),0) as monto_avg_real,"
        " COALESCE(SUM(p.monto_diagnostico),0) as total_nom,"
        " COALESCE(SUM(" + real_expr + "),0) as total_real"
        " FROM proyectos p " + IPC_JOIN +
        " WHERE p.programa_id=? AND p.especialista IS NOT NULL AND p.especialista NOT IN ('-')"
        " GROUP BY p.especialista ORDER BY n DESC LIMIT 20",
        (pid,))

    esp_real_map = {r['especialista']: dict(r) for r in por_esp_real}
    por_especialista_merged = []
    for e in por_especialista:
        d = dict(e)
        real = esp_real_map.get(d['especialista'], {})
        d['monto_avg_real'] = real.get('monto_avg_real', d['monto_avg'])
        d['total_nom']      = real.get('total_nom', 0)
        d['total_real']     = real.get('total_real', 0)
        por_especialista_merged.append(d)

    return render_template('programs/clinica_dashboard.html',
        programa=programa, proyectos=proyectos, hitos=hitos,
        total=total, activos=activos, finalizados=finalizados,
        en_fin=en_fin, suspendidos=suspendidos,
        total_facturado=total_facturado,
        total_facturado_real=total_facturado_real,
        monto_promedio=monto_promedio,
        ipc_fecha_val=ipc_fecha_val,
        pipeline=pipeline,
        por_anio=[dict(r) for r in por_anio],
        por_municipio=[dict(r) for r in por_municipio],
        rubros_raw=[dict(r) for r in rubros_raw],
        por_especialista=por_especialista_merged,
        por_esp_real=[dict(r) for r in por_esp_real],
        convenios=convenios_enriched,
        total_convenios=total_convenios,
        total_convenios_real=total_convenios_real,
        estado_emp_dist=[dict(r) for r in estado_emp_dist],
        por_periodo=[dict(r) for r in por_periodo],
        mapa_municipios_clinica=[dict(r) for r in mapa_municipios_clinica],
    )


# ─── Dashboard FONICS ─────────────────────────────────────────────────────────
def _fonics_detail(pid, programa, proyectos, hitos):
    total        = len(proyectos)
    finalizados  = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='finalizado'", (pid,), one=True)['n']
    activos      = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='activo'", (pid,), one=True)['n']
    suspendidos  = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='suspendido'", (pid,), one=True)['n']
    total_anr    = query("SELECT COALESCE(SUM(anr_monto),0) as s FROM proyectos WHERE programa_id=?", (pid,), one=True)['s']

    ipc_fecha_val, _ = get_ipc_config()
    IPC_JOIN, has_ipc = build_ipc_join(None, ipc_fecha_val, programa_id=pid)
    real_expr = ipc_anr_expr(has_ipc)

    total_anr_real = query(
        f"SELECT COALESCE(SUM({real_expr}),0) as s FROM proyectos p {IPC_JOIN} WHERE p.programa_id=?",
        (pid,), one=True
    )['s']

    estados_dist = query("SELECT estado, COUNT(*) as n FROM proyectos WHERE programa_id=? GROUP BY estado ORDER BY n DESC", (pid,))

    por_municipio = query("""
        SELECT municipio, COUNT(*) as n FROM proyectos
        WHERE programa_id=? AND municipio IS NOT NULL AND municipio != ''
        GROUP BY municipio ORDER BY n DESC LIMIT 15
    """, (pid,))

    por_anio = query(f"""
        SELECT p.anio, COUNT(*) as n,
               COALESCE(SUM(p.anr_monto),0) as total_anr,
               COALESCE(SUM({real_expr}),0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.anio IS NOT NULL
        GROUP BY p.anio ORDER BY p.anio
    """, (pid,))

    por_ib = query(f"""
        SELECT p.beneficiario, COUNT(*) as n,
               COALESCE(SUM(p.anr_monto),0) as total_anr,
               COALESCE(SUM({real_expr}),0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.beneficiario IS NOT NULL
        GROUP BY p.beneficiario ORDER BY n DESC LIMIT 15
    """, (pid,))

    avances_por_hito = {}
    for h in hitos:
        completados = query("""
            SELECT COUNT(*) as n FROM avances_hitos ah
            JOIN proyectos p ON ah.proyecto_id = p.id
            WHERE ah.hito_id=? AND p.programa_id=? AND ah.estado='completado'
        """, (h['id'], pid), one=True)['n']
        avances_por_hito[h['id']] = completados

    return render_template('programs/fonics_dashboard.html',
        programa=programa, proyectos=proyectos, hitos=hitos,
        total=total, activos=activos, finalizados=finalizados, suspendidos=suspendidos,
        total_anr=total_anr, total_anr_real=total_anr_real, ipc_fecha_val=ipc_fecha_val,
        estados_dist=[dict(r) for r in estados_dist],
        por_municipio=[dict(r) for r in por_municipio],
        por_anio=[dict(r) for r in por_anio],
        por_ib=[dict(r) for r in por_ib],
        avances_por_hito=avances_por_hito,
    )


# ─── Dashboard CLIC ───────────────────────────────────────────────────────────
def _clic_detail(pid, programa, proyectos, hitos):
    total        = len(proyectos)
    activos      = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='activo'", (pid,), one=True)['n']
    finalizados  = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='finalizado'", (pid,), one=True)['n']
    suspendidos  = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='suspendido'", (pid,), one=True)['n']

    municipios_cubiertos = query("""
        SELECT COUNT(DISTINCT municipio) as n FROM proyectos
        WHERE programa_id=? AND municipio IS NOT NULL AND municipio != ''
    """, (pid,), one=True)['n']

    total_anr = query("SELECT COALESCE(SUM(anr_monto),0) as s FROM proyectos WHERE programa_id=?", (pid,), one=True)['s']

    ipc_fecha_val, _ = get_ipc_config()
    IPC_JOIN, has_ipc = build_ipc_join(None, ipc_fecha_val, programa_id=pid)

    total_anr_real = query(
        f"SELECT COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)),0) as s "
        f"FROM proyectos p {IPC_JOIN} WHERE p.programa_id=?", (pid,), one=True
    )['s'] if has_ipc else total_anr

    estados_dist = query("SELECT estado, COUNT(*) as n FROM proyectos WHERE programa_id=? GROUP BY estado ORDER BY n DESC", (pid,))

    por_municipio = query(f"""
        SELECT p.municipio,
               COUNT(*) as n,
               SUM(CASE WHEN p.estado='activo'     THEN 1 ELSE 0 END) as activos,
               SUM(CASE WHEN p.estado='finalizado' THEN 1 ELSE 0 END) as finalizados,
               COALESCE(SUM(p.anr_monto), 0) as total_anr,
               COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)), 0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.municipio IS NOT NULL AND p.municipio != ''
        GROUP BY p.municipio ORDER BY total_real DESC LIMIT 20
    """, (pid,))

    por_anio = query(f"""
        SELECT p.anio,
               COUNT(*) as n,
               COALESCE(SUM(p.anr_monto), 0) as total_anr,
               COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)), 0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.anio IS NOT NULL
        GROUP BY p.anio ORDER BY p.anio
    """, (pid,))

    por_area = query(f"""
        SELECT p.area_tematica,
               COUNT(*) as n,
               COALESCE(SUM(p.anr_monto), 0) as total_anr,
               COALESCE(SUM(p.anr_monto * COALESCE(ipc.ponderador,1)), 0) as total_real
        FROM proyectos p {IPC_JOIN}
        WHERE p.programa_id=? AND p.area_tematica IS NOT NULL AND p.area_tematica != ''
        GROUP BY p.area_tematica ORDER BY total_real DESC LIMIT 15
    """, (pid,))

    # ANR real por proyecto
    real_expr = ipc_anr_expr(has_ipc, alias='p')
    real_per_proy = {r['id']: r['s'] for r in query(
        f"SELECT p.id, COALESCE({real_expr}, 0) as s FROM proyectos p {IPC_JOIN} WHERE p.programa_id=?",
        (pid,)
    )} if has_ipc else {}
    proyectos_enriched = []
    for p in proyectos:
        d = dict(p)
        d['anr_real'] = real_per_proy.get(d['id'], d.get('anr_monto') or 0)
        proyectos_enriched.append(d)

    # Avance por hito
    avances_por_hito = {}
    for h in hitos:
        completados = query("""
            SELECT COUNT(*) as n FROM avances_hitos ah
            JOIN proyectos p ON ah.proyecto_id = p.id
            WHERE ah.hito_id=? AND p.programa_id=? AND ah.estado='completado'
        """, (h['id'], pid), one=True)['n']
        avances_por_hito[h['id']] = completados

    # Últimas novedades de centros
    novedades_recientes = query("""
        SELECT n.titulo, n.tipo, n.fecha, p.nombre as centro, p.municipio
        FROM novedades n
        JOIN proyectos p ON n.proyecto_id = p.id
        WHERE p.programa_id=?
        ORDER BY n.fecha DESC, n.created_at DESC LIMIT 10
    """, (pid,))

    return render_template('programs/clic_dashboard.html',
        programa=programa, proyectos=proyectos_enriched, hitos=hitos,
        total=total, activos=activos, finalizados=finalizados, suspendidos=suspendidos,
        municipios_cubiertos=municipios_cubiertos,
        total_anr=total_anr, total_anr_real=total_anr_real,
        ipc_fecha_val=ipc_fecha_val,
        estados_dist=[dict(r) for r in estados_dist],
        por_municipio=[dict(r) for r in por_municipio],
        por_anio=[dict(r) for r in por_anio],
        por_area=[dict(r) for r in por_area],
        avances_por_hito=avances_por_hito,
        novedades_recientes=[dict(r) for r in novedades_recientes],
    )
