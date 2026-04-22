from flask import Blueprint, render_template, jsonify
from database import query
from helpers.auth import login_required
from helpers.ipc import build_ipc_join, get_ipc_config, ipc_anr_expr

bp = Blueprint('dashboard', __name__)


@bp.route('/dashboard')
@login_required
def dashboard():
    programas = query("SELECT * FROM programas WHERE activo=1 ORDER BY id")
    stats = {}
    for p in programas:
        total      = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=?", (p['id'],), one=True)['n']
        activos    = query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='activo'", (p['id'],), one=True)['n']
        finalizados= query("SELECT COUNT(*) as n FROM proyectos WHERE programa_id=? AND estado='finalizado'", (p['id'],), one=True)['n']
        ipc_rule   = query("SELECT campo_monto FROM ipc_config WHERE programa_id=?", (p['id'],), one=True)
        campo_monto= ipc_rule['campo_monto'] if ipc_rule and ipc_rule['campo_monto'] else 'anr_monto'
        anr        = query(f"SELECT COALESCE(SUM({campo_monto}),0) as s FROM proyectos WHERE programa_id=?", (p['id'],), one=True)['s']
        stats[p['id']] = {'total': total, 'activos': activos, 'finalizados': finalizados, 'anr': anr, 'campo_monto': campo_monto}

    total_proyectos  = query("SELECT COUNT(*) as n FROM proyectos", one=True)['n']
    total_anr        = query("SELECT COALESCE(SUM(anr_monto),0) as s FROM proyectos", one=True)['s']
    proyectos_activos= query("SELECT COUNT(*) as n FROM proyectos WHERE estado='activo'", one=True)['n']

    ultimas_novedades = query("""
        SELECT n.*, p.nombre as proyecto_nombre, pr.codigo as programa_codigo, pr.color as programa_color,
               u.nombre as agente_nombre, u.apellido as agente_apellido
        FROM novedades n
        JOIN proyectos p ON n.proyecto_id = p.id
        JOIN programas pr ON p.programa_id = pr.id
        LEFT JOIN users u ON n.registrado_por = u.id
        ORDER BY n.created_at DESC LIMIT 8
    """)

    estados_data = query("SELECT estado, COUNT(*) as n FROM proyectos GROUP BY estado")

    por_programa = query("""
        SELECT pr.id, pr.nombre, pr.codigo, pr.color,
               COUNT(p.id) as n, COALESCE(SUM(p.anr_monto),0) as anr
        FROM programas pr LEFT JOIN proyectos p ON p.programa_id = pr.id
        WHERE pr.activo=1 GROUP BY pr.id
    """)

    ipc_meta      = query("SELECT clave, valor FROM configuracion WHERE clave LIKE 'ipc_%'")
    ipc_fecha_val = {r['clave']: r['valor'] for r in ipc_meta}.get('ipc_ultima_fecha', '2026-02')

    ipc_stats = {}
    for p in programas:
        ipc_join, has_ipc = build_ipc_join(None, ipc_fecha_val, programa_id=p['id'], alias='proy')
        campo_monto = stats[p['id']]['campo_monto']
        real_expr = ipc_anr_expr(has_ipc, alias='proy', campo_monto=campo_monto)
        if has_ipc:
            row = query(
                f"SELECT COALESCE(SUM({real_expr}),0) as s "
                f"FROM proyectos proy {ipc_join} "
                f"WHERE proy.programa_id=?",
                (p['id'],), one=True
            )
            ipc_stats[p['id']] = row['s'] if row else stats[p['id']]['anr']
        else:
            ipc_stats[p['id']] = stats[p['id']]['anr']

    total_anr_real = sum(ipc_stats.values())

    por_programa_ipc = []
    for r in por_programa:
        d = dict(r)
        prog_obj = next((p for p in programas if p['nombre'] == d.get('nombre')), None)
        d['anr_real'] = ipc_stats.get(prog_obj['id'], d.get('anr', 0)) if prog_obj else d.get('anr', 0)
        por_programa_ipc.append(d)

    return render_template('dashboard/index.html',
        programas=programas, stats=stats,
        total_proyectos=total_proyectos, total_anr=total_anr,
        proyectos_activos=proyectos_activos,
        total_anr_real=total_anr_real, ipc_fecha_val=ipc_fecha_val,
        ultimas_novedades=ultimas_novedades,
        estados_data=[dict(r) for r in estados_data],
        por_programa=[dict(r) for r in por_programa],
        por_programa_ipc=por_programa_ipc,
        ipc_stats=ipc_stats)


@bp.route('/api/dashboard/stats')
@login_required
def api_stats():
    por_programa = query("""
        SELECT pr.nombre, pr.color, pr.codigo,
               COUNT(p.id) as total,
               SUM(CASE WHEN p.estado='activo' THEN 1 ELSE 0 END) as activos,
               SUM(CASE WHEN p.estado='finalizado' THEN 1 ELSE 0 END) as finalizados,
               COALESCE(SUM(p.anr_monto), 0) as total_anr
        FROM programas pr
        LEFT JOIN proyectos p ON p.programa_id = pr.id
        WHERE pr.activo=1 GROUP BY pr.id
    """)
    return jsonify({'por_programa': [dict(p) for p in por_programa]})
