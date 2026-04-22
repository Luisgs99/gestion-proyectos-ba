from datetime import date
from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from database import query, execute
from helpers.auth import login_required, editor_required, admin_required
from helpers.filtros import get_filtros_config, get_filter_options, apply_filtros

bp = Blueprint('proyectos', __name__)


@bp.route('/proyectos')
@login_required
def list():
    programa_id = request.args.get('programa_id', '')
    search      = request.args.get('q', '')

    sql = """
        SELECT p.*, pr.nombre as programa_nombre, pr.codigo as programa_codigo,
               pr.color as programa_color, u.nombre as agente_nombre, u.apellido as agente_apellido
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        LEFT JOIN users u ON p.agente_id = u.id
        WHERE 1=1
    """
    args = []

    if session.get('rol') == 'agente':
        sql += " AND (p.agente_id=? OR EXISTS (SELECT 1 FROM asignaciones a WHERE a.proyecto_id=p.id AND a.agente_id=?))"
        args.extend([session['user_id'], session['user_id']])

    if programa_id:
        sql += " AND p.programa_id=?"
        args.append(programa_id)

    if search:
        sql += " AND (p.nombre LIKE ? OR p.beneficiario LIKE ? OR p.adoptante LIKE ? OR p.codigo LIKE ?)"
        args.extend([f'%{search}%'] * 4)

    filtros_activos        = []
    filtros_con_opciones   = {}
    active_filter_count    = 0

    if programa_id:
        filtros_activos = get_filtros_config(int(programa_id), solo_activos=True)
        sql, args, active_filter_count = apply_filtros(sql, args, filtros_activos, request.args)
        for f in filtros_activos:
            if f['filter_type'] == 'select':
                filtros_con_opciones[f['field_key']] = get_filter_options(f['field_key'], int(programa_id))

    sql += " ORDER BY pr.id, p.nombre"
    proyectos = query(sql, args)
    programas = query("SELECT * FROM programas WHERE activo=1 ORDER BY id")

    return render_template('projects/list.html',
        proyectos=proyectos, programas=programas,
        filtro_programa=programa_id, search=search,
        filtros_activos=filtros_activos,
        filtros_con_opciones=filtros_con_opciones,
        active_filter_count=active_filter_count,
        current_filters=request.args)


@bp.route('/proyectos/nuevo', methods=['GET', 'POST'])
@editor_required
def nuevo():
    programas = query("SELECT * FROM programas WHERE activo=1 ORDER BY nombre")
    agentes   = query("SELECT * FROM users WHERE rol='agente' AND activo=1 ORDER BY apellido")
    if request.method == 'POST':
        d   = request.form
        pid = execute("""
            INSERT INTO proyectos (programa_id, nombre, codigo, descripcion, beneficiario,
                tipo_beneficiario, adoptante, tipo_adoptante, anr_monto, estado,
                fecha_inicio, fecha_fin_prevista, agente_id, municipio, localidad,
                area_tematica, contacto_nombre, contacto_email, contacto_telefono)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (d.get('programa_id'), d.get('nombre'), d.get('codigo'), d.get('descripcion'),
              d.get('beneficiario'), d.get('tipo_beneficiario', 'universidad'),
              d.get('adoptante'), d.get('tipo_adoptante', 'empresa'),
              float(d.get('anr_monto') or 0), d.get('estado', 'activo'),
              d.get('fecha_inicio') or None, d.get('fecha_fin_prevista') or None,
              d.get('agente_id') or None, d.get('municipio'), d.get('localidad'),
              d.get('area_tematica'), d.get('contacto_nombre'),
              d.get('contacto_email'), d.get('contacto_telefono')))

        try:
            anio_nuevo = int(d.get('anio')) if d.get('anio') else None
        except (ValueError, TypeError):
            anio_nuevo = None
        hitos = query("""SELECT id FROM hitos WHERE programa_id=?
            AND (anio_desde IS NULL OR anio_desde <= ?)
            AND (anio_hasta IS NULL OR anio_hasta >= ?)""",
            (d.get('programa_id'), anio_nuevo or 9999, anio_nuevo or 0))
        for h in hitos:
            execute("INSERT OR IGNORE INTO avances_hitos (proyecto_id, hito_id, estado, registrado_por) VALUES (?,?,'pendiente',?)",
                    (pid, h['id'], session['user_id']))
        flash('Proyecto creado exitosamente.', 'success')
        return redirect(url_for('proyectos.detail', pid=pid))
    return render_template('projects/form.html', programas=programas, agentes=agentes, proyecto=None)


@bp.route('/proyectos/<int:pid>')
@login_required
def detail(pid):
    proyecto = query("""
        SELECT p.*, pr.nombre as programa_nombre, pr.codigo as programa_codigo,
               pr.color as programa_color, pr.id as programa_id_val,
               u.nombre as agente_nombre, u.apellido as agente_apellido
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        LEFT JOIN users u ON p.agente_id = u.id
        WHERE p.id=?
    """, (pid,), one=True)
    if not proyecto:
        flash('Proyecto no encontrado.', 'danger')
        return redirect(url_for('proyectos.list'))

    anio_proy = proyecto['anio'] if proyecto['anio'] else None
    hitos_avances = query("""
        SELECT h.*, ah.estado as avance_estado, ah.porcentaje, ah.fecha_prevista,
               ah.fecha_real, ah.observaciones, ah.id as avance_id
        FROM hitos h
        LEFT JOIN avances_hitos ah ON ah.hito_id = h.id AND ah.proyecto_id=?
        WHERE h.programa_id=?
          AND (h.anio_desde IS NULL OR h.anio_desde <= ?)
          AND (h.anio_hasta IS NULL OR h.anio_hasta >= ?)
        ORDER BY h.orden
    """, (pid, proyecto['programa_id_val'], anio_proy or 9999, anio_proy or 0))

    novedades = query("""
        SELECT n.*, u.nombre as autor_nombre, u.apellido as autor_apellido
        FROM novedades n
        LEFT JOIN users u ON n.registrado_por = u.id
        WHERE n.proyecto_id=?
        ORDER BY n.fecha DESC, n.created_at DESC
    """, (pid,))

    if session.get('rol') == 'agente':
        is_assigned = query("""
            SELECT 1 FROM proyectos WHERE id=? AND agente_id=?
            UNION SELECT 1 FROM asignaciones WHERE proyecto_id=? AND agente_id=?
        """, (pid, session['user_id'], pid, session['user_id']))
        if not is_assigned:
            flash('Solo podés ver tus proyectos asignados.', 'warning')

    completados   = sum(1 for h in hitos_avances if h['avance_estado'] == 'completado')
    total_hitos   = len(hitos_avances)
    porcentaje    = int((completados / total_hitos * 100)) if total_hitos > 0 else 0
    tiene_etapas  = any(h['etapa'] for h in hitos_avances)

    return render_template('projects/detail.html',
        proyecto=proyecto, hitos_avances=hitos_avances, novedades=novedades,
        completados=completados, total_hitos=total_hitos, porcentaje=porcentaje,
        tiene_etapas=tiene_etapas)


@bp.route('/proyectos/<int:pid>/editar', methods=['GET', 'POST'])
@editor_required
def editar(pid):
    proyecto  = query("SELECT * FROM proyectos WHERE id=?", (pid,), one=True)
    if not proyecto:
        flash('Proyecto no encontrado.', 'danger')
        return redirect(url_for('proyectos.list'))
    programas = query("SELECT * FROM programas WHERE activo=1 ORDER BY nombre")
    agentes   = query("SELECT * FROM users WHERE rol='agente' AND activo=1 ORDER BY apellido")
    if request.method == 'POST':
        d = request.form
        execute("""
            UPDATE proyectos SET programa_id=?, nombre=?, codigo=?, descripcion=?,
                beneficiario=?, tipo_beneficiario=?, adoptante=?, tipo_adoptante=?,
                anr_monto=?, estado=?, fecha_inicio=?, fecha_fin_prevista=?,
                fecha_fin_real=?, agente_id=?, municipio=?, localidad=?,
                area_tematica=?, contacto_nombre=?, contacto_email=?, contacto_telefono=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (d.get('programa_id'), d.get('nombre'), d.get('codigo'), d.get('descripcion'),
              d.get('beneficiario'), d.get('tipo_beneficiario', 'universidad'),
              d.get('adoptante'), d.get('tipo_adoptante', 'empresa'),
              float(d.get('anr_monto') or 0), d.get('estado', 'activo'),
              d.get('fecha_inicio') or None, d.get('fecha_fin_prevista') or None,
              d.get('fecha_fin_real') or None,
              d.get('agente_id') or None, d.get('municipio'), d.get('localidad'),
              d.get('area_tematica'), d.get('contacto_nombre'),
              d.get('contacto_email'), d.get('contacto_telefono'), pid))
        flash('Proyecto actualizado.', 'success')
        return redirect(url_for('proyectos.detail', pid=pid))
    return render_template('projects/form.html', programas=programas, agentes=agentes, proyecto=proyecto)


@bp.route('/proyectos/<int:pid>/hito/<int:hid>', methods=['POST'])
@editor_required
def actualizar_hito(pid, hid):
    d          = request.form
    porcentaje = int(d.get('porcentaje', 0))
    execute("""
        INSERT INTO avances_hitos (proyecto_id, hito_id, estado, fecha_prevista, fecha_real,
                                   porcentaje, observaciones, registrado_por)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(proyecto_id, hito_id) DO UPDATE SET
            estado=excluded.estado, fecha_prevista=excluded.fecha_prevista,
            fecha_real=excluded.fecha_real, porcentaje=excluded.porcentaje,
            observaciones=excluded.observaciones, registrado_por=excluded.registrado_por,
            updated_at=CURRENT_TIMESTAMP
    """, (pid, hid, d.get('estado', 'pendiente'),
          d.get('fecha_prevista') or None, d.get('fecha_real') or None,
          porcentaje, d.get('observaciones'), session['user_id']))

    proy_info = query("SELECT anio, programa_id FROM proyectos WHERE id=?", (pid,), one=True)
    anio_pi   = proy_info['anio'] if proy_info and proy_info['anio'] else None
    prog_pi   = proy_info['programa_id'] if proy_info else None
    total_h   = query("""SELECT COUNT(*) as n FROM hitos
                         WHERE programa_id=?
                           AND (anio_desde IS NULL OR anio_desde <= ?)
                           AND (anio_hasta IS NULL OR anio_hasta >= ?)""",
                      (prog_pi, anio_pi or 9999, anio_pi or 0), one=True)['n']
    completados = query("""SELECT COUNT(*) as n FROM avances_hitos ah
                           JOIN hitos h ON ah.hito_id=h.id
                           WHERE ah.proyecto_id=? AND ah.estado='completado'
                             AND h.programa_id=?
                             AND (h.anio_desde IS NULL OR h.anio_desde <= ?)
                             AND (h.anio_hasta IS NULL OR h.anio_hasta >= ?)""",
                        (pid, prog_pi, anio_pi or 9999, anio_pi or 0), one=True)['n']
    pct = int(completados / total_h * 100) if total_h > 0 else 0
    execute("UPDATE proyectos SET porcentaje_avance=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (pct, pid))

    flash('Hito actualizado.', 'success')
    return redirect(url_for('proyectos.detail', pid=pid))


@bp.route('/proyectos/<int:pid>/novedad', methods=['POST'])
@editor_required
def agregar_novedad(pid):
    d = request.form
    execute("""
        INSERT INTO novedades (proyecto_id, titulo, descripcion, tipo, fecha, registrado_por)
        VALUES (?,?,?,?,?,?)
    """, (pid, d.get('titulo'), d.get('descripcion'), d.get('tipo', 'novedad'),
          d.get('fecha') or date.today().isoformat(), session['user_id']))
    flash('Novedad registrada.', 'success')
    return redirect(url_for('proyectos.detail', pid=pid))


@bp.route('/proyectos/<int:pid>/eliminar', methods=['POST'])
@admin_required
def eliminar(pid):
    execute("DELETE FROM avances_hitos WHERE proyecto_id=?", (pid,))
    execute("DELETE FROM novedades WHERE proyecto_id=?", (pid,))
    execute("DELETE FROM asignaciones WHERE proyecto_id=?", (pid,))
    execute("DELETE FROM proyectos WHERE id=?", (pid,))
    flash('Proyecto eliminado.', 'success')
    return redirect(url_for('proyectos.list'))


@bp.route('/api/programa/<int:pid>/stats')
@login_required
def api_programa_stats(pid):
    estados   = query("SELECT estado, COUNT(*) as n FROM proyectos WHERE programa_id=? GROUP BY estado", (pid,))
    hitos_data= query("""
        SELECT h.nombre, h.etapa, h.orden,
               SUM(CASE WHEN ah.estado='completado' THEN 1 ELSE 0 END) as completados,
               SUM(CASE WHEN ah.estado='en_proceso' THEN 1 ELSE 0 END) as en_proceso,
               COUNT(ah.id) as total
        FROM hitos h
        LEFT JOIN avances_hitos ah ON ah.hito_id = h.id
        LEFT JOIN proyectos p ON ah.proyecto_id = p.id AND p.programa_id=?
        WHERE h.programa_id=? AND h.anio_hasta IS NULL
        GROUP BY h.id ORDER BY h.orden
    """, (pid, pid))
    municipios= query("""
        SELECT municipio, COUNT(*) as n FROM proyectos
        WHERE programa_id=? AND municipio IS NOT NULL AND municipio != ''
        GROUP BY municipio ORDER BY n DESC LIMIT 12
    """, (pid,))
    return jsonify({
        'estados':    [dict(e) for e in estados],
        'hitos':      [dict(h) for h in hitos_data],
        'municipios': [dict(m) for m in municipios],
    })
