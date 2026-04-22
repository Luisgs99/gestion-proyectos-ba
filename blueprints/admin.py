import json
from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from werkzeug.security import generate_password_hash
from database import query, execute, get_db
from helpers.auth import admin_required, login_required, editor_required, can_edit
from helpers.filtros import get_filtros_config

bp = Blueprint('admin', __name__)


# ─── Organigrama ─────────────────────────────────────────────────────────────
@bp.route('/organigrama')
@login_required
def organigrama():
    unidades = query("""
        SELECT u.*,
               json_group_array(json_object(
                   'id', p.id, 'nombre', p.nombre, 'cargo', p.cargo,
                   'subtipo', p.subtipo, 'orden', p.orden
               ) ORDER BY p.orden) as personas_json
        FROM org_unidades u
        LEFT JOIN org_personas p ON p.unidad_id = u.id AND p.activo = 1
        GROUP BY u.id ORDER BY u.orden
    """)
    result = []
    for u in unidades:
        d = dict(u)
        d['personas'] = json.loads(d['personas_json']) if d['personas_json'] else []
        d['personas'] = [p for p in d['personas'] if p['nombre']]
        result.append(d)
    return render_template('organigrama.html', unidades=result)


@bp.route('/organigrama/add_persona', methods=['POST'])
@login_required
def org_add_persona():
    if not can_edit():
        return {'ok': False, 'error': 'Sin permisos'}, 403
    unidad_id = request.form.get('unidad_id')
    nombre    = request.form.get('nombre', '').strip()
    cargo     = request.form.get('cargo', '').strip()
    subtipo   = request.form.get('subtipo') or None
    if not unidad_id or not nombre or not cargo:
        return {'ok': False, 'error': 'Faltan datos'}, 400
    max_orden = query("SELECT COALESCE(MAX(orden),0)+1 as n FROM org_personas WHERE unidad_id=?",
                      (unidad_id,), one=True)['n']
    pid = execute("INSERT INTO org_personas (unidad_id,nombre,cargo,subtipo,orden) VALUES (?,?,?,?,?)",
                  (unidad_id, nombre, cargo, subtipo, max_orden))
    return {'ok': True, 'id': pid, 'nombre': nombre, 'cargo': cargo, 'subtipo': subtipo}


@bp.route('/organigrama/remove_persona/<int:pid>', methods=['POST'])
@login_required
def org_remove_persona(pid):
    if not can_edit():
        return {'ok': False, 'error': 'Sin permisos'}, 403
    execute("UPDATE org_personas SET activo=0 WHERE id=?", (pid,))
    return {'ok': True}


# ─── Filtros dinámicos ────────────────────────────────────────────────────────
@bp.route('/admin/filtros')
@login_required
def filtros():
    programas        = query("SELECT * FROM programas ORDER BY nombre")
    filtros_por_prog = {p['id']: get_filtros_config(p['id']) for p in programas}
    return render_template('admin/filtros.html',
        programas=programas, filtros_por_prog=filtros_por_prog)


@bp.route('/admin/filtros/<int:prog_id>', methods=['POST'])
@login_required
def filtros_guardar(prog_id):
    conn = get_db()
    conn.execute("UPDATE filtros_config SET enabled=0 WHERE programa_id=?", (prog_id,))
    orden_data = request.form.get('orden_json', '[]')
    try:
        orden_list = json.loads(orden_data)
    except Exception:
        orden_list = []
    for i, fk in enumerate(orden_list):
        conn.execute(
            "UPDATE filtros_config SET enabled=1, orden=? WHERE programa_id=? AND field_key=?",
            (i + 1, prog_id, fk)
        )
    conn.commit()
    flash('Configuración de filtros guardada.', 'success')
    return redirect(url_for('admin.filtros'))


# ─── Usuarios ─────────────────────────────────────────────────────────────────
@bp.route('/admin/usuarios')
@admin_required
def usuarios():
    users = query("SELECT * FROM users ORDER BY rol, apellido")
    return render_template('admin/usuarios.html', users=users)


@bp.route('/admin/usuarios/nuevo', methods=['GET', 'POST'])
@admin_required
def usuario_nuevo():
    if request.method == 'POST':
        d       = request.form
        hash_pw = generate_password_hash(d.get('password'))
        try:
            execute("""INSERT INTO users (nombre, apellido, email, password_hash, rol)
                       VALUES (?,?,?,?,?)""",
                    (d.get('nombre'), d.get('apellido'), d.get('email'), hash_pw, d.get('rol', 'agente')))
            flash('Usuario creado.', 'success')
        except Exception as e:
            flash(f'Error: {str(e)}', 'danger')
        return redirect(url_for('admin.usuarios'))
    return render_template('admin/usuario_form.html', usuario=None)


@bp.route('/admin/usuarios/<int:uid>/editar', methods=['GET', 'POST'])
@admin_required
def usuario_editar(uid):
    usuario = query("SELECT * FROM users WHERE id=?", (uid,), one=True)
    if request.method == 'POST':
        d = request.form
        if d.get('password'):
            hash_pw = generate_password_hash(d.get('password'))
            execute("UPDATE users SET nombre=?, apellido=?, email=?, password_hash=?, rol=?, activo=? WHERE id=?",
                    (d.get('nombre'), d.get('apellido'), d.get('email'), hash_pw, d.get('rol'), int(d.get('activo', 1)), uid))
        else:
            execute("UPDATE users SET nombre=?, apellido=?, email=?, rol=?, activo=? WHERE id=?",
                    (d.get('nombre'), d.get('apellido'), d.get('email'), d.get('rol'), int(d.get('activo', 1)), uid))
        flash('Usuario actualizado.', 'success')
        return redirect(url_for('admin.usuarios'))
    return render_template('admin/usuario_form.html', usuario=usuario)


@bp.route('/admin/usuarios/<int:uid>/eliminar', methods=['POST'])
@admin_required
def usuario_eliminar(uid):
    if uid == session.get('user_id'):
        flash('No podés eliminar tu propio usuario.', 'danger')
        return redirect(url_for('admin.usuarios'))
    execute("DELETE FROM users WHERE id=?", (uid,))
    flash('Usuario eliminado.', 'success')
    return redirect(url_for('admin.usuarios'))
