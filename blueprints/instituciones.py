import io
import os
import uuid
import pandas as pd
from datetime import date
from werkzeug.utils import secure_filename
from flask import (Blueprint, render_template, request, redirect,
                   url_for, session, flash, send_file, current_app, jsonify)
from database import query, execute, get_db
from helpers.auth import login_required, editor_required, can_edit

bp = Blueprint('instituciones', __name__)

TIPOS_DOC = ['convenio', 'estatuto', 'resolucion', 'nota', 'acta', 'otro']
TIPOS_NOVEDAD = ['reunion', 'comunicacion', 'acuerdo', 'conflicto', 'novedad']
TIPOS_INST = ['universidad', 'conicet', 'hospital', 'empresa', 'municipio', 'ong', 'otro']
ESTADOS_VINCULO = ['activo', 'en_gestion', 'pendiente', 'inactivo']


def _docs_folder():
    folder = os.path.join(current_app.config['UPLOAD_FOLDER'], 'instituciones')
    os.makedirs(folder, exist_ok=True)
    return folder


# ─── Listado ──────────────────────────────────────────────────────────────────
@bp.route('/instituciones')
@login_required
def list():
    q = request.args.get('q', '').strip()
    tipo = request.args.get('tipo', '')
    estado = request.args.get('estado', '')

    sql = """
        SELECT i.*,
               COUNT(DISTINCT ic.id) as n_contactos,
               COUNT(DISTINCT id2.id) as n_documentos
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

    instituciones = query(sql, params)
    return render_template('instituciones/list.html',
                           instituciones=instituciones,
                           tipos=TIPOS_INST, estados=ESTADOS_VINCULO,
                           q=q, tipo=tipo, estado=estado)


# ─── Detalle ──────────────────────────────────────────────────────────────────
@bp.route('/instituciones/<int:iid>')
@login_required
def detail(iid):
    inst = query("SELECT * FROM instituciones WHERE id=?", (iid,), one=True)
    if not inst:
        flash('Institución no encontrada.', 'danger')
        return redirect(url_for('instituciones.list'))

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

    # Proyectos vinculados (por nombre_corto o nombre en beneficiario / ib2)
    terminos = [inst['nombre']]
    if inst['nombre_corto']:
        terminos.append(inst['nombre_corto'])
    proyectos_vinculados = []
    for t in terminos:
        rows = query("""
            SELECT p.id, p.nombre, p.codigo, p.estado, p.anr_monto, p.anio,
                   pr.nombre as prog_nombre, pr.codigo as prog_codigo, pr.color as prog_color
            FROM proyectos p
            JOIN programas pr ON p.programa_id = pr.id
            WHERE p.beneficiario LIKE ? OR p.ib2 LIKE ?
            ORDER BY p.nombre
        """, (f'%{t}%', f'%{t}%'))
        for r in rows:
            if not any(x['id'] == r['id'] for x in proyectos_vinculados):
                proyectos_vinculados.append(r)

    tab = request.args.get('tab', 'info')
    return render_template('instituciones/detail.html',
                           inst=inst, contactos=contactos,
                           documentos=documentos, novedades=novedades,
                           proyectos_vinculados=proyectos_vinculados,
                           tipos_doc=TIPOS_DOC, tipos_novedad=TIPOS_NOVEDAD,
                           tab=tab)


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
        return redirect(url_for('instituciones.list'))
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
        return redirect(url_for('instituciones.list'))
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
    return redirect(url_for('instituciones.list'))


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
    return redirect(url_for('instituciones.list'))


# ─── Documentos ───────────────────────────────────────────────────────────────
@bp.route('/instituciones/<int:iid>/documentos/subir', methods=['POST'])
@editor_required
def subir_doc(iid):
    inst = query("SELECT id FROM instituciones WHERE id=?", (iid,), one=True)
    if not inst:
        return redirect(url_for('instituciones.list'))

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
