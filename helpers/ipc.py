from database import query


def get_ipc_config():
    """Devuelve (ipc_ultima_fecha, ipc_primera_fecha) desde configuracion."""
    meta = {r['clave']: r['valor'] for r in
            query("SELECT clave, valor FROM configuracion WHERE clave LIKE 'ipc_%'")}
    return (meta.get('ipc_ultima_fecha', '2026-02'),
            meta.get('ipc_primera_fecha', '2019-01'))


def get_ipc_rule(programa_id):
    """Devuelve la regla IPC de un programa desde ipc_config, o None si no tiene."""
    return query("SELECT * FROM ipc_config WHERE programa_id=?", (programa_id,), one=True)


def build_ipc_join(programa_codigo_or_id, ipc_fecha_val, programa_id=None, alias='p'):
    """
    Construye LEFT JOIN de ponderadores según la regla en ipc_config.
    Si la regla tiene fecha_expr_sql usa esa expresión (con {a} como placeholder del alias).
    Devuelve (join_sql, has_ipc).
    """
    if programa_id is not None:
        rule = query("SELECT * FROM ipc_config WHERE programa_id=?", (programa_id,), one=True)
    else:
        rule = query("""SELECT ic.* FROM ipc_config ic
                        JOIN programas pg ON ic.programa_id = pg.id
                        WHERE pg.codigo=?""", (programa_codigo_or_id,), one=True)
    if not rule:
        return "", False

    a = alias
    if rule['fecha_expr_sql']:
        fecha_expr = rule['fecha_expr_sql'].replace('{a}', a)
        null_guard = f"{a}.{rule['campo_anio']} IS NOT NULL OR {a}.periodo_facturacion IS NOT NULL"
    else:
        campo  = rule['campo_anio']
        offset = rule['anio_offset']
        mes    = rule['mes_desembolso']
        if offset:
            fecha_expr = f"(CAST({a}.{campo}+{offset} AS TEXT) || '-{mes}')"
        else:
            fecha_expr = f"(CAST({a}.{campo} AS TEXT) || '-{mes}')"
        null_guard = f"{a}.{campo} IS NOT NULL"

    join_sql = f"""
        LEFT JOIN ponderadores_ipc ipc
            ON ipc.fecha_desembolso = ({fecha_expr})
           AND ipc.fecha_valuacion  = '{ipc_fecha_val}'
           AND ({null_guard})
    """
    return join_sql, True


def ipc_anr_expr(has_ipc, alias='p', campo_monto='anr_monto'):
    """Expresión SQL para valor monetario actualizado."""
    if has_ipc:
        return f"{alias}.{campo_monto} * COALESCE(ipc.ponderador, 1)"
    return f"{alias}.{campo_monto}"


def ipc_fecha_desemb_label(programa_id):
    """Etiqueta legible para la regla de fecha de desembolso de un programa."""
    rule = query("SELECT * FROM ipc_config WHERE programa_id=?", (programa_id,), one=True)
    if not rule:
        return None
    meses = {
        '01': 'enero',   '02': 'febrero',   '03': 'marzo',
        '04': 'abril',   '05': 'mayo',       '06': 'junio',
        '07': 'julio',   '08': 'agosto',     '09': 'septiembre',
        '10': 'octubre', '11': 'noviembre',  '12': 'diciembre',
    }
    mes_nombre  = meses.get(rule['mes_desembolso'], rule['mes_desembolso'])
    campo_label = {
        'anio':           'convocatoria',
        'anio_redaccion': 'redaccion',
        'anio_aprobacion':'aprobacion',
    }.get(rule['campo_anio'], rule['campo_anio'])
    offset_txt = f"+{rule['anio_offset']}año" if rule['anio_offset'] else ""
    return f"{mes_nombre} del {campo_label}{offset_txt}"
