from database import query


def get_filtros_config(programa_id, solo_activos=False):
    """Devuelve la config de filtros de un programa, ordenada por 'orden'."""
    sql = "SELECT * FROM filtros_config WHERE programa_id=?"
    if solo_activos:
        sql += " AND enabled=1"
    sql += " ORDER BY orden"
    return query(sql, (programa_id,))


def get_filter_options(field_key, programa_id):
    """Devuelve los valores distintos para un campo en un programa (para dropdowns)."""
    if field_key in ('ita_recibido', 'ita_firmado', 'ita_subsanar', 'ita_subsanado'):
        return []  # booleanos, no usan dropdown
    try:
        rows = query(
            f"SELECT DISTINCT {field_key} FROM proyectos "
            f"WHERE programa_id=? AND {field_key} IS NOT NULL ORDER BY {field_key}",
            (programa_id,)
        )
        return [r[field_key] for r in rows if r[field_key] not in ('', '-', None)]
    except Exception:
        return []


def apply_filtros(base_query, params, filtros_activos, request_args):
    """
    Aplica los filtros activos al query base.
    Devuelve (query_str, params, active_count).
    """
    active_count = 0
    for f in filtros_activos:
        fk  = f['field_key']
        val = request_args.get(fk, '').strip()
        if not val:
            continue
        if f['filter_type'] == 'boolean':
            base_query += f" AND {fk} IS NOT NULL"
        elif f['filter_type'] == 'text':
            base_query += f" AND {fk} LIKE ?"
            params.append(f'%{val}%')
        else:
            base_query += f" AND {fk} = ?"
            params.append(val)
        active_count += 1
    return base_query, params, active_count
