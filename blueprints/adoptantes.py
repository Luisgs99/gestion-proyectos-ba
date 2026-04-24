from flask import Blueprint, render_template, request
from database import query
from helpers.auth import login_required
from helpers.ipc import get_ipc_config

bp = Blueprint('adoptantes', __name__)

_BASE_WHERE = "p.adoptante IS NOT NULL AND p.adoptante != '' AND p.adoptante != '-'"
_MUNIC_COND = "(LOWER(p.adoptante) LIKE 'municipalidad%' OR LOWER(p.adoptante) LIKE 'municipio %' OR LOWER(p.adoptante) LIKE 'municipio de %')"

# CLIC (tipo CLIC): el municipio (p.municipio) actúa como adoptante municipal
_CLIC_ES_MUNIC = "(pr.codigo='CLIC' AND UPPER(p.linea)='CLIC' AND p.municipio IS NOT NULL AND p.municipio!='')"

# Para empresas: CLINICA usa p.nombre; CLIC tipo CLIC usa p.municipio
_EFF_ADOP   = f"CASE WHEN pr.codigo='CLINICA' THEN p.nombre WHEN {_CLIC_ES_MUNIC} THEN p.municipio ELSE p.adoptante END"
_EFF_SECTOR = "CASE WHEN pr.codigo='CLINICA' THEN p.rubro     ELSE p.sector_actividad_1 END"
_EFF_ANR    = "CASE WHEN pr.codigo='CLINICA' THEN COALESCE(p.monto_diagnostico,0) ELSE COALESCE(p.anr_monto,0) END"

_VALID_ADOP = (
    "(pr.codigo='CLINICA' AND p.nombre IS NOT NULL AND p.nombre!='' AND p.nombre!='-')"
    f" OR ({_CLIC_ES_MUNIC})"
    " OR (pr.codigo!='CLINICA' AND p.adoptante IS NOT NULL AND p.adoptante!='' AND p.adoptante!='-')"
)

_EFF_MUNIC_COND = (
    f"(LOWER({_EFF_ADOP}) LIKE 'municipalidad%'"
    f" OR LOWER({_EFF_ADOP}) LIKE 'municipio %'"
    f" OR LOWER({_EFF_ADOP}) LIKE 'municipio de %'"
    f" OR {_CLIC_ES_MUNIC})"
)

# Normaliza nombre de municipio quitando prefijos para unificar filas entre programas.
# "Municipalidad de Luján" / "Municipio de Luján" / "Luján" → "Luján"
# Longitudes de prefijos (con espacio final): 17, 14, 13, 10
_MUNIC_NORM = (
    f"CASE"
    f" WHEN LOWER({_EFF_ADOP}) LIKE 'municipalidad de %' THEN TRIM(SUBSTR({_EFF_ADOP}, 18))"
    f" WHEN LOWER({_EFF_ADOP}) LIKE 'municipalidad %'    THEN TRIM(SUBSTR({_EFF_ADOP}, 15))"
    f" WHEN LOWER({_EFF_ADOP}) LIKE 'municipio de %'     THEN TRIM(SUBSTR({_EFF_ADOP}, 14))"
    f" WHEN LOWER({_EFF_ADOP}) LIKE 'municipio %'        THEN TRIM(SUBSTR({_EFF_ADOP}, 11))"
    f" ELSE {_EFF_ADOP}"
    f" END"
)


def _clinica_ipc(ipc_fecha_val):
    """
    Devuelve (join_sql, anr_act_expr) para ajustar monto_diagnostico por IPC.
    Usa alias ipc_c para convivir con otros joins en la misma query.
    Cae a sin-ajuste si CLINICA no tiene regla en ipc_config.
    """
    rule = query("""SELECT ic.* FROM ipc_config ic
                    JOIN programas pg ON ic.programa_id = pg.id
                    WHERE pg.codigo='CLINICA'""", one=True)
    if not rule:
        return "", "COALESCE(p.monto_diagnostico, 0)"

    campo = rule['campo_anio']
    offset = rule['anio_offset']
    mes = rule['mes_desembolso']
    if offset:
        fecha_expr = f"(CAST(p.{campo}+{offset} AS TEXT) || '-{mes}')"
    else:
        fecha_expr = f"(CAST(p.{campo} AS TEXT) || '-{mes}')"

    join_sql = f"""
        LEFT JOIN ponderadores_ipc ipc_c
            ON ipc_c.fecha_desembolso = ({fecha_expr})
           AND ipc_c.fecha_valuacion  = '{ipc_fecha_val}'
           AND p.{campo} IS NOT NULL
           AND pr.codigo = 'CLINICA'"""

    anr_act_expr = "COALESCE(p.monto_diagnostico * COALESCE(ipc_c.ponderador, 1), p.monto_diagnostico, 0)"
    return join_sql, anr_act_expr


@bp.route('/adoptantes')
@login_required
def list():
    tab = request.args.get('tab', 'empresas')

    ipc_fecha_val, _ = get_ipc_config()
    clinica_join, clinica_anr_act = _clinica_ipc(ipc_fecha_val)

    eff_anr_act = (
        f"CASE WHEN pr.codigo='CLINICA' THEN {clinica_anr_act}"
        f" ELSE COALESCE(p.anr_actualizado, p.anr_monto, 0) END"
    )

    empresas = query(f"""
        SELECT {_EFF_ADOP} AS adoptante,
               p.municipio,
               {_EFF_SECTOR} AS sector,
               COUNT(*) AS n_proyectos,
               SUM({_EFF_ANR}) AS total_anr,
               SUM({eff_anr_act}) AS total_anr_actual,
               GROUP_CONCAT(DISTINCT pr.codigo) AS programas,
               GROUP_CONCAT(DISTINCT pr.color)  AS colores
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        {clinica_join}
        WHERE ({_VALID_ADOP}) AND NOT {_EFF_MUNIC_COND}
          AND NOT (pr.codigo='FITBA' AND UPPER(p.linea)='B')
        GROUP BY {_EFF_ADOP}
        ORDER BY n_proyectos DESC, total_anr DESC
    """)

    municipios = query(f"""
        WITH base AS (
            SELECT
                {_MUNIC_NORM}                                    AS munic_norm,
                p.municipio,
                pr.codigo, pr.color,
                COALESCE(p.anr_monto, 0)                         AS anr,
                COALESCE(p.anr_actualizado, p.anr_monto, 0)      AS anr_act
            FROM proyectos p
            JOIN programas pr ON p.programa_id = pr.id
            WHERE ({_VALID_ADOP}) AND {_EFF_MUNIC_COND}
        )
        SELECT
            munic_norm                          AS adoptante,
            COALESCE(MIN(municipio), munic_norm) AS municipio,
            COUNT(*)                            AS n_proyectos,
            SUM(anr)                            AS total_anr,
            SUM(anr_act)                        AS total_anr_actual,
            GROUP_CONCAT(DISTINCT codigo)       AS programas,
            GROUP_CONCAT(DISTINCT color)        AS colores
        FROM base
        GROUP BY munic_norm
        ORDER BY n_proyectos DESC, total_anr DESC
    """)

    kpi = query(f"""
        SELECT
            COUNT(DISTINCT CASE WHEN NOT {_EFF_MUNIC_COND} THEN {_EFF_ADOP}   END) AS n_empresas,
            COUNT(DISTINCT CASE WHEN     {_EFF_MUNIC_COND} THEN {_MUNIC_NORM} END) AS n_municipios,
            COUNT(*) AS n_proyectos,
            SUM({_EFF_ANR}) AS total_anr
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE ({_VALID_ADOP})
    """, one=True)

    return render_template('adoptantes/list.html',
                           tab=tab,
                           kpi=dict(kpi),
                           empresas=[dict(r) for r in empresas],
                           municipios=[dict(r) for r in municipios])
