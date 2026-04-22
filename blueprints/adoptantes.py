from flask import Blueprint, render_template, request
from database import query
from helpers.auth import login_required

bp = Blueprint('adoptantes', __name__)

_BASE_WHERE = "p.adoptante IS NOT NULL AND p.adoptante != '' AND p.adoptante != '-'"
_MUNIC_COND = "(LOWER(p.adoptante) LIKE 'municipalidad%' OR LOWER(p.adoptante) LIKE 'municipio %' OR LOWER(p.adoptante) LIKE 'municipio de %')"


@bp.route('/adoptantes')
@login_required
def list():
    tab = request.args.get('tab', 'empresas')

    empresas = query(f"""
        SELECT p.adoptante, p.municipio, p.sector_actividad_1 AS sector,
               COUNT(*) AS n_proyectos,
               SUM(COALESCE(p.anr_monto, 0)) AS total_anr,
               GROUP_CONCAT(DISTINCT pr.codigo) AS programas,
               GROUP_CONCAT(DISTINCT pr.color) AS colores
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE {_BASE_WHERE} AND NOT {_MUNIC_COND}
        GROUP BY p.adoptante
        ORDER BY n_proyectos DESC, total_anr DESC
    """)

    municipios = query(f"""
        SELECT p.adoptante, p.municipio,
               COUNT(*) AS n_proyectos,
               SUM(COALESCE(p.anr_monto, 0)) AS total_anr,
               GROUP_CONCAT(DISTINCT pr.codigo) AS programas,
               GROUP_CONCAT(DISTINCT pr.color) AS colores
        FROM proyectos p
        JOIN programas pr ON p.programa_id = pr.id
        WHERE {_BASE_WHERE} AND {_MUNIC_COND}
        GROUP BY p.adoptante
        ORDER BY n_proyectos DESC, total_anr DESC
    """)

    kpi = query(f"""
        SELECT
            COUNT(DISTINCT CASE WHEN NOT {_MUNIC_COND} THEN p.adoptante END) AS n_empresas,
            COUNT(DISTINCT CASE WHEN {_MUNIC_COND} THEN p.adoptante END) AS n_municipios,
            COUNT(*) AS n_proyectos,
            SUM(COALESCE(p.anr_monto, 0)) AS total_anr
        FROM proyectos p
        WHERE {_BASE_WHERE}
    """, one=True)

    return render_template('adoptantes/list.html',
                           tab=tab,
                           kpi=dict(kpi),
                           empresas=[dict(r) for r in empresas],
                           municipios=[dict(r) for r in municipios])
