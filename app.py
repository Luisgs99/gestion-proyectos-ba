import os
from flask import Flask
from database import init_db
from helpers.auth import can_edit, is_admin

from blueprints.auth         import bp as auth_bp
from blueprints.dashboard    import bp as dashboard_bp
from blueprints.programas    import bp as programas_bp
from blueprints.proyectos    import bp as proyectos_bp
from blueprints.importacion  import bp as importacion_bp
from blueprints.reportes     import bp as reportes_bp
from blueprints.admin        import bp as admin_bp
from blueprints.instituciones import bp as instituciones_bp
from blueprints.adoptantes   import bp as adoptantes_bp
from blueprints.sync         import bp as sync_bp

app = Flask(__name__)
app.secret_key = 'gba-subsecretaria-innovacion-2024-secret'
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

app.jinja_env.globals.update(can_edit=can_edit, is_admin=is_admin)

app.register_blueprint(auth_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(programas_bp)
app.register_blueprint(proyectos_bp)
app.register_blueprint(importacion_bp)
app.register_blueprint(reportes_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(instituciones_bp)
app.register_blueprint(adoptantes_bp)
app.register_blueprint(sync_bp)

if __name__ == '__main__':
    init_db()
    print("\nSistema de Gestión de Proyectos - Subsecretaría de Innovación GBA")
    print("=" * 60)
    print("Credenciales de prueba:")
    print("  Admin:        admin@subsecretaria.gba.gov.ar / admin123")
    print("  Visualizador: subsecretario@gba.gov.ar / subsec123")
    print("  Agente:       agente@gba.gov.ar / agente123")
    print("=" * 60)
    app.run(debug=True, port=5050)
