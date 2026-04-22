from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import check_password_hash
from database import query

bp = Blueprint('auth', __name__)


@bp.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('dashboard.dashboard'))
    return redirect(url_for('auth.login'))


@bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email    = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        user = query("SELECT * FROM users WHERE email=? AND activo=1", (email,), one=True)
        if user and check_password_hash(user['password_hash'], password):
            session['user_id'] = user['id']
            session['nombre']  = f"{user['nombre']} {user['apellido']}"
            session['rol']     = user['rol']
            session['email']   = user['email']
            return redirect(url_for('dashboard.dashboard'))
        flash('Email o contraseña incorrectos.', 'danger')
    return render_template('auth/login.html')


@bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('auth.login'))
