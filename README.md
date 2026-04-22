# GBA Innova — Sistema de Gestión de Proyectos de Innovación
## Subsecretaría de Innovación · Provincia de Buenos Aires

---

## Descripción

Plataforma web para gestionar y hacer seguimiento de proyectos tecnológicos entre universidades, empresas y el Gobierno de la Provincia de Buenos Aires. Incluye los programas **FITBA**, **ORBITA**, **CLIC**, **Clínica Tecnológica** y **FONICS**.

---

## Instalación

### Requisitos
- Python 3.8+
- pip

### Dependencias
```bash
pip install flask werkzeug openpyxl pandas xlsxwriter
```

### Iniciar el servidor
```bash
cd gestion-proyectos-ba
python app.py
```
Accedé en: **http://localhost:5050**

---

## Usuarios de acceso

| Rol | Email | Contraseña | Permisos |
|-----|-------|------------|----------|
| **Administrador** | admin@subsecretaria.gba.gov.ar | admin123 | Todo: ver, crear, editar, eliminar, gestionar usuarios |
| **Visualizador** | subsecretario@gba.gov.ar | subsec123 | Ver todos los módulos y reportes (sin editar) |
| **Agente** | agente@gba.gov.ar | agente123 | Ver/editar sus proyectos asignados, ver el resto |

---

## Funcionalidades

### 📊 Dashboard
- Estadísticas globales: total de proyectos, ANR comprometido, activos
- Gráficos de distribución por programa y por estado
- Feed de últimas novedades

### 📁 Programas
- Listado de los 5 programas (FITBA, ORBITA, CLIC, Clínica Tecnológica, FONICS)
- Vista detallada por programa con:
  - Tabla de proyectos con avance
  - Gráficos: estados, municipios, ANR por proyecto
  - Avance global por hito

### 🚀 Proyectos
- ABM completo de proyectos
- Por cada proyecto:
  - Datos: beneficiario, adoptante, ANR, municipio, fechas
  - Seguimiento de hitos del programa (actualizables)
  - Registro de novedades (logros, problemas, hitos alcanzados)
- Filtros por programa, estado, búsqueda de texto
- Exportación a Excel

### 📤 Importar Excel
- Importación masiva de proyectos desde planillas Excel
- Descarga de plantillas por programa (incluye columnas de hitos)
- Historial de importaciones

### 📈 Reportes
- Visualizaciones agregadas: proyectos por programa, ANR, avance
- Exportación filtrada a Excel

### 👥 Administración de usuarios (solo Admin)
- Crear/editar/desactivar usuarios
- Asignar roles

---

## Hitos por programa

| Programa | Hitos |
|----------|-------|
| **FITBA** | Presentación → Aprobación → Convenio → 3 desembolsos → 2 informes → Informe final → Cierre |
| **FONICS** | Evaluación → Convenio → Desembolso → Informe avance → Informe final → Cierre |
| **CLIC** | Diagnóstico → Apertura → 1ra actividad → Informe semestral → Evaluación anual |
| **Clínica** | Diagnóstico → Plan → Implementación → Informe final |
| **ORBITA** | Selección → Inicio → Hito formativo → Mentorías → Demo Day → Cierre |

---

## Migración a MySQL (opcional)

Para usar MySQL en vez de SQLite:

1. Instalar: `pip install PyMySQL`
2. En `database.py` reemplazar las conexiones SQLite por:
```python
import pymysql
conn = pymysql.connect(host='localhost', user='usuario', password='pass', db='gba_innovacion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
```
3. El esquema SQL es compatible con MySQL con ajustes mínimos (AUTOINCREMENT → AUTO_INCREMENT).

---

## Estructura del proyecto

```
gestion-proyectos-ba/
├── app.py              # Aplicación Flask con todas las rutas
├── database.py         # Funciones de base de datos + inicialización
├── proyectos_ba.db     # Base de datos SQLite (se crea al iniciar)
├── static/
│   ├── css/style.css   # Estilos completos
│   └── js/main.js      # JavaScript
├── templates/
│   ├── base.html       # Layout base con sidebar
│   ├── auth/           # Login
│   ├── dashboard/      # Dashboard principal
│   ├── programs/       # Listado y detalle de programas, importación
│   ├── projects/       # Listado, detalle y formulario de proyectos
│   ├── reports/        # Reportes y visualizaciones
│   └── admin/          # Gestión de usuarios
└── uploads/            # Archivos Excel subidos
```
