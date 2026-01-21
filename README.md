# Sistema de Detección de Anomalías con Prophet

Sistema completo de detección de anomalías en series temporales usando Facebook Prophet, integrado con SQL Server para producción.

## Características

- Detección de anomalías usando Facebook Prophet
- Entrenamiento de modelos desde SQL Server
- Procesamiento en tiempo real con worker continuo
- Integración completa con SQL Server
- Configuración centralizada mediante YAML
- Listo para producción

## Instalación

### Requisitos

- Python 3.8+
- SQL Server con ODBC Driver 17
- Acceso a base de datos SQL Server

### Pasos

1. **Clonar o descargar el repositorio**

2. **Instalar dependencias:**
```bash
pip install -r requirements.txt
```

3. **Configurar conexión SQL:**
```bash
# Copiar el archivo de ejemplo
cp config.yaml.example config.yaml

# Editar config.yaml con tus credenciales
```

4. **Configurar `config.yaml`:**
```yaml
database:
  server: "TU_SERVIDOR_SQL"
  port: 1433
  username: "TU_USUARIO"
  password: "TU_CONTRASEÑA"
  driver: "{ODBC Driver 17 for SQL Server}"
  schema: "dbo"

anomaly_system:
  input_database: "tu_base_datos"
  input_table: "datos_proceso"
  output_database: "tu_base_datos"
  output_table: "anomalies_detector"
```

## Estructura del Proyecto

```
github/
├── pipeline/
│   └── scripts/
│       └── prophet_anomaly_detector.py  # Detector principal
├── sql_utils.py                          # Utilidades SQL
├── config_loader.py                      # Cargador de configuración
├── config.yaml.example                   # Ejemplo de configuración
├── write_training_data_to_sql.py         # Escribir datos a SQL
├── train_from_sql.py                     # Entrenar modelos
├── detect_from_sql.py                    # Detectar anomalías (PRODUCCIÓN)
├── worker_procesamiento.py               # Worker continuo (opcional)
├── requirements.txt                       # Dependencias
└── README.md                             # Este archivo
```

## Flujo del Sistema

```
1. Datos de Entrada (CSV) 
   ↓
2. write_training_data_to_sql.py → Escribe a SQL (tabla: datos_proceso)
   ↓
3. train_from_sql.py → Entrena modelos Prophet → Guarda modelos .pkl
   ↓
4. detect_from_sql.py → Lee SQL → Detecta anomalías → Escribe resultados (tabla: anomalies_detector)
   ↓
5. worker_procesamiento.py (opcional) → Procesa continuamente nuevos datos
```

## Uso

### Paso 1: Escribir Datos de Entrada

Si tienes archivos CSV con datos de proceso:

```bash
python write_training_data_to_sql.py
```

**Nota:** Este script busca archivos CSV en la carpeta `output/` con el patrón `*_cleaned.csv`.

### Paso 2: Entrenar Modelos

Entrena los modelos Prophet leyendo datos desde SQL:

```bash
python train_from_sql.py
```

Los modelos se guardan en `pipeline/models/prophet/`.

### Paso 3: Detectar Anomalías (Producción)

Detecta anomalías en los datos de SQL y escribe resultados:

```bash
python detect_from_sql.py
```

### Paso 4: Procesamiento Continuo (Opcional)

Ejecuta un worker que procesa automáticamente nuevos datos:

```bash
# Verificar cada 10 minutos (default)
python worker_procesamiento.py

# Verificar cada 5 minutos
python worker_procesamiento.py --interval 5
```

## Estructura de Tablas SQL

### Tabla de Entrada: `datos_proceso`

```sql
CREATE TABLE dbo.datos_proceso (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    datetime DATETIME NOT NULL,
    variable_name VARCHAR(100) NOT NULL,
    value DECIMAL(18,6),
    source_file VARCHAR(255),
    created_at DATETIME DEFAULT GETDATE()
)
```

### Tabla de Salida: `anomalies_detector`

```sql
CREATE TABLE dbo.anomalies_detector (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    ds DATETIME NOT NULL,
    y DECIMAL(18,6),
    yhat DECIMAL(18,6),
    yhat_lower DECIMAL(18,6),
    yhat_upper DECIMAL(18,6),
    residual DECIMAL(18,6),
    outside_interval BIT,
    high_residual BIT,
    is_anomaly BIT,
    anomaly_score DECIMAL(5,2),
    variable VARCHAR(100) NOT NULL,
    prediction_error_pct DECIMAL(5,2),
    source_file VARCHAR(255),
    processed_at DATETIME DEFAULT GETDATE()
)
```

**Nota:** Las tablas se crean automáticamente al ejecutar los scripts.

## Formato de Datos

### Entrada (CSV)

Los archivos CSV deben tener:
- Columna `DATETIME` con fechas/horas
- Columnas con nombres de variables (una por variable)
- Valores numéricos

Ejemplo:
```csv
DATETIME,VAR1,VAR2,VAR3
2024-01-01 00:00:00,10.5,20.3,30.1
2024-01-01 00:30:00,10.7,20.5,30.2
```

### SQL (Formato Largo)

Los datos en SQL se almacenan en formato largo:
- `datetime`: Fecha/hora
- `variable_name`: Nombre de la variable
- `value`: Valor numérico

## Configuración Avanzada

### Parámetros del Detector

Puedes modificar los parámetros del detector en `train_from_sql.py`:

```python
detector = ProphetAnomalyDetector(
    interval_width=0.95,              # 95% intervalo de confianza
    changepoint_prior_scale=0.05,     # Flexibilidad del modelo
    seasonality_mode='multiplicative', # Modo de estacionalidad
    daily_seasonality=True,           # Estacionalidad diaria
    weekly_seasonality=True,          # Estacionalidad semanal
    yearly_seasonality=False,         # Estacionalidad anual
    anomaly_threshold=2.0             # Umbral de anomalía (desviaciones estándar)
)
```

## Solución de Problemas

### Error: "No se encuentra config.yaml"

**Solución:** Copia `config.yaml.example` a `config.yaml` y completa las credenciales.

### Error: "pyodbc no está instalado"

**Solución:** 
```bash
pip install pyodbc
```

### Error: "ODBC Driver 17 for SQL Server not found"

**Solución:** Instala el driver ODBC desde [Microsoft](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### Error: "No se encontraron modelos entrenados"

**Solución:** Ejecuta primero `train_from_sql.py` para entrenar los modelos.

## Notas Importantes

1. **Seguridad:** Nunca subas `config.yaml` con credenciales reales a Git. Usa `.gitignore` (ya incluido).

2. **Modelos:** Los modelos entrenados se guardan en `pipeline/models/prophet/`. No olvides hacer backup.

3. **Rendimiento:** El entrenamiento puede tardar varios minutos dependiendo del número de variables.

4. **Datos:** Asegúrate de tener al menos 10 puntos de datos por variable para entrenar el modelo.

## Licencia

Este proyecto es de uso interno.

## Contribución

Para contribuir, por favor:
1. Crea una rama para tu feature
2. Realiza tus cambios
3. Envía un pull request

## Contacto

Para preguntas o soporte, contacta al equipo de desarrollo.
