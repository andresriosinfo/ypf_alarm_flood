"""
Script para entrenar modelos leyendo datos desde SQL Server
"""

import sys
from pathlib import Path
import pandas as pd

# Agregar el directorio padre al path
sys.path.append(str(Path(__file__).parent))

from sql_utils import SQLConnection
from pipeline.scripts.prophet_anomaly_detector import ProphetAnomalyDetector

# Cargar configuración desde config.yaml
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))
from config_loader import get_sql_config_dict, get_anomaly_config

# Configuración de conexión SQL
SQL_CONFIG = get_sql_config_dict(get_anomaly_config().get('input_database', 'otms_main'))
ANOMALY_CONFIG = get_anomaly_config()


def read_data_from_sql(sql_conn: SQLConnection, start_date: str = None, 
                       end_date: str = None) -> pd.DataFrame:
    """
    Lee datos desde SQL Server y los convierte a formato ancho
    
    Parámetros:
    -----------
    sql_conn : SQLConnection
        Conexión a SQL
    start_date : str
        Fecha de inicio (opcional, formato: 'YYYY-MM-DD')
    end_date : str
        Fecha de fin (opcional, formato: 'YYYY-MM-DD')
        
    Retorna:
    --------
    pd.DataFrame en formato ancho (datetime + variables en columnas)
    """
    print("\n[INFO] Leyendo datos desde SQL Server...")
    
    # Construir query usando configuración
    query = f"SELECT datetime, variable_name, value FROM {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['input_table']}"
    
    conditions = []
    if start_date:
        conditions.append(f"datetime >= '{start_date}'")
    if end_date:
        conditions.append(f"datetime <= '{end_date}'")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ORDER BY datetime, variable_name"
    
    print(f"  Query: {query[:100]}...")
    
    # Ejecutar query
    df_long = sql_conn.execute_query(query)
    
    if df_long is None or len(df_long) == 0:
        print("[ERROR] No se encontraron datos en SQL")
        return None
    
    print(f"  Filas leídas: {len(df_long):,}")
    
    # Convertir a formato ancho
    print("  Transformando a formato ancho...")
    df_wide = df_long.pivot_table(
        index='datetime',
        columns='variable_name',
        values='value',
        aggfunc='first'  # Si hay duplicados, tomar el primero
    )
    
    # Resetear índice para que datetime sea columna
    df_wide = df_wide.reset_index()
    df_wide.rename(columns={'datetime': 'DATETIME'}, inplace=True)
    
    print(f"  Dimensiones finales: {df_wide.shape[0]} filas x {df_wide.shape[1]} columnas")
    print(f"  Variables: {df_wide.shape[1] - 1}")
    print(f"  Rango temporal: {df_wide['DATETIME'].min()} a {df_wide['DATETIME'].max()}")
    
    return df_wide


def main():
    print("="*80)
    print("ENTRENAMIENTO DE MODELOS DESDE SQL SERVER")
    print("="*80)
    
    # Configuración
    models_dir = Path("pipeline/models/prophet")
    models_dir.mkdir(parents=True, exist_ok=True)
    
    # Conectar a SQL
    sql_conn = SQLConnection(**SQL_CONFIG)
    if not sql_conn.connect():
        return
    
    try:
        # Leer datos desde SQL
        # Si quieres filtrar por fechas, puedes usar:
        # df = read_data_from_sql(sql_conn, start_date='2024-10-01', end_date='2024-10-31')
        df = read_data_from_sql(sql_conn)
        
        if df is None:
            return
        
        # Seleccionar variables (excluir datetime)
        variables = [col for col in df.columns if col != 'DATETIME']
        
        print(f"\n[OK] Entrenando modelos para {len(variables)} variables")
        
        # Crear detector
        detector = ProphetAnomalyDetector(
            interval_width=0.95,
            changepoint_prior_scale=0.05,
            seasonality_mode='multiplicative',
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=False,
            anomaly_threshold=2.0
        )
        
        # Entrenar modelos
        print("\n" + "="*80)
        print("ENTRENANDO MODELOS")
        print("="*80)
        
        try:
            detector.train_multiple_variables(
                df=df,
                variables=variables,
                datetime_col='DATETIME',
                verbose=True
            )
        except KeyboardInterrupt:
            print("\n\n[ADVERTENCIA] Entrenamiento interrumpido por el usuario")
            return
        except Exception as e:
            print(f"\n[ERROR] Error durante el entrenamiento: {str(e)}")
            import traceback
            traceback.print_exc()
            return
        
        # Guardar modelos
        print("\n" + "="*80)
        print("GUARDANDO MODELOS")
        print("="*80)
        
        try:
            detector.save_models(str(models_dir))
            print(f"\n[OK] Modelos guardados exitosamente en: {models_dir}")
        except Exception as e:
            print(f"\n[ERROR] Error guardando modelos: {str(e)}")
            import traceback
            traceback.print_exc()
            return
        
        # Resumen final
        print("\n" + "="*80)
        print("RESUMEN")
        print("="*80)
        print(f"[OK] Modelos entrenados: {len(detector.models)}")
        print(f"[OK] Ubicacion: {models_dir}")
        print(f"\nPróximos pasos:")
        print(f"  1. Ejecuta 'python detect_from_sql.py' para detectar anomalías desde SQL")
        
    finally:
        sql_conn.disconnect()


if __name__ == '__main__':
    main()



