"""
Worker para procesamiento continuo de anomalías
Verifica periódicamente si hay nuevos datos y los procesa automáticamente
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import argparse
import logging
from typing import Optional

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

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)  # Asegurar que se muestre en consola
    ]
)
logger = logging.getLogger(__name__)


def create_anomalies_table(sql_conn: SQLConnection):
    """Crea la tabla anomalies_detector si no existe"""
    columns = {
        'id': 'BIGINT IDENTITY(1,1) PRIMARY KEY',
        'ds': 'DATETIME NOT NULL',
        'y': 'DECIMAL(18,6)',
        'yhat': 'DECIMAL(18,6)',
        'yhat_lower': 'DECIMAL(18,6)',
        'yhat_upper': 'DECIMAL(18,6)',
        'residual': 'DECIMAL(18,6)',
        'outside_interval': 'BIT',
        'high_residual': 'BIT',
        'is_anomaly': 'BIT',
        'anomaly_score': 'DECIMAL(5,2)',
        'variable': 'VARCHAR(100) NOT NULL',
        'prediction_error_pct': 'DECIMAL(5,2)',
        'source_file': 'VARCHAR(255)',
        'processed_at': 'DATETIME DEFAULT GETDATE()'
    }
    
    sql_conn.create_table_if_not_exists(ANOMALY_CONFIG['output_table'], columns=columns)
    
    # Crear índices si no existen
    indexes = [
        (f"idx_ds", f"CREATE INDEX idx_ds ON {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['output_table']}(ds)"),
        (f"idx_variable", f"CREATE INDEX idx_variable ON {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['output_table']}(variable)"),
        (f"idx_is_anomaly", f"CREATE INDEX idx_is_anomaly ON {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['output_table']}(is_anomaly)"),
        (f"idx_processed_at", f"CREATE INDEX idx_processed_at ON {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['output_table']}(processed_at)")
    ]
    
    for idx_name, idx_query in indexes:
        try:
            check_query = f"""
                SELECT COUNT(*) 
                FROM sys.indexes 
                WHERE name = '{idx_name}' AND object_id = OBJECT_ID('{SQL_CONFIG['schema']}.{ANOMALY_CONFIG['output_table']}')
            """
            cursor = sql_conn._conn.cursor()
            cursor.execute(check_query)
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            
            if not exists:
                sql_conn.execute_non_query(idx_query)
        except Exception as e:
            pass


def get_last_processed_datetime(sql_conn: SQLConnection) -> Optional[datetime]:
    """Obtiene el último datetime procesado de anomalies_detector"""
    query = f"SELECT MAX(ds) as last_ds FROM {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['output_table']}"
    result = sql_conn.execute_query(query)
    
    if result is None or result.empty or result['last_ds'].iloc[0] is None:
        return None
    
    return pd.to_datetime(result['last_ds'].iloc[0])


def get_new_data_from_sql(sql_conn: SQLConnection, since_datetime: datetime) -> Optional[pd.DataFrame]:
    """
    Lee nuevos datos desde SQL Server desde un datetime específico
    
    Parámetros:
    -----------
    sql_conn : SQLConnection
        Conexión a SQL Server
    since_datetime : datetime
        Fecha desde la cual leer datos nuevos
    """
    query = f"""
        SELECT datetime, variable_name, value, source_file
        FROM {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['input_table']}
        WHERE datetime > '{since_datetime.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY datetime, variable_name
    """
    
    df_long = sql_conn.execute_query(query)
    
    if df_long is None or len(df_long) == 0:
        return None
    
    return df_long


def convert_long_to_wide(df_long: pd.DataFrame) -> pd.DataFrame:
    """Convierte datos de formato largo a ancho"""
    df_wide = df_long.pivot_table(
        index='datetime',
        columns='variable_name',
        values='value',
        aggfunc='first'
    )
    
    df_wide = df_wide.reset_index()
    df_wide.rename(columns={'datetime': 'DATETIME'}, inplace=True)
    
    return df_wide


def process_new_anomalies(sql_conn: SQLConnection,
                         detector: ProphetAnomalyDetector,
                         since_datetime: datetime) -> tuple[int, int]:
    """
    Procesa nuevos datos y detecta anomalías
    
    Retorna:
    --------
    tuple: (número de datetimes procesados, número de anomalías detectadas)
    """
    # Leer nuevos datos
    df_long = get_new_data_from_sql(sql_conn, since_datetime)
    
    if df_long is None or len(df_long) == 0:
        return 0, 0
    
    print(f"  [INFO] Datos encontrados: {len(df_long)} filas")
    sys.stdout.flush()
    
    # Convertir a formato ancho
    df_wide = convert_long_to_wide(df_long)
    unique_datetimes = df_wide['DATETIME'].nunique()
    print(f"  [INFO] Datos convertidos: {unique_datetimes} datetime(s) únicos, {df_wide.shape[1]-1} variables")
    sys.stdout.flush()
    
    # Obtener variables disponibles en los modelos
    available_vars = [v for v in detector.models.keys() if v in df_wide.columns]
    
    if not available_vars:
        print(f"  [ADVERTENCIA] No hay variables comunes entre modelos ({len(detector.models)} modelos) y datos ({len(df_wide.columns)-1} variables)")
        sys.stdout.flush()
        return 0, 0
    
    print(f"  [INFO] Variables a analizar: {len(available_vars)} de {len(detector.models)} modelos")
    sys.stdout.flush()
    
    # Detectar anomalías
    try:
        results = detector.detect_anomalies_multiple(
            df=df_wide,
            variables=available_vars,
            datetime_col='DATETIME',
            combine_results=True
        )
        
        if results is None or len(results) == 0:
            return 0, 0
        
        # Agregar source_file si no existe
        if 'source_file' not in results.columns:
            source_file_map = df_long.groupby(['datetime', 'variable_name'])['source_file'].first().to_dict()
            results['source_file'] = results.apply(
                lambda row: source_file_map.get((row['ds'], row['variable']), 'unknown'),
                axis=1
            )
        
        # Convertir booleanos a 0/1 para SQL Server BIT
        bool_cols = ['outside_interval', 'high_residual', 'is_anomaly']
        for col in bool_cols:
            if col in results.columns:
                results[col] = results[col].astype(int)
        
        # Limpiar valores numéricos para SQL Server
        if 'anomaly_score' in results.columns:
            results['anomaly_score'] = results['anomaly_score'].fillna(0).clip(lower=0, upper=999.99)
        
        if 'prediction_error_pct' in results.columns:
            results['prediction_error_pct'] = results['prediction_error_pct'].replace([np.inf, -np.inf], np.nan)
            results['prediction_error_pct'] = results['prediction_error_pct'].fillna(0).clip(lower=0, upper=999.99)
        
        # Seleccionar columnas para escribir
        columns_to_write = [
            'ds', 'y', 'yhat', 'yhat_lower', 'yhat_upper', 'residual',
            'outside_interval', 'high_residual', 'is_anomaly', 'anomaly_score',
            'variable', 'prediction_error_pct', 'source_file'
        ]
        
        available_cols = [col for col in columns_to_write if col in results.columns]
        results_to_write = results[available_cols].copy()
        
        # Escribir a SQL
        success = sql_conn.write_dataframe(
            results_to_write,
            table_name=ANOMALY_CONFIG['output_table'],
            if_exists='append'
        )
        
        if success:
            n_anomalies = results['is_anomaly'].sum()
            return unique_datetimes, n_anomalies
        else:
            return 0, 0
            
    except Exception as e:
        logger.error(f"Error procesando anomalías: {str(e)}", exc_info=True)
        return 0, 0


class AnomalyDetectionWorker:
    """Worker para procesamiento continuo de anomalías"""
    
    def __init__(self, check_interval_minutes: int = 10):
        """
        Inicializa el worker
        
        Parámetros:
        -----------
        check_interval_minutes : int
            Intervalo en minutos entre verificaciones (default: 10)
        """
        self.check_interval_minutes = check_interval_minutes
        self.check_interval_seconds = check_interval_minutes * 60
        self.sql_conn = None
        self.detector = None
        self.last_processed_datetime = None
        self.total_processed = 0
        self.total_anomalies = 0
        self.iterations = 0
        
    def initialize(self) -> bool:
        """Inicializa conexiones y carga modelos"""
        # Verificar modelos
        models_dir = Path("pipeline/models/prophet")
        if not models_dir.exists() or not list(models_dir.glob("prophet_model_*.pkl")):
            print(f"[ERROR] No se encontraron modelos entrenados en {models_dir}")
            print("[ERROR] Entrena los modelos primero: python train_from_sql.py")
            sys.stdout.flush()
            return False
        
        # Conectar a SQL
        print("[INFO] Conectando a SQL Server...")
        sys.stdout.flush()
        self.sql_conn = SQLConnection(**SQL_CONFIG)
        if not self.sql_conn.connect():
            print("[ERROR] No se pudo conectar a SQL Server")
            sys.stdout.flush()
            return False
        
        try:
            # Crear tabla de resultados si no existe
            create_anomalies_table(self.sql_conn)
            
            # Cargar detector y modelos
            print(f"[INFO] Cargando modelos desde: {models_dir}")
            sys.stdout.flush()
            self.detector = ProphetAnomalyDetector()
            self.detector.load_models(str(models_dir))
            print(f"[OK] {len(self.detector.models)} modelos cargados exitosamente")
            sys.stdout.flush()
            
            # Obtener último datetime procesado
            self.last_processed_datetime = get_last_processed_datetime(self.sql_conn)
            if self.last_processed_datetime:
                print(f"[INFO] Último datetime procesado: {self.last_processed_datetime}")
            else:
                print("[INFO] No hay datos procesados anteriormente. Procesará desde hace 24 horas")
                # Si no hay datos procesados, buscar desde hace 24 horas
                self.last_processed_datetime = datetime.now() - timedelta(hours=24)
            sys.stdout.flush()
            
            return True
            
        except Exception as e:
            logger.error(f"Error inicializando worker: {str(e)}", exc_info=True)
            return False
    
    def check_and_process(self) -> bool:
        """
        Verifica si hay nuevos datos y los procesa
        
        Retorna:
        --------
        bool: True si se procesaron datos, False si no había datos nuevos
        """
        try:
            # Verificar si hay nuevos datos
            df_long = get_new_data_from_sql(self.sql_conn, self.last_processed_datetime)
            
            if df_long is None or len(df_long) == 0:
                return False
            
            # Procesar nuevos datos
            n_datetimes, n_anomalies = process_new_anomalies(
                self.sql_conn,
                self.detector,
                self.last_processed_datetime
            )
            
            if n_datetimes > 0:
                # Actualizar último datetime procesado
                query = f"SELECT MAX(datetime) as last_datetime FROM {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['input_table']}"
                result = self.sql_conn.execute_query(query)
                if result is not None and not result.empty and result['last_datetime'].iloc[0] is not None:
                    new_last = pd.to_datetime(result['last_datetime'].iloc[0])
                    if new_last > self.last_processed_datetime:
                        self.last_processed_datetime = new_last
                
                self.total_processed += n_datetimes
                self.total_anomalies += n_anomalies
                
                print(f"  [OK] Procesados: {n_datetimes} datetime(s), Anomalías: {n_anomalies} (Total: {self.total_anomalies})")
                sys.stdout.flush()
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error en check_and_process: {str(e)}", exc_info=True)
            return False
    
    def run(self):
        """Ejecuta el worker en modo continuo"""
        print("\n" + "="*80)
        print("WORKER DE DETECCIÓN DE ANOMALÍAS")
        print("="*80)
        sys.stdout.flush()
        
        if not self.initialize():
            print("[ERROR] No se pudo inicializar el worker")
            sys.stdout.flush()
            return
        
        print(f"\n[INFO] Intervalo de verificación: {self.check_interval_minutes} minutos")
        print("[INFO] Presiona Ctrl+C para detener\n")
        sys.stdout.flush()
        
        try:
            while True:
                self.iterations += 1
                current_time = datetime.now()
                
                print(f"[Iteración {self.iterations}] {current_time.strftime('%Y-%m-%d %H:%M:%S')} - Verificando nuevos datos...")
                sys.stdout.flush()
                
                processed = self.check_and_process()
                
                if not processed:
                    print("  [INFO] No hay datos nuevos para procesar")
                    sys.stdout.flush()
                
                # Mostrar estadísticas cada 10 iteraciones
                if self.iterations % 10 == 0:
                    print(f"  [ESTADÍSTICAS] {self.total_processed} datetime(s) procesados, {self.total_anomalies} anomalía(s) detectadas")
                    sys.stdout.flush()
                
                # Esperar antes de la próxima verificación
                print(f"  [INFO] Esperando {self.check_interval_minutes} minutos hasta la próxima verificación...\n")
                sys.stdout.flush()
                time.sleep(self.check_interval_seconds)
                
        except KeyboardInterrupt:
            print("\n" + "="*80)
            print("WORKER DETENIDO POR EL USUARIO")
            print("="*80)
            print(f"[OK] Iteraciones completadas: {self.iterations}")
            print(f"[OK] Total de datetimes procesados: {self.total_processed}")
            print(f"[OK] Total de anomalías detectadas: {self.total_anomalies}")
            sys.stdout.flush()
        
        finally:
            if self.sql_conn:
                self.sql_conn.disconnect()
                print("[INFO] Conexión a SQL cerrada")
                sys.stdout.flush()


def main():
    print("Iniciando worker...")
    sys.stdout.flush()
    
    parser = argparse.ArgumentParser(
        description='Worker para procesamiento continuo de anomalías',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Verificar cada 10 minutos (default)
  python worker_procesamiento.py
  
  # Verificar cada 5 minutos
  python worker_procesamiento.py --interval 5
  
  # Verificar cada 30 minutos
  python worker_procesamiento.py --interval 30
        """
    )
    parser.add_argument('--interval', type=int, default=10,
                       help='Intervalo en minutos entre verificaciones (default: 10)')
    
    args = parser.parse_args()
    
    print(f"Configurado para verificar cada {args.interval} minutos")
    sys.stdout.flush()
    
    worker = AnomalyDetectionWorker(check_interval_minutes=args.interval)
    worker.run()


if __name__ == '__main__':
    main()



