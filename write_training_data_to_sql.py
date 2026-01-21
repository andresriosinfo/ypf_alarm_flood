"""
Script para escribir los datos de entrenamiento a SQL Server
Escribe los datos limpios en la tabla datos_proceso
"""

import sys
from pathlib import Path
import pandas as pd

# Agregar el directorio padre al path
sys.path.append(str(Path(__file__).parent))

from sql_utils import SQLConnection

# Cargar configuración desde config.yaml
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))
from config_loader import get_sql_config_dict, get_anomaly_config

# Configuración de conexión SQL
SQL_CONFIG = get_sql_config_dict(get_anomaly_config().get('input_database', 'otms_main'))
ANOMALY_CONFIG = get_anomaly_config()


def create_datos_proceso_table(sql_conn: SQLConnection):
    """Crea la tabla datos_proceso si no existe"""
    print("\n[INFO] Verificando/creando tabla datos_proceso...")
    
    # Definir estructura de la tabla
    # Necesitamos una estructura flexible para múltiples variables
    # Opción 1: Formato largo (datetime, variable_name, value)
    columns = {
        'id': 'BIGINT IDENTITY(1,1) PRIMARY KEY',
        'datetime': 'DATETIME NOT NULL',
        'variable_name': 'VARCHAR(100) NOT NULL',
        'value': 'DECIMAL(18,6)',
        'source_file': 'VARCHAR(255)',
        'created_at': 'DATETIME DEFAULT GETDATE()'
    }
    
    sql_conn.create_table_if_not_exists(ANOMALY_CONFIG['input_table'], columns=columns)
    
    # Crear índices si no existen
    indexes = [
        (f"idx_datetime", f"CREATE INDEX idx_datetime ON {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['input_table']}(datetime)"),
        (f"idx_variable", f"CREATE INDEX idx_variable ON {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['input_table']}(variable_name)"),
        (f"idx_created", f"CREATE INDEX idx_created ON {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['input_table']}(created_at)")
    ]
    
    for idx_name, idx_query in indexes:
        try:
            # Verificar si el índice ya existe
            check_query = f"""
                SELECT COUNT(*) 
                FROM sys.indexes 
                WHERE name = '{idx_name}' AND object_id = OBJECT_ID('{SQL_CONFIG['schema']}.{ANOMALY_CONFIG['input_table']}')
            """
            cursor = sql_conn._conn.cursor()
            cursor.execute(check_query)
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            
            if not exists:
                sql_conn.execute_non_query(idx_query)
        except Exception as e:
            # Ignorar errores de índices
            pass


def transform_data_to_long_format(df: pd.DataFrame, datetime_col: str = 'DATETIME', 
                                  source_file: str = None) -> pd.DataFrame:
    """
    Transforma datos de formato ancho (variables en columnas) a formato largo
    
    Parámetros:
    -----------
    df : pd.DataFrame
        DataFrame con formato ancho
    datetime_col : str
        Nombre de la columna de fecha
    source_file : str
        Nombre del archivo fuente
        
    Retorna:
    --------
    pd.DataFrame en formato largo
    """
    # Seleccionar columnas de variables (excluir datetime)
    variable_cols = [col for col in df.columns if col != datetime_col]
    
    # Convertir a formato largo
    df_long = df.melt(
        id_vars=[datetime_col],
        value_vars=variable_cols,
        var_name='variable_name',
        value_name='value'
    )
    
    # Agregar source_file si se proporciona
    if source_file:
        df_long['source_file'] = source_file
    
    # Renombrar datetime
    df_long.rename(columns={datetime_col: 'datetime'}, inplace=True)
    
    # Eliminar filas con valores nulos
    df_long = df_long.dropna(subset=['value'])
    
    return df_long


def main():
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    print("="*80)
    print("ESCRIBIENDO DATOS DE ENTRENAMIENTO A SQL SERVER")
    print("="*80)
    sys.stdout.flush()
    
    # Configuración
    data_dir = Path("output")
    datetime_col = 'DATETIME'
    
    # Encontrar archivos de datos limpios
    cleaned_files = list(data_dir.glob("*_cleaned.csv"))
    
    if not cleaned_files:
        print(f"\n[ERROR] No se encontraron archivos de datos limpios en {data_dir}")
        print("   Ejecuta primero el protocolo de seleccion de variables:")
        print("   python run_protocol.py")
        return
    
    print(f"\n[INFO] Archivos de datos encontrados: {len(cleaned_files)}")
    
    # Conectar a SQL
    sql_conn = SQLConnection(**SQL_CONFIG)
    if not sql_conn.connect():
        return
    
    try:
        # Crear tabla si no existe
        create_datos_proceso_table(sql_conn)
        
        # Procesar cada archivo
        total_rows = 0
        
        for data_file in cleaned_files:
            print(f"\n{'='*80}")
            print(f"Procesando: {data_file.name}")
            print(f"{'='*80}")
            
            # Cargar datos
            df = pd.read_csv(data_file, parse_dates=[datetime_col])
            print(f"  Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
            
            # Transformar a formato largo
            print("  Transformando a formato largo...")
            import sys
            sys.stdout.flush()
            
            df_long = transform_data_to_long_format(
                df, 
                datetime_col=datetime_col,
                source_file=data_file.stem
            )
            print(f"  Filas en formato largo: {len(df_long):,}")
            sys.stdout.flush()
            
            # Escribir a SQL
            print("  Escribiendo a SQL Server (esto puede tomar varios minutos)...")
            sys.stdout.flush()
            
            success = sql_conn.write_dataframe(
                df_long,
                table_name=ANOMALY_CONFIG['input_table'],
                if_exists='append'
            )
            sys.stdout.flush()
            
            if success:
                total_rows += len(df_long)
                print(f"  [OK] Datos escritos exitosamente")
            else:
                print(f"  [ERROR] Error escribiendo datos")
        
        print(f"\n{'='*80}")
        print("RESUMEN")
        print(f"{'='*80}")
        print(f"[OK] Total de filas escritas: {total_rows:,}")
        print(f"[OK] Base de datos: {SQL_CONFIG['database']}")
        print(f"[OK] Tabla: {SQL_CONFIG['schema']}.{ANOMALY_CONFIG['input_table']}")
        
    finally:
        sql_conn.disconnect()


if __name__ == '__main__':
    main()



