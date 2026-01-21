"""
Utilidades para conexión y operaciones con SQL Server
"""

try:
    import pyodbc
except ImportError:
    print("[ERROR] pyodbc no está instalado. Ejecuta: pip install pyodbc")
    raise

try:
    from sqlalchemy import create_engine
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    print("[ADVERTENCIA] SQLAlchemy no está instalado. Usando método alternativo.")

import pandas as pd
from typing import Optional
import os


class SQLConnection:
    """Clase para manejar conexiones a SQL Server"""
    
    def __init__(self, server: str, database: str, username: str, password: str, port: int = 1433):
        """
        Inicializa la conexión a SQL Server
        
        Parámetros:
        -----------
        server : str
            IP o nombre del servidor
        database : str
            Nombre de la base de datos
        username : str
            Usuario
        password : str
            Contraseña
        port : int
            Puerto (default: 1433)
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        self._conn = None
    
    def connect(self):
        """Establece conexión a la base de datos"""
        try:
            self._conn = pyodbc.connect(self.connection_string)
            print(f"[OK] Conectado a SQL Server: {self.server}")
            return True
        except Exception as e:
            print(f"[ERROR] Error conectando a SQL Server: {str(e)}")
            return False
    
    def disconnect(self):
        """Cierra la conexión"""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """
        Ejecuta una consulta SELECT y retorna un DataFrame
        
        Parámetros:
        -----------
        query : str
            Consulta SQL
            
        Retorna:
        --------
        pd.DataFrame o None si hay error
        """
        try:
            df = pd.read_sql(query, self._conn)
            return df
        except Exception as e:
            print(f"[ERROR] Error ejecutando query: {str(e)}")
            return None
    
    def execute_non_query(self, query: str) -> bool:
        """
        Ejecuta una consulta que no retorna datos (INSERT, UPDATE, DELETE, CREATE, etc.)
        
        Parámetros:
        -----------
        query : str
            Consulta SQL
            
        Retorna:
        --------
        bool: True si fue exitoso, False si hubo error
        """
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            self._conn.commit()
            cursor.close()
            return True
        except Exception as e:
            print(f"[ERROR] Error ejecutando query: {str(e)}")
            self._conn.rollback()
            return False
    
    def write_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = "dbo", 
                       if_exists: str = "append", index: bool = False) -> bool:
        """
        Escribe un DataFrame a una tabla SQL
        
        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame a escribir
        table_name : str
            Nombre de la tabla
        schema : str
            Schema (default: dbo)
        if_exists : str
            'append', 'replace', o 'fail' (default: append)
        index : bool
            Si escribir el índice (default: False)
            
        Retorna:
        --------
        bool: True si fue exitoso, False si hubo error
        """
        try:
            full_table_name = f"{schema}.{table_name}"
            
            # Usar método directo con pyodbc (más confiable para SQL Server)
            # pandas.to_sql con method='multi' tiene problemas con muchos parámetros en SQL Server
            # Inserción manual con pyodbc (más confiable para SQL Server)
            if df.empty:
                print(f"[ADVERTENCIA] DataFrame vacío")
                return True
            
            columns = list(df.columns)
            placeholders = ','.join(['?' for _ in columns])
            columns_str = ','.join([f'[{col}]' for col in columns])
            
            if if_exists == 'replace':
                drop_query = f"IF OBJECT_ID('{schema}.{table_name}', 'U') IS NOT NULL DROP TABLE [{schema}].[{table_name}]"
                cursor = self._conn.cursor()
                cursor.execute(drop_query)
                self._conn.commit()
                cursor.close()
            
            insert_query = f"INSERT INTO [{schema}].[{table_name}] ({columns_str}) VALUES ({placeholders})"
            cursor = self._conn.cursor()
            
            # Habilitar fast_executemany para mejor rendimiento
            try:
                cursor.fast_executemany = True
            except:
                pass  # Algunas versiones de pyodbc no lo soportan
            
            # Insertar en chunks para evitar problemas con SQL Server
            chunk_size = 1000  # Aumentado para mejor rendimiento
            total_inserted = 0
            total_rows = len(df)
            
            print(f"  Insertando {total_rows:,} filas en chunks de {chunk_size}...")
            import sys
            import time
            
            start_time = time.time()
            
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                rows = [tuple(row) for row in chunk.values]
                
                try:
                    cursor.executemany(insert_query, rows)
                    total_inserted += len(rows)
                except Exception as e:
                    print(f"\n[ERROR] Error insertando chunk {i//chunk_size + 1}: {str(e)}")
                    self._conn.rollback()
                    cursor.close()
                    return False
                
                # Mostrar progreso cada 5000 filas o al final
                if total_inserted % 5000 == 0 or total_inserted == total_rows:
                    pct = (total_inserted/total_rows*100) if total_rows > 0 else 0
                    elapsed = time.time() - start_time
                    rate = total_inserted / elapsed if elapsed > 0 else 0
                    print(f"  Progreso: {total_inserted:,}/{total_rows:,} filas ({pct:.1f}%) - {rate:.0f} filas/seg")
                    sys.stdout.flush()
            
            self._conn.commit()
            cursor.close()
            elapsed = time.time() - start_time
            print(f"[OK] {total_inserted:,} filas escritas en {full_table_name} en {elapsed:.1f} segundos")
            return True
                
        except Exception as e:
            print(f"[ERROR] Error escribiendo a {table_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def create_table_if_not_exists(self, table_name: str, schema: str = "dbo", 
                                   columns: dict = None) -> bool:
        """
        Crea una tabla si no existe
        
        Parámetros:
        -----------
        table_name : str
            Nombre de la tabla
        schema : str
            Schema (default: dbo)
        columns : dict
            Diccionario con nombre_columna: tipo_dato
            
        Retorna:
        --------
        bool: True si fue exitoso o la tabla ya existe
        """
        try:
            # Verificar si la tabla existe
            check_query = f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
            """
            cursor = self._conn.cursor()
            cursor.execute(check_query)
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            
            if exists:
                print(f"[INFO] La tabla {schema}.{table_name} ya existe")
                return True
            
            # Crear tabla
            if columns:
                columns_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
                create_query = f"""
                    CREATE TABLE [{schema}].[{table_name}] (
                        {columns_def}
                    )
                """
                return self.execute_non_query(create_query)
            else:
                print(f"[ADVERTENCIA] No se especificaron columnas para crear la tabla")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error creando tabla {table_name}: {str(e)}")
            return False
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()



