"""
Módulo para cargar configuración desde config.yaml
"""

import yaml
from pathlib import Path
from typing import Dict, Any

_config_path = Path(__file__).parent / 'config.yaml'

def load_config() -> Dict[str, Any]:
    """Carga la configuración desde config.yaml"""
    if not _config_path.exists():
        raise FileNotFoundError(f"No se encuentra config.yaml en {_config_path}")
    
    with open(_config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config

def get_database_config() -> Dict[str, Any]:
    """Retorna la configuración de la base de datos"""
    config = load_config()
    return config['database']

def get_flood_config() -> Dict[str, Any]:
    """Retorna la configuración del sistema de flood"""
    config = load_config()
    return config['flood_system']

def get_anomaly_config() -> Dict[str, Any]:
    """Retorna la configuración del sistema de anomalías"""
    config = load_config()
    return config['anomaly_system']

def get_sql_config_dict(database: str = None) -> Dict[str, Any]:
    """Retorna SQL_CONFIG como diccionario para compatibilidad
    
    Args:
        database: Nombre de la base de datos. Si es None, usa la base de datos por defecto.
    """
    db_config = get_database_config()
    config = {
        'server': db_config['server'],
        'port': db_config['port'],
        'username': db_config['username'],
        'password': db_config['password'],
        'driver': db_config['driver'],
        'schema': db_config['schema']
    }
    
    # Si se especifica una base de datos, usarla; si no, usar la del config (si existe)
    if database:
        config['database'] = database
    elif 'database' in db_config:
        config['database'] = db_config['database']
    else:
        # Si no hay base de datos por defecto, usar la de entrada del flood system
        flood_config = get_flood_config()
        config['database'] = flood_config.get('input_database', 'otms_main')
    
    return config




