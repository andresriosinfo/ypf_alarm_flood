"""
Detector de Anomalías usando Facebook Prophet

Este módulo implementa un detector de anomalías robusto basado en Prophet,
que es ideal para series temporales con tendencias y estacionalidad.
"""

import pandas as pd
import numpy as np
from prophet import Prophet
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import warnings
import pickle
import json
from datetime import datetime
warnings.filterwarnings('ignore')


class ProphetAnomalyDetector:
    """
    Detector de anomalías usando Facebook Prophet.
    
    Prophet es ideal para:
    - Series temporales con tendencias
    - Datos con estacionalidad (diaria, semanal, mensual)
    - Detección robusta de anomalías basada en intervalos de predicción
    """
    
    def __init__(self,
                 interval_width: float = 0.95,
                 changepoint_prior_scale: float = 0.05,
                 seasonality_mode: str = 'multiplicative',
                 daily_seasonality: bool = True,
                 weekly_seasonality: bool = True,
                 yearly_seasonality: bool = False,
                 anomaly_threshold: float = 2.0):
        """
        Parámetros:
        -----------
        interval_width : float
            Ancho del intervalo de predicción (0.95 = 95% de confianza)
        changepoint_prior_scale : float
            Flexibilidad del modelo para cambios de tendencia (mayor = más flexible)
        seasonality_mode : str
            'additive' o 'multiplicative'
        daily_seasonality : bool
            Si incluir estacionalidad diaria
        weekly_seasonality : bool
            Si incluir estacionalidad semanal
        yearly_seasonality : bool
            Si incluir estacionalidad anual
        anomaly_threshold : float
            Número de desviaciones estándar fuera del intervalo de confianza para considerar anomalía
        """
        self.interval_width = interval_width
        self.changepoint_prior_scale = changepoint_prior_scale
        self.seasonality_mode = seasonality_mode
        self.daily_seasonality = daily_seasonality
        self.weekly_seasonality = weekly_seasonality
        self.yearly_seasonality = yearly_seasonality
        self.anomaly_threshold = anomaly_threshold
        
        self.models = {}  # Un modelo por variable
        self.variable_stats = {}  # Estadísticas de cada variable
        
    def prepare_data_for_prophet(self, 
                                 df: pd.DataFrame, 
                                 variable: str,
                                 datetime_col: str = 'DATETIME') -> pd.DataFrame:
        """
        Prepara los datos en el formato requerido por Prophet.
        
        Prophet requiere columnas: 'ds' (fecha) y 'y' (valor)
        """
        prophet_df = pd.DataFrame({
            'ds': pd.to_datetime(df[datetime_col]),
            'y': df[variable].values
        })
        
        # Eliminar valores NaN
        prophet_df = prophet_df.dropna()
        
        # Ordenar por fecha
        prophet_df = prophet_df.sort_values('ds').reset_index(drop=True)
        
        return prophet_df
    
    def train_model(self,
                   df: pd.DataFrame,
                   variable: str,
                   datetime_col: str = 'DATETIME',
                   verbose: bool = False) -> Prophet:
        """
        Entrena un modelo Prophet para una variable específica.
        
        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame con los datos
        variable : str
            Nombre de la variable a modelar
        datetime_col : str
            Nombre de la columna de fecha/hora
        verbose : bool
            Si mostrar información de entrenamiento
        
        Retorna:
        --------
        Prophet : Modelo entrenado
        """
        # Preparar datos
        prophet_df = self.prepare_data_for_prophet(df, variable, datetime_col)
        
        if len(prophet_df) < 10:
            raise ValueError(f"Insuficientes datos para entrenar modelo de {variable}. Mínimo 10 puntos requeridos.")
        
        # Crear y configurar modelo
        model = Prophet(
            interval_width=self.interval_width,
            changepoint_prior_scale=self.changepoint_prior_scale,
            seasonality_mode=self.seasonality_mode,
            daily_seasonality=self.daily_seasonality,
            weekly_seasonality=self.weekly_seasonality,
            yearly_seasonality=self.yearly_seasonality
        )
        
        # Entrenar modelo
        if verbose:
            print(f"  Entrenando modelo para {variable}...")
        
        model.fit(prophet_df)
        
        # Guardar estadísticas de la variable
        self.variable_stats[variable] = {
            'mean': prophet_df['y'].mean(),
            'std': prophet_df['y'].std(),
            'min': prophet_df['y'].min(),
            'max': prophet_df['y'].max(),
            'n_points': len(prophet_df)
        }
        
        return model
    
    def detect_anomalies(self,
                        model: Prophet,
                        df: pd.DataFrame,
                        variable: str,
                        datetime_col: str = 'DATETIME') -> pd.DataFrame:
        """
        Detecta anomalías usando el modelo Prophet entrenado.
        
        Parámetros:
        -----------
        model : Prophet
            Modelo Prophet entrenado
        df : pd.DataFrame
            DataFrame con los datos a analizar
        variable : str
            Nombre de la variable
        datetime_col : str
            Nombre de la columna de fecha/hora
        
        Retorna:
        --------
        pd.DataFrame : DataFrame con predicciones y detección de anomalías
        """
        # Preparar datos
        prophet_df = self.prepare_data_for_prophet(df, variable, datetime_col)
        
        # Hacer predicciones
        forecast = model.predict(prophet_df[['ds']])
        
        # Combinar datos reales con predicciones
        results = prophet_df.copy()
        results['yhat'] = forecast['yhat'].values
        results['yhat_lower'] = forecast['yhat_lower'].values
        results['yhat_upper'] = forecast['yhat_upper'].values
        
        # Calcular residuales (diferencia entre valor real y predicho)
        results['residual'] = results['y'] - results['yhat']
        
        # Calcular desviación estándar de los residuales
        residual_std = results['residual'].std()
        
        # Detectar anomalías
        # Una anomalía es cuando:
        # 1. El valor está fuera del intervalo de confianza, O
        # 2. El residual está más de 'anomaly_threshold' desviaciones estándar del promedio
        results['outside_interval'] = (results['y'] < results['yhat_lower']) | (results['y'] > results['yhat_upper'])
        results['high_residual'] = np.abs(results['residual']) > (self.anomaly_threshold * residual_std)
        results['is_anomaly'] = results['outside_interval'] | results['high_residual']
        
        # Calcular score de anomalía (0-100, mayor = más anómalo)
        # Basado en qué tan lejos está del intervalo de confianza
        results['anomaly_score'] = 0.0
        
        # Para valores fuera del intervalo
        mask_above = results['y'] > results['yhat_upper']
        mask_below = results['y'] < results['yhat_lower']
        
        if mask_above.any():
            results.loc[mask_above, 'anomaly_score'] = (
                (results.loc[mask_above, 'y'] - results.loc[mask_above, 'yhat_upper']) / 
                (results.loc[mask_above, 'yhat_upper'] - results.loc[mask_above, 'yhat']) * 50
            ).clip(upper=100)
        
        if mask_below.any():
            results.loc[mask_below, 'anomaly_score'] = (
                (results.loc[mask_below, 'yhat_lower'] - results.loc[mask_below, 'y']) / 
                (results.loc[mask_below, 'yhat'] - results.loc[mask_below, 'yhat_lower']) * 50
            ).clip(upper=100)
        
        # Ajustar score basado en residuales
        results['anomaly_score'] = np.maximum(
            results['anomaly_score'],
            (np.abs(results['residual']) / residual_std * 20).clip(upper=100)
        )
        
        # Agregar información adicional
        results['variable'] = variable
        results['prediction_error_pct'] = np.abs(results['residual'] / results['yhat'] * 100).replace([np.inf, -np.inf], np.nan)
        
        return results
    
    def train_multiple_variables(self,
                                df: pd.DataFrame,
                                variables: List[str],
                                datetime_col: str = 'DATETIME',
                                verbose: bool = True) -> Dict[str, Prophet]:
        """
        Entrena modelos Prophet para múltiples variables.
        
        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame con los datos
        variables : List[str]
            Lista de variables a modelar
        datetime_col : str
            Nombre de la columna de fecha/hora
        verbose : bool
            Si mostrar progreso
        
        Retorna:
        --------
        Dict[str, Prophet] : Diccionario de modelos entrenados
        """
        self.models = {}
        failed_variables = []
        
        if verbose:
            print(f"\nEntrenando modelos Prophet para {len(variables)} variables...")
            print("="*80)
        
        for i, var in enumerate(variables, 1):
            try:
                if verbose:
                    print(f"[{i}/{len(variables)}] Procesando {var}...")
                
                model = self.train_model(df, var, datetime_col, verbose=False)
                self.models[var] = model
                
                if verbose:
                    print(f"  [OK] Modelo entrenado exitosamente")
                    
            except Exception as e:
                failed_variables.append(var)
                if verbose:
                    print(f"  [ERROR] Error entrenando modelo: {str(e)}")
        
        if verbose:
            print("\n" + "="*80)
            print(f"Modelos entrenados exitosamente: {len(self.models)}/{len(variables)}")
            if failed_variables:
                print(f"Variables con errores: {len(failed_variables)}")
                print(f"  {', '.join(failed_variables[:10])}")
                if len(failed_variables) > 10:
                    print(f"  ... y {len(failed_variables) - 10} más")
        
        return self.models
    
    def detect_anomalies_multiple(self,
                                  df: pd.DataFrame,
                                  variables: Optional[List[str]] = None,
                                  datetime_col: str = 'DATETIME',
                                  combine_results: bool = True) -> pd.DataFrame:
        """
        Detecta anomalías para múltiples variables.
        
        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame con los datos
        variables : Optional[List[str]]
            Lista de variables a analizar (None = todas las entrenadas)
        datetime_col : str
            Nombre de la columna de fecha/hora
        combine_results : bool
            Si combinar todos los resultados en un solo DataFrame
        
        Retorna:
        --------
        pd.DataFrame : Resultados de detección de anomalías
        """
        if variables is None:
            variables = list(self.models.keys())
        
        if not self.models:
            raise ValueError("No hay modelos entrenados. Llama a train_multiple_variables primero.")
        
        all_results = []
        
        print(f"\nDetectando anomalías en {len(variables)} variables...")
        
        for i, var in enumerate(variables, 1):
            if var not in self.models:
                print(f"[{i}/{len(variables)}] [ADVERTENCIA] {var}: Modelo no encontrado, saltando...")
                continue
            
            try:
                print(f"[{i}/{len(variables)}] Analizando {var}...", end=' ')
                results = self.detect_anomalies(self.models[var], df, var, datetime_col)
                all_results.append(results)
                
                n_anomalies = results['is_anomaly'].sum()
                print(f"[OK] ({n_anomalies} anomalias detectadas)")
                
            except Exception as e:
                print(f"[ERROR] Error: {str(e)}")
        
        if not all_results:
            raise ValueError("No se pudieron procesar variables. Verifica los datos.")
        
        if combine_results:
            combined = pd.concat(all_results, ignore_index=True)
            return combined
        else:
            return all_results
    
    def save_models(self, directory: str):
        """Guarda los modelos entrenados en archivos pickle."""
        Path(directory).mkdir(parents=True, exist_ok=True)
        
        for var, model in self.models.items():
            # Limpiar nombre de variable para nombre de archivo
            safe_name = var.replace('/', '_').replace('\\', '_').replace(' ', '_').replace('-', '_')
            model_path = Path(directory) / f"prophet_model_{safe_name}.pkl"
            
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
        
        # Guardar estadísticas
        stats_path = Path(directory) / "variable_stats.json"
        with open(stats_path, 'w') as f:
            json.dump(self.variable_stats, f, indent=2, default=str)
        
        # Guardar configuración
        config = {
            'interval_width': self.interval_width,
            'changepoint_prior_scale': self.changepoint_prior_scale,
            'seasonality_mode': self.seasonality_mode,
            'daily_seasonality': self.daily_seasonality,
            'weekly_seasonality': self.weekly_seasonality,
            'yearly_seasonality': self.yearly_seasonality,
            'anomaly_threshold': self.anomaly_threshold,
            'variables': list(self.models.keys())
        }
        
        config_path = Path(directory) / "detector_config.json"
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"\nModelos guardados en: {directory}")
        print(f"  - {len(self.models)} modelos")
        print(f"  - Estadísticas de variables")
        print(f"  - Configuración del detector")
    
    def load_models(self, directory: str):
        """Carga modelos guardados previamente."""
        directory = Path(directory)
        
        # Cargar configuración
        config_path = directory / "detector_config.json"
        config = {}
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            self.interval_width = config.get('interval_width', 0.95)
            self.changepoint_prior_scale = config.get('changepoint_prior_scale', 0.05)
            self.seasonality_mode = config.get('seasonality_mode', 'multiplicative')
            self.daily_seasonality = config.get('daily_seasonality', True)
            self.weekly_seasonality = config.get('weekly_seasonality', True)
            self.yearly_seasonality = config.get('yearly_seasonality', False)
            self.anomaly_threshold = config.get('anomaly_threshold', 2.0)
        
        # Cargar modelos
        model_files = list(directory.glob("prophet_model_*.pkl"))
        self.models = {}
        
        for model_file in model_files:
            with open(model_file, 'rb') as f:
                model = pickle.load(f)
                # Extraer nombre de variable del nombre de archivo
                var_name = model_file.stem.replace("prophet_model_", "").replace("_", "/")
                # Intentar encontrar el nombre original en la lista de variables
                if 'variables' in config:
                    for orig_var in config['variables']:
                        safe_orig = orig_var.replace('/', '_').replace('\\', '_').replace(' ', '_').replace('-', '_')
                        if safe_orig == model_file.stem.replace("prophet_model_", ""):
                            var_name = orig_var
                            break
                self.models[var_name] = model
        
        # Cargar estadísticas
        stats_path = directory / "variable_stats.json"
        if stats_path.exists():
            with open(stats_path, 'r') as f:
                self.variable_stats = json.load(f)
        
        print(f"Modelos cargados: {len(self.models)} desde {directory}")
    
    def get_anomaly_summary(self, results_df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera un resumen de anomalías detectadas.
        
        Parámetros:
        -----------
        results_df : pd.DataFrame
            DataFrame con resultados de detección
        
        Retorna:
        --------
        pd.DataFrame : Resumen por variable
        """
        summary = results_df.groupby('variable').agg({
            'is_anomaly': ['sum', 'mean'],
            'anomaly_score': ['mean', 'max'],
            'residual': ['mean', 'std'],
            'y': 'count'
        }).round(4)
        
        summary.columns = ['n_anomalies', 'anomaly_rate', 'avg_score', 'max_score', 
                          'avg_residual', 'std_residual', 'n_points']
        summary = summary.sort_values('n_anomalies', ascending=False)
        
        return summary



