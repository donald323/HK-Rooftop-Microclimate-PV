"""
Optional data interpolation for filling small gaps in time series data.
Only interpolates when explicitly enabled and within conservative gap limits.
"""

import pandas as pd
import json
from pathlib import Path

try:
    from .logging_config import get_logger
except ImportError:
    from logging_config import get_logger

logger = get_logger(__name__)


class DataInterpolator:
    """Apply optional interpolation to fill missing values in time series data."""
    
    def __init__(self, config_file='../config/interpolation_config.json'):
        self.config_path = Path(__file__).parent / config_file
        self.config = self._load_config()
        
    def _load_config(self):
        with open(self.config_path, 'r') as f:
            return json.load(f)
    
    def is_enabled(self):
        return self.config.get('enabled', False)
    
    def should_interpolate_column(self, column_name):
        """Check if column should be interpolated. Returns (should_interpolate, method, data_type)."""
        for data_type, settings in self.config.get('data_types', {}).items():
            if not settings.get('enabled', False):
                continue
            for pattern in settings.get('applies_to', []):
                if pattern in column_name:
                    return True, settings.get('method', 'linear'), data_type
        return False, None, None
    
    def interpolate_column(self, series, method='linear', limit=None):
        """Interpolate missing values in a series."""
        original_na = series.isna()
        interpolated = series.interpolate(method=method, limit=limit, limit_direction='both')
        interp_mask = original_na & interpolated.notna()
        return interpolated, interp_mask
    
    def interpolate_dataframe(self, df, datetime_col='DateTime'):
        """Apply interpolation to applicable columns."""
        if not self.is_enabled():
            return df.copy(), {'enabled': False}
        
        max_gap = self.config['settings']['max_gap_minutes']
        logger.info(f"Interpolation enabled (max gap: {max_gap} min)")
        
        df_result = df.copy()
        metadata = {
            'enabled': True,
            'max_gap_minutes': max_gap,
            'interpolated_columns': [],
            'total_interpolated_values': 0
        }
        
        for column in df.columns:
            if column == datetime_col:
                continue
                
            should_interp, method, data_type = self.should_interpolate_column(column)
            if not should_interp:
                continue
            
            # Interpolate with limit based on max_gap
            limit = max_gap if max_gap else None
            interpolated, interp_mask = self.interpolate_column(df[column], method, limit)
            
            if interp_mask.any():
                df_result[column] = interpolated
                
                if self.config['settings'].get('mark_interpolated', True):
                    df_result[f"{column}_interpolated"] = interp_mask
                
                count = interp_mask.sum()
                metadata['interpolated_columns'].append({
                    'column': column,
                    'data_type': data_type,
                    'method': method,
                    'count': int(count)
                })
                metadata['total_interpolated_values'] += int(count)
        
        total = metadata['total_interpolated_values']
        if total > 0:
            cols = ', '.join([f"{c['column']} ({c['count']})" for c in metadata['interpolated_columns']])
            logger.warning(f"Generated {total} synthetic values: {cols}")
        else:
            logger.info("No interpolation needed")
        
        return df_result, metadata
