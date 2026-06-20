"""
Optional data interpolation for filling small gaps in time series data.
Only interpolates when explicitly enabled and within conservative gap limits.
"""

import numpy as np
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
        """Check if column should be interpolated. Returns (should_interpolate, method, data_type, is_circular)."""
        for data_type, settings in self.config.get('data_types', {}).items():
            if not settings.get('enabled', False):
                continue
            for pattern in settings.get('applies_to', []):
                if pattern in column_name:
                    is_circular = settings.get('circular', False)
                    return True, settings.get('method', 'linear'), data_type, is_circular
        return False, None, None, False
    
    def interpolate_column(self, series, method='linear', limit=None):
        """Interpolate missing values in a series."""
        original_na = series.isna()
        interpolated = series.interpolate(method=method, limit=limit, limit_direction='both')
        interp_mask = original_na & interpolated.notna()
        return interpolated, interp_mask

    def angular_interpolate_column(self, series, limit=None):
        """
        Angular interpolation for directional variables (e.g., wind direction 0-360).
        Decomposes into sine/cosine components, interpolates those, then recomputes the angle.
        """
        original_na = series.isna()
        valid = series.dropna()

        if len(valid) < 2:
            return series, original_na

        # Convert degrees to radians
        theta = np.radians(valid)
        sin_vals = np.sin(theta)
        cos_vals = np.cos(theta)

        # Build full-length series for sin/cos with NaN at missing positions
        sin_series = pd.Series(index=series.index, dtype=float)
        cos_series = pd.Series(index=series.index, dtype=float)
        sin_series[valid.index] = sin_vals
        cos_series[valid.index] = cos_vals

        # Interpolate sin and cos components
        sin_interp = sin_series.interpolate(method='linear', limit=limit, limit_direction='both')
        cos_interp = cos_series.interpolate(method='linear', limit=limit, limit_direction='both')

        # Recompute angle from interpolated components
        angle_rad = np.arctan2(sin_interp, cos_interp)
        angle_deg = np.degrees(angle_rad)

        # Normalize to [0, 360)
        interpolated = angle_deg % 360

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
                
            should_interp, method, data_type, is_circular = self.should_interpolate_column(column)
            if not should_interp:
                continue

            limit = max_gap if max_gap else None

            # Use angular interpolation for directional variables (e.g., wind direction)
            if is_circular:
                interpolated, interp_mask = self.angular_interpolate_column(df[column], limit)
                actual_method = f'angular ({method})'
            else:
                interpolated, interp_mask = self.interpolate_column(df[column], method, limit)
                actual_method = method
            
            if interp_mask.any():
                df_result[column] = interpolated
                
                if self.config['settings'].get('mark_interpolated', True):
                    df_result[f"{column}_interpolated"] = interp_mask
                
                count = interp_mask.sum()
                metadata['interpolated_columns'].append({
                    'column': column,
                    'data_type': data_type,
                    'method': actual_method,
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
