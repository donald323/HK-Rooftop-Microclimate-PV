"""
Quality control for meteorological and environmental data.
Processing order: 1) Constant value detection, 2) Sensor failure periods, 3) Boundary checks
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from .logging_config import get_logger
    from .exceptions import ValidationError, ConfigurationError
except ImportError:
    from logging_config import get_logger
    from exceptions import ValidationError, ConfigurationError

logger = get_logger(__name__)


def _resolve_config_path(config_file: str) -> Path:
    """Resolve config paths whether absolute or relative to this module."""
    candidate = Path(config_file)
    if candidate.is_absolute():
        return candidate
    return Path(__file__).parent / candidate


class QualityController:
    """
    Apply comprehensive quality control to data including boundaries and failure periods.
    """
    
    def __init__(self, boundaries_file='../config/qc_boundaries.json',
                 failures_file='../config/sensor_failures.json',
                 constant_value_file='../config/constant_value_detection.json',
                 data_type='microclimate'):
        """
        Initialize quality controller with configurations and validation.
        
        Args:
            boundaries_file: Path to JSON file containing QC boundaries
            failures_file: Path to JSON file containing sensor failure periods
            constant_value_file: Path to JSON file for constant value detection config
            data_type: Type of data to process ('microclimate', 'power', etc.)
            
        Raises:
            ConfigurationError: If configuration files cannot be loaded
            ValidationError: If data_type is invalid
        """
        # Validate data_type
        valid_types = ['microclimate', 'power', 'weather']
        if data_type not in valid_types:
            raise ValidationError(
                f"Invalid data_type: '{data_type}'. "
                f"Valid types: {', '.join(valid_types)}"
            )
        
        self.data_type = data_type
        
        try:
            self.boundaries = self._load_boundaries(boundaries_file)
            logger.info(f"Loaded QC boundaries for {data_type}: {len(self.boundaries)} variable types")
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load QC boundaries from {boundaries_file}: {e}"
            )
        
        try:
            self.sensor_failures = self._load_sensor_failures(failures_file)
            if self.sensor_failures:
                logger.info(f"Loaded sensor failure periods: {len(self.sensor_failures)} files configured")
        except Exception as e:
            logger.warning(f"Could not load sensor failures: {e}. Continuing without failure detection.")
            self.sensor_failures = {}
        
        try:
            self.constant_value_config = self._load_constant_value_config(constant_value_file)
            if self.constant_value_config and self.constant_value_config.get('enabled', True):
                logger.info(f"Loaded constant value detection: {self.constant_value_config['window_days']}-day window")
        except Exception as e:
            logger.warning(f"Could not load constant value config: {e}. Continuing without constant value detection.")
            self.constant_value_config = {}
    
    def _load_boundaries(self, boundaries_file):
        """
        Load QC boundaries from JSON file with validation.
        
        Raises:
            ConfigurationError: If file cannot be loaded or is invalid
        """
        boundaries_path = _resolve_config_path(boundaries_file)
        
        if not boundaries_path.exists():
            raise ConfigurationError(
                f"QC boundaries file not found: {boundaries_path}. "
                f"Please ensure the configuration file exists."
            )
        
        try:
            with open(boundaries_path, 'r') as f:
                all_boundaries = json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"Invalid JSON in boundaries file {boundaries_path}: {e}"
            )
        except Exception as e:
            raise ConfigurationError(
                f"Failed to read boundaries file {boundaries_path}: {e}"
            )
        
        if self.data_type not in all_boundaries:
            logger.warning(
                f"No QC boundaries defined for data_type '{self.data_type}'. "
                f"Available types: {', '.join(all_boundaries.keys())}"
            )
            return {}
        
        return all_boundaries.get(self.data_type, {})
    
    def _load_sensor_failures(self, failures_file):
        """Load sensor failure periods from JSON file."""
        failures_path = _resolve_config_path(failures_file)
        try:
            with open(failures_path, 'r') as f:
                failures_config = json.load(f)
            return failures_config.get('sensor_failures', {})
        except FileNotFoundError:
            # Fallback to hardcoded definitions if file not found
            return {
                'microclimate/dev9010D231200001.csv': {  # Tower C bare roof
                    'start': '2024-07-28',
                    'end': '2024-10-16', 
                    'sensors': ['Humi_', 'Met_', 'TC', 'Temp_']
                },
                'microclimate/dev9010D231200002.csv': {  # Library Bare Roof
                    'start': '2024-07-31',
                    'end': '2025-02-10',
                    'sensors': ['Humi_', 'Met_', 'TC', 'Temp_']
                }
            }
    
    def _load_constant_value_config(self, constant_value_file):
        """Load constant value detection configuration from JSON file."""
        config_path = _resolve_config_path(constant_value_file)
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            return {}
    
    def _get_variable_type(self, column_name):
        """
        Determine variable type based on column name patterns.
        
        Args:
            column_name: Name of the column to classify
            
        Returns:
            Variable type key or None if no match found
        """
        column_name = str(column_name)
        
        for var_type, config in self.boundaries.items():
            for pattern in config['applies_to']:
                if pattern in column_name:
                    return var_type
        return None
    
    def apply_constant_value_detection(self, df, exclude_columns=None):
        """
        Detect and flag days where variables have constant values (sensor failures).
        Groups data by date and checks if standard deviation is below tolerance for window_days.
        
        Args:
            df: DataFrame with datetime index or DateTime column
            exclude_columns: List of column names to skip
            
        Returns:
            Tuple of (processed_dataframe, detection_stats)
        """
        if not self.constant_value_config or not self.constant_value_config.get('enabled', True):
            return df.copy(), {}
        
        df_checked = df.copy()
        detection_stats = {}
        
        # Get datetime column
        datetime_col = None
        if 'datetime' in df_checked.columns:
            datetime_col = 'datetime'
        elif 'DateTime' in df_checked.columns:
            datetime_col = 'DateTime'
        else:
            # Try to use index if it's datetime-based
            if hasattr(df_checked.index, 'to_pydatetime'):
                df_checked = df_checked.reset_index()
                datetime_col = df_checked.columns[0]
            else:
                logger.warning("No datetime column found for constant value detection")
                return df_checked, {}
        
        # Ensure datetime column is properly typed
        df_checked[datetime_col] = pd.to_datetime(df_checked[datetime_col])
        
        # Get configuration parameters
        default_window = self.constant_value_config.get('window_days', 1)
        default_tolerance = self.constant_value_config.get('tolerance', 1e-6)
        applies_to = self.constant_value_config.get('applies_to', {})
        
        if exclude_columns is None:
            exclude_columns = []
        
        # Add datetime column to exclusions
        exclude_columns = list(exclude_columns) + [datetime_col]
        
        # Create date column for grouping
        df_checked['_date'] = df_checked[datetime_col].dt.date
        
        # Get unique dates sorted
        unique_dates = sorted(df_checked['_date'].unique())
        
        # Process each variable category
        for var_category, var_config in applies_to.items():
            if not var_config.get('enabled', True):
                continue
            
            patterns = var_config.get('patterns', [])
            window_days = var_config.get('window_days', default_window)
            tolerance = var_config.get('tolerance', default_tolerance)
            
            # Find matching columns
            matching_cols = []
            for col in df_checked.columns:
                if col in exclude_columns or col == '_date':
                    continue
                for pattern in patterns:
                    if pattern in col:
                        matching_cols.append(col)
                        break
            
            # Check each matching column for constant values
            for col in matching_cols:
                # Group by date and calculate daily statistics
                grouped = df_checked.groupby('_date')[col]
                daily_std = grouped.std()
                
                # Use counter approach to detect consecutive constant days
                consecutive_count = 0
                consecutive_days = []
                all_flagged_dates = []
                flagged_count = 0
                
                for date in unique_dates:
                    if date in daily_std.index and daily_std[date] < tolerance:
                        # Constant day detected
                        consecutive_count += 1
                        consecutive_days.append(date)
                        
                        # If we've reached the window threshold, flag these days
                        if consecutive_count >= window_days:
                            for day in consecutive_days:
                                day_mask = df_checked['_date'] == day
                                flagged_count += day_mask.sum()
                                all_flagged_dates.append(str(day))
                                df_checked.loc[day_mask, col] = np.nan
                            # Reset counter
                            consecutive_count = 0
                            consecutive_days = []
                    else:
                        # Non-constant day, reset counter
                        consecutive_count = 0
                        consecutive_days = []
                
                # Store statistics if any days were flagged
                if all_flagged_dates:
                    detection_stats[col] = {
                        'variable_category': var_category,
                        'flagged_count': flagged_count,
                        'window_days': window_days,
                        'tolerance': tolerance,
                        'dates': all_flagged_dates
                    }
        
        # Remove temporary date column
        df_checked = df_checked.drop(columns=['_date'])
        
        return df_checked, detection_stats
    
    def apply_sensor_failures(self, df, filename):
        """
        Apply sensor failure periods by setting data to NaN.
        
        Args:
            df: DataFrame with datetime index or DateTime column
            filename: Name of the data file
        
        Returns:
            Tuple of (processed_dataframe, failure_stats)
        """
        if filename not in self.sensor_failures:
            return df.copy(), {}
        
        df_failures = df.copy()
        failure_info = self.sensor_failures[filename]
        
        # Ensure datetime column is available
        datetime_col = None
        if 'datetime' in df_failures.columns:
            datetime_col = 'datetime'
        elif 'DateTime' in df_failures.columns:
            datetime_col = 'DateTime'
        elif hasattr(df_failures.index, 'name') and 'datetime' in str(df_failures.index.name).lower():
            # Use index if it's datetime-based
            pass
        else:
            return df_failures, {}
        
        # Apply datetime conversion if needed
        if datetime_col:
            df_failures[datetime_col] = pd.to_datetime(df_failures[datetime_col])
            failure_mask = (df_failures[datetime_col] >= failure_info['start']) & \
                          (df_failures[datetime_col] <= failure_info['end'])
        else:
            # Use datetime index
            failure_mask = (df_failures.index >= failure_info['start']) & \
                          (df_failures.index <= failure_info['end'])
        
        # Find affected columns and set to NaN during failure period
        affected_columns = []
        for sensor_pattern in failure_info['sensors']:
            pattern_cols = [col for col in df_failures.columns if sensor_pattern in col]
            affected_columns.extend(pattern_cols)
        
        failure_stats = {}
        if affected_columns:
            affected_count = failure_mask.sum()
            df_failures.loc[failure_mask, affected_columns] = np.nan
            
            failure_stats = {
                'affected_columns': affected_columns,
                'failure_period': f"{failure_info['start']} to {failure_info['end']}",
                'affected_timestamps': affected_count,
                'sensor_patterns': failure_info['sensors']
            }
        
        return df_failures, failure_stats
    
    def apply_boundaries(self, df, exclude_columns=None):
        """
        Apply QC boundaries to DataFrame, setting out-of-range values to NaN.
        
        Args:
            df: Input DataFrame
            exclude_columns: List of column names to skip (e.g., ['DateTime'])
            
        Returns:
            DataFrame with QC applied
        """
        if exclude_columns is None:
            exclude_columns = ['DateTime']
        
        df_qc = df.copy()
        qc_stats = {}
        
        for column in df_qc.columns:
            if column in exclude_columns:
                continue
                
            var_type = self._get_variable_type(column)
            if var_type is None:
                continue
            
            # Get boundaries for this variable type
            config = self.boundaries[var_type]
            
            # Use time-dependent boundaries if configured (works for power, irradiance, etc.)
            if config.get('time_dependent', {}).get('enabled', False):
                qc_stats[column] = self._apply_time_dependent_boundaries(df_qc, column, config, var_type)
            else:
                # Standard boundary application
                lower_bound = config['lower_bound']
                upper_bound = config['upper_bound']
                
                # Count values before QC
                initial_count = df_qc[column].notna().sum()
                
                # Apply boundaries
                mask = (df_qc[column] <= lower_bound) | (df_qc[column] >= upper_bound)
                flagged_count = mask.sum()
                
                if flagged_count > 0:
                    df_qc.loc[mask, column] = np.nan
                    
                    # Store QC statistics
                    qc_stats[column] = {
                        'variable_type': var_type,
                        'initial_count': initial_count,
                        'flagged_count': flagged_count,
                        'flagged_percentage': (flagged_count / initial_count * 100) if initial_count > 0 else 0,
                        'bounds': f"{lower_bound} to {upper_bound} {config['unit']}"
                    }
        
        return df_qc, qc_stats
    
    def _apply_time_dependent_boundaries(self, df_qc, column, config, var_type):
        """
        Apply time-dependent boundaries based on configuration.
        Works for any variable type: power, solar irradiance, etc.
        
        Args:
            df_qc: DataFrame to modify (modified in place)
            column: Column name to apply boundaries to
            config: Boundary configuration dict with time_dependent settings
            var_type: Variable type identifier for statistics
            
        Returns:
            QC statistics dict
        """

        # Fallback info
        lower_bound = config['lower_bound']
        upper_bound = config['upper_bound']
        initial_count = df_qc[column].notna().sum()
        mask = (df_qc[column] <= lower_bound) | (df_qc[column] >= upper_bound)
        flagged_count = mask.sum()
        if flagged_count > 0:
            df_qc.loc[mask, column] = np.nan
        fall_back_info = {
                'variable_type': var_type,
                'initial_count': initial_count,
                'flagged_count': flagged_count,
                'flagged_percentage': (flagged_count / initial_count * 100) if initial_count > 0 else 0,
                'bounds': f"{lower_bound} to {upper_bound} {config['unit']}"
            }

        # Check if time-dependent QC is enabled and configured
        if not config.get('time_dependent', {}).get('enabled', False):
            # Fall back to standard boundaries
            return fall_back_info
        
        # Parse time-dependent configuration
        time_config = config['time_dependent']
        daytime_config = time_config['daytime']
        nighttime_config = time_config['nighttime']
        
        start_hour, end_hour = daytime_config['hours']
        daytime_lower = daytime_config['lower_bound']
        nighttime_lower = nighttime_config['lower_bound']
        nighttime_upper = nighttime_config.get('upper_bound', upper_bound)  # Use config upper_bound if not specified
        
        # Parse datetime column (try both common formats)
        if 'DateTime' in df_qc.columns:
            dt_col = 'DateTime'
        elif 'datetime' in df_qc.columns:
            dt_col = 'datetime'
        else:
            # Fallback to standard boundaries if no datetime column
            return fall_back_info
        
        # Ensure datetime column is datetime type
        df_qc[dt_col] = pd.to_datetime(df_qc[dt_col])
        
        # Extract hour from datetime
        hours = df_qc[dt_col].dt.hour
        
        # Count values before QC
        initial_count = df_qc[column].notna().sum()
        
        # Create time-dependent masks based on configuration
        daytime_mask = (hours >= start_hour) & (hours < end_hour)
        daytime_out_of_bounds = daytime_mask & ((df_qc[column] < daytime_lower) | (df_qc[column] >= upper_bound))
        
        # Nighttime: all other hours
        nighttime_mask = ~daytime_mask
        nighttime_out_of_bounds = nighttime_mask & ((df_qc[column] < nighttime_lower) | (df_qc[column] >= nighttime_upper))
        
        # Combine masks
        total_mask = daytime_out_of_bounds | nighttime_out_of_bounds
        flagged_count = total_mask.sum()
        
        # Apply boundaries
        if flagged_count > 0:
            df_qc.loc[total_mask, column] = np.nan
        
        # Calculate detailed statistics
        daytime_flagged = daytime_out_of_bounds.sum()
        nighttime_flagged = nighttime_out_of_bounds.sum()
        
        return {
            'variable_type': var_type,
            'initial_count': initial_count,
            'flagged_count': flagged_count,
            'flagged_percentage': (flagged_count / initial_count * 100) if initial_count > 0 else 0,
            'bounds': f"Daytime ({start_hour:02d}:00-{end_hour:02d}:00): {daytime_lower} to {upper_bound} {config['unit']}, Nighttime: {nighttime_lower} to {nighttime_upper} {config['unit']}",
            'daytime_flagged': daytime_flagged,
            'nighttime_flagged': nighttime_flagged,
            'daytime_percentage': (daytime_flagged / initial_count * 100) if initial_count > 0 else 0,
            'nighttime_percentage': (nighttime_flagged / initial_count * 100) if initial_count > 0 else 0
        }
    
    def apply_to_file(self, input_file, output_file, exclude_columns=None):
        """
        Apply comprehensive QC to a CSV file with integrated processing order:
        1) Constant value detection, 2) Sensor failure periods, 3) Boundary checks
        
        Args:
            input_file: Path to input CSV file (merged data)
            output_file: Path to output CSV file (quality controlled)
            exclude_columns: List of column names to skip
            
        Returns:
            Dictionary of comprehensive QC statistics
        """
        input_file = Path(input_file)
        output_file = Path(output_file)
        
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        # Read data
        df = pd.read_csv(input_file)
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
        elif 'DateTime' in df.columns:
            df['DateTime'] = pd.to_datetime(df['DateTime'])
        
        filename = input_file.name
        
        # If input_file is from data/merged/microclimate/file.csv, create "microclimate/file.csv"
        if 'merged' in str(input_file):
            parts = input_file.parts
            if 'merged' in parts:
                merged_idx = parts.index('merged')
                if merged_idx + 1 < len(parts):  # Has subdirectory after merged
                    relative_key = '/'.join(parts[merged_idx + 1:])
                else:
                    relative_key = filename
            else:
                relative_key = filename
        else:
            relative_key = filename
        
        all_stats = {
            'filename': filename,
            'relative_key': relative_key,
            'processing_order': ['constant_values', 'failures', 'boundaries'],
            'constant_values': {},
            'failures': {},
            'boundaries': {}
        }
        
        # Step 1: Detect constant values (automated sensor failure detection)
        df_step1, constant_value_stats = self.apply_constant_value_detection(df, exclude_columns)
        all_stats['constant_values'] = constant_value_stats
        
        # Step 2: Apply sensor failure periods (documented failures)
        df_step2, failure_stats = self.apply_sensor_failures(df_step1, relative_key)
        all_stats['failures'] = failure_stats
        
        # Step 3: Apply boundary quality control (range validation)
        df_final, boundary_stats = self.apply_boundaries(df_step2, exclude_columns)
        all_stats['boundaries'] = boundary_stats
        
        # Save result
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df_final.to_csv(output_file, index=False)
        
        # Print comprehensive summary
        self._print_qc_summary(filename, all_stats)
        
        return all_stats
    
    def _print_qc_summary(self, filename, stats):
        """Log a minimal QC summary with final results only."""
        # Calculate total flagged values across all QC checks
        total_flagged = 0
        if stats['constant_values']:
            total_flagged += sum(s['flagged_count'] for s in stats['constant_values'].values())
        if stats['boundaries']:
            total_flagged += sum(s['flagged_count'] for s in stats['boundaries'].values())
        
        logger.info("QC applied to %s: %d values flagged", filename, total_flagged)
    
    def get_boundaries_info(self):
        """Get information about all configured boundaries."""
        info = {}
        for var_type, config in self.boundaries.items():
            info[var_type] = {
                'description': config['description'],
                'range': f"{config['lower_bound']} to {config['upper_bound']} {config['unit']}",
                'patterns': config['applies_to']
            }
        return info