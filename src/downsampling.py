"""
Data downsampling utilities for PVIGR data processing.
Provides functions to downsample time series data to specified frequencies.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
from tqdm import tqdm

try:
    from .logging_config import get_logger
    from .validation import InputValidator
    from .exceptions import ValidationError, DataProcessingError
except ImportError:
    from logging_config import get_logger
    from validation import InputValidator
    from exceptions import ValidationError, DataProcessingError

logger = get_logger(__name__)


class DataDownsampler:
    """
    Downsample time series data to specified frequencies with appropriate aggregation methods.
    """
    
    def __init__(self, default_frequency='1H'):
        """
        Initialize data downsampler.
        
        Args:
            default_frequency: Default downsampling frequency (e.g., '1H', '30min', '1D')
        """
        self.default_frequency = default_frequency
        self.aggregation_methods = self._define_aggregation_methods()
    
    def _define_aggregation_methods(self):
        """
        Define how different variable types should be aggregated during downsampling.
        
        Returns:
            Dictionary mapping variable patterns to aggregation methods
        """
        return {
            # Temperature variables - use mean
            'temperature': {
                'method': 'mean',
                'patterns': ['Temp', 'TC']
            },
            # Humidity variables - use mean  
            'humidity': {
                'method': 'mean',
                'patterns': ['Humi']
            },
            # Wind speed - use mean
            'wind_speed': {
                'method': 'mean', 
                'patterns': ['Met_Speed']
            },
            # Wind direction - use circular mean (special handling needed)
            'wind_direction': {
                'method': 'circular_mean',
                'patterns': ['Met_Dir']
            },
            # Soil moisture - use mean
            'soil_moisture': {
                'method': 'mean',
                'patterns': ['SoilMoisture']
            },
            # Solar irradiance - use mean
            'solar_irradiance': {
                'method': 'mean',
                'patterns': ['Pyranometer']
            },
            # Power - use mean for average power output (PV systems and wind turbines)
            'power': {
                'method': 'mean',
                'patterns': ['Optimizer']
            }
        }
    
    def _get_aggregation_method(self, column_name):
        """
        Determine the appropriate aggregation method for a column.
        
        Args:
            column_name: Name of the column
            
        Returns:
            Aggregation method string ('mean', 'sum', 'circular_mean', etc.)
        """
        column_name = str(column_name)
        
        for var_type, config in self.aggregation_methods.items():
            for pattern in config['patterns']:
                if pattern in column_name:
                    return config['method']
        
        # Default to mean for unknown variables
        return 'mean'
    
    def _circular_mean(self, angles):
        """
        Calculate circular mean for wind direction data.
        
        Args:
            angles: Series of angles in degrees
            
        Returns:
            Circular mean in degrees
        """
        # Remove NaN values
        valid_angles = angles.dropna()
        
        if len(valid_angles) == 0:
            return np.nan
        
        # Convert to radians
        angles_rad = np.deg2rad(valid_angles)
        
        # Calculate circular mean
        x_mean = np.mean(np.cos(angles_rad))
        y_mean = np.mean(np.sin(angles_rad))
        
        # Convert back to degrees and normalize to 0-360
        circular_mean_rad = np.arctan2(y_mean, x_mean)
        circular_mean_deg = np.rad2deg(circular_mean_rad)
        
        # Ensure positive angle (0-360)
        if circular_mean_deg < 0:
            circular_mean_deg += 360
            
        return circular_mean_deg
    
    def downsample_dataframe(self, df, frequency=None, datetime_column=None):
        """
        Downsample a DataFrame to the specified frequency with validation.
        
        Args:
            df: Input DataFrame with time series data
            frequency: Downsampling frequency (e.g., '1h', '30min', '1D')
            datetime_column: Name of datetime column (if not index)
            
        Returns:
            Downsampled DataFrame
            
        Raises:
            ValidationError: If inputs are invalid
            DataProcessingError: If downsampling fails
        """
        # Validate DataFrame
        df = InputValidator.validate_dataframe(
            df,
            min_rows=1,
            allow_empty=False
        )
        
        # Validate and use frequency
        if frequency is None:
            frequency = self.default_frequency
        frequency = InputValidator.validate_frequency(frequency)
        
        logger.info(f"Downsampling {len(df)} records to {frequency} frequency")
        
        try:
            # Prepare DataFrame with datetime index
            df_work = df.copy()
            
            # Handle datetime column/index
            if datetime_column is not None:
                if datetime_column not in df_work.columns:
                    raise ValidationError(
                        f"Datetime column '{datetime_column}' not found. "
                        f"Available columns: {', '.join(df_work.columns)}"
                    )
                df_work = InputValidator.validate_datetime_column(df_work, datetime_column)
                df_work = df_work.set_index(datetime_column)
            elif not isinstance(df_work.index, pd.DatetimeIndex):
                # Try to find a datetime column
                datetime_candidates = ['DateTime', 'datetime', 'Timestamp', 'timestamp']
                found_col = None
                for col in datetime_candidates:
                    if col in df_work.columns:
                        found_col = col
                        break
                
                if found_col:
                    logger.info(f"Using '{found_col}' as datetime column")
                    df_work = InputValidator.validate_datetime_column(df_work, found_col)
                    df_work = df_work.set_index(found_col)
                else:
                    raise ValidationError(
                        "No datetime column found and index is not DatetimeIndex. "
                        f"Please specify datetime_column parameter. "
                        f"Available columns: {', '.join(df.columns)}"
                    )
            
            # Ensure index is datetime
            if not isinstance(df_work.index, pd.DatetimeIndex):
                raise ValidationError("Index must be DatetimeIndex for downsampling")
            
            # Apply downsampling with appropriate aggregation methods
            downsampled_data = {}
            
            for column in df_work.columns:
                method = self._get_aggregation_method(column)
                
                if method == 'mean':
                    downsampled_data[column] = df_work[column].resample(frequency).mean()
                elif method == 'sum':
                    downsampled_data[column] = df_work[column].resample(frequency).sum()
                elif method == 'circular_mean':
                    downsampled_data[column] = df_work[column].resample(frequency).apply(self._circular_mean)
                else:
                    # Default to mean
                    downsampled_data[column] = df_work[column].resample(frequency).mean()
            
            # Combine into DataFrame
            df_downsampled = pd.DataFrame(downsampled_data)
            
            # Reset index to get datetime as column
            df_downsampled.reset_index(inplace=True)
            
            return df_downsampled
            
        except ValidationError:
            raise
        except Exception as e:
            logger.error(f"Error during downsampling: {str(e)}")
            raise DataProcessingError(f"Downsampling failed: {str(e)}") from e
    
    def downsample_file(self, input_file, output_file, frequency=None, datetime_column=None):
        """
        Downsample a CSV file to the specified frequency.
        
        Args:
            input_file: Path to input CSV file (quality controlled data)
            output_file: Path to output CSV file (downsampled data)
            frequency: Downsampling frequency (e.g., '1H', '30min', '1D')
            datetime_column: Name of datetime column (auto-detect if None)
            
        Returns:
            Dictionary with downsampling statistics
        """
        input_file = Path(input_file)
        output_file = Path(output_file)
        
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        if frequency is None:
            frequency = self.default_frequency
        
        # Read data
        df = pd.read_csv(input_file)
        
        # Get original data info
        original_rows = len(df)
        
        # Downsample data
        df_downsampled = self.downsample_dataframe(df, frequency, datetime_column)
        
        # Save result
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df_downsampled.to_csv(output_file, index=False)
        
        # Calculate statistics
        downsampled_rows = len(df_downsampled)
        reduction_factor = original_rows / downsampled_rows if downsampled_rows > 0 else 0
        
        stats = {
            'input_file': str(input_file),
            'output_file': str(output_file),
            'frequency': frequency,
            'original_rows': original_rows,
            'downsampled_rows': downsampled_rows,
            'reduction_factor': reduction_factor,
            'columns_processed': list(df_downsampled.columns)
        }
        
        # Store stats without printing details
        return stats

    def downsample_qc_merged_file(self, input_file, output_file, frequency=None, datetime_column=None):
        """
        Downsample quality-controlled merged files containing mixed variable types.
        
        Args:
            input_file: Path to quality controlled merged file
            output_file: Path to output downsampled file
            frequency: Downsampling frequency
            datetime_column: Name of datetime column
            
        Returns:
            Dictionary with downsampling statistics
        """
        # Standard downsampling works for mixed types - patterns handle variable detection
        return self.downsample_file(input_file, output_file, frequency, datetime_column)
