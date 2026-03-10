"""
Input validation utilities for PVIGR data processing.

Provides comprehensive validation functions for data integrity checking,
input validation, and data quality assurance.
"""

from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

try:
    from .exceptions import (
        ValidationError,
        DataIntegrityError,
        ConfigurationError
    )
    from .logging_config import get_logger
except ImportError:
    from exceptions import (
        ValidationError,
        DataIntegrityError,
        ConfigurationError
    )
    from logging_config import get_logger

logger = get_logger(__name__)


class InputValidator:
    """
    Comprehensive input validation for data processing functions.
    """
    
    @staticmethod
    def validate_file_path(
        file_path: Union[str, Path],
        must_exist: bool = True,
        allowed_extensions: Optional[List[str]] = None
    ) -> Path:
        """
        Validate file path and check existence/extension.
        
        Args:
            file_path: Path to validate
            must_exist: Whether file must exist
            allowed_extensions: List of allowed file extensions (e.g., ['.csv', '.xlsx'])
            
        Returns:
            Validated Path object
            
        Raises:
            ValidationError: If validation fails
            
        Example:
            >>> path = validate_file_path("data.csv", must_exist=True, allowed_extensions=['.csv'])
        """
        if file_path is None:
            raise ValidationError("File path cannot be None")
        
        path = Path(file_path)
        
        # Check existence
        if must_exist and not path.exists():
            raise ValidationError(
                f"File not found: {path}. "
                f"Please check the path and ensure the file exists."
            )
        
        # Check extension
        if allowed_extensions:
            if path.suffix.lower() not in [ext.lower() for ext in allowed_extensions]:
                raise ValidationError(
                    f"Invalid file extension: {path.suffix}. "
                    f"Allowed extensions: {', '.join(allowed_extensions)}"
                )
        
        return path
    
    @staticmethod
    def validate_directory_path(
        dir_path: Union[str, Path],
        must_exist: bool = True,
        create_if_missing: bool = False
    ) -> Path:
        """
        Validate directory path and optionally create it.
        
        Args:
            dir_path: Directory path to validate
            must_exist: Whether directory must exist
            create_if_missing: Create directory if it doesn't exist
            
        Returns:
            Validated Path object
            
        Raises:
            ValidationError: If validation fails
        """
        if dir_path is None:
            raise ValidationError("Directory path cannot be None")
        
        path = Path(dir_path)
        
        if not path.exists():
            if create_if_missing:
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Created directory: {path}")
                except Exception as e:
                    raise ValidationError(
                        f"Failed to create directory {path}: {e}"
                    ) from e
            elif must_exist:
                raise ValidationError(
                    f"Directory not found: {path}. "
                    f"Please create the directory or set create_if_missing=True."
                )
        elif not path.is_dir():
            raise ValidationError(
                f"Path exists but is not a directory: {path}"
            )
        
        return path
    
    @staticmethod
    def validate_dataframe(
        df: Any,
        required_columns: Optional[List[str]] = None,
        min_rows: int = 1,
        allow_empty: bool = False
    ) -> pd.DataFrame:
        """
        Validate DataFrame structure and content.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            min_rows: Minimum number of rows
            allow_empty: Allow empty DataFrame
            
        Returns:
            Validated DataFrame
            
        Raises:
            ValidationError: If validation fails
        """
        # Check if it's a DataFrame
        if not isinstance(df, pd.DataFrame):
            raise ValidationError(
                f"Expected pandas DataFrame, got {type(df).__name__}"
            )
        
        # Check if empty
        if df.empty:
            if allow_empty:
                return df
            raise ValidationError(
                "DataFrame is empty. Cannot process empty data."
            )
        
        # Check minimum rows
        if len(df) < min_rows:
            raise ValidationError(
                f"DataFrame has insufficient rows. "
                f"Required: {min_rows}, Found: {len(df)}"
            )
        
        # Check required columns
        if required_columns:
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                raise ValidationError(
                    f"Missing required columns: {', '.join(missing_columns)}. "
                    f"Available columns: {', '.join(df.columns)}"
                )
        
        return df
    
    @staticmethod
    def validate_datetime_column(
        df: pd.DataFrame,
        column_name: str
    ) -> pd.DataFrame:
        """
        Validate and convert datetime column.
        
        Args:
            df: DataFrame containing datetime column
            column_name: Name of datetime column
            
        Returns:
            DataFrame with validated datetime column
            
        Raises:
            ValidationError: If validation fails
        """
        if column_name not in df.columns:
            raise ValidationError(
                f"DateTime column '{column_name}' not found in DataFrame. "
                f"Available columns: {', '.join(df.columns)}"
            )
        
        # Try to convert to datetime
        try:
            df[column_name] = pd.to_datetime(df[column_name])
        except Exception as e:
            raise ValidationError(
                f"Failed to convert '{column_name}' to datetime: {e}. "
                f"Please ensure the column contains valid date/time values."
            ) from e
        
        # Check for NaT values
        nat_count = df[column_name].isna().sum()
        if nat_count > 0:
            logger.warning(
                f"Found {nat_count} invalid datetime values in '{column_name}'"
            )
        
        return df
    
    @staticmethod
    def validate_frequency(frequency: str) -> str:
        """
        Validate pandas frequency string.
        
        Args:
            frequency: Frequency string (e.g., '1h', '30min', '1D')
            
        Returns:
            Validated frequency string
            
        Raises:
            ValidationError: If frequency is invalid
        """
        if not frequency:
            raise ValidationError("Frequency cannot be empty")
        
        valid_patterns = ['h', 'min', 'D', 'W', 'M', 'Y', 'S']
        
        # Check if frequency contains valid pattern
        if not any(pattern in frequency for pattern in valid_patterns):
            raise ValidationError(
                f"Invalid frequency: '{frequency}'. "
                f"Examples of valid frequencies: '1h', '30min', '1D'"
            )
        
        # Try to create a timedelta to validate
        try:
            pd.Timedelta(frequency)
        except Exception:
            # Some frequencies like 'M' (month) can't be converted to Timedelta
            # but are still valid for resampling
            pass
        
        return frequency
    
    @staticmethod
    def validate_numeric_range(
        value: Union[int, float],
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
        param_name: str = "value"
    ) -> Union[int, float]:
        """
        Validate numeric value is within acceptable range.
        
        Args:
            value: Value to validate
            min_value: Minimum acceptable value
            max_value: Maximum acceptable value
            param_name: Name of parameter for error messages
            
        Returns:
            Validated value
            
        Raises:
            ValidationError: If value is out of range
        """
        if not isinstance(value, (int, float)):
            raise ValidationError(
                f"{param_name} must be numeric, got {type(value).__name__}"
            )
        
        if np.isnan(value):
            raise ValidationError(f"{param_name} cannot be NaN")
        
        if np.isinf(value):
            raise ValidationError(f"{param_name} cannot be infinite")
        
        if min_value is not None and value < min_value:
            raise ValidationError(
                f"{param_name} must be >= {min_value}, got {value}"
            )
        
        if max_value is not None and value > max_value:
            raise ValidationError(
                f"{param_name} must be <= {max_value}, got {value}"
            )
        
        return value
    
    @staticmethod
    def validate_configuration(
        config: Dict[str, Any],
        required_keys: List[str],
        config_name: str = "configuration"
    ) -> Dict[str, Any]:
        """
        Validate configuration dictionary has required keys.
        
        Args:
            config: Configuration dictionary
            required_keys: List of required key names
            config_name: Name of configuration for error messages
            
        Returns:
            Validated configuration
            
        Raises:
            ConfigurationError: If validation fails
        """
        if not isinstance(config, dict):
            raise ConfigurationError(
                f"{config_name} must be a dictionary, got {type(config).__name__}"
            )
        
        missing_keys = set(required_keys) - set(config.keys())
        if missing_keys:
            raise ConfigurationError(
                f"Missing required keys in {config_name}: {', '.join(missing_keys)}. "
                f"Available keys: {', '.join(config.keys())}"
            )
        
        return config


class DataIntegrityChecker:
    """
    Check data integrity and quality issues.
    """
    
    @staticmethod
    def check_duplicates(
        df: pd.DataFrame,
        subset: Optional[List[str]] = None,
        raise_on_error: bool = False
    ) -> tuple[pd.DataFrame, int]:
        """
        Check for and optionally remove duplicate rows.
        
        Args:
            df: DataFrame to check
            subset: Columns to check for duplicates
            raise_on_error: Raise error if duplicates found
            
        Returns:
            Tuple of (cleaned_dataframe, duplicate_count)
            
        Raises:
            DataIntegrityError: If duplicates found and raise_on_error=True
        """
        duplicate_count = df.duplicated(subset=subset).sum()
        
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate rows")
            
            if raise_on_error:
                raise DataIntegrityError(
                    f"Found {duplicate_count} duplicate rows. "
                    f"Please review and remove duplicates."
                )
            
            # Remove duplicates
            df_clean = df.drop_duplicates(subset=subset, keep='first')
            logger.info(f"Removed {duplicate_count} duplicate rows")
            
            return df_clean, duplicate_count
        
        return df, 0
    
    @staticmethod
    def check_missing_data(
        df: pd.DataFrame,
        max_missing_ratio: float = 0.5,
        raise_on_error: bool = False
    ) -> Dict[str, float]:
        """
        Check for missing data in DataFrame.
        
        Args:
            df: DataFrame to check
            max_missing_ratio: Maximum acceptable missing data ratio per column
            raise_on_error: Raise error if missing data exceeds threshold
            
        Returns:
            Dictionary mapping column names to missing data ratios
            
        Raises:
            DataIntegrityError: If missing data exceeds threshold and raise_on_error=True
        """
        missing_ratios = df.isna().sum() / len(df)
        missing_dict = missing_ratios[missing_ratios > 0].to_dict()
        
        if missing_dict:
            logger.info(f"Missing data found in {len(missing_dict)} columns")
            
            # Check if any column exceeds threshold
            excessive_missing = {
                col: ratio for col, ratio in missing_dict.items()
                if ratio > max_missing_ratio
            }
            
            if excessive_missing:
                message = "Excessive missing data in columns:\n"
                for col, ratio in excessive_missing.items():
                    message += f"  - {col}: {ratio:.1%}\n"
                
                if raise_on_error:
                    raise DataIntegrityError(message)
                else:
                    logger.warning(message)
        
        return missing_dict
    
    @staticmethod
    def check_temporal_consistency(
        df: pd.DataFrame,
        datetime_column: str,
        expected_frequency: Optional[str] = None,
        raise_on_error: bool = False
    ) -> Dict[str, Any]:
        """
        Check temporal consistency of time series data.
        
        Args:
            df: DataFrame with time series data
            datetime_column: Name of datetime column
            expected_frequency: Expected sampling frequency
            raise_on_error: Raise error if issues found
            
        Returns:
            Dictionary with temporal consistency metrics
            
        Raises:
            DataIntegrityError: If critical issues found and raise_on_error=True
        """
        if datetime_column not in df.columns:
            raise ValidationError(f"DateTime column '{datetime_column}' not found")
        
        # Sort by datetime
        df_sorted = df.sort_values(datetime_column)
        time_diffs = df_sorted[datetime_column].diff()
        
        # Calculate statistics
        results = {
            'total_records': len(df),
            'time_range': (df_sorted[datetime_column].min(), df_sorted[datetime_column].max()),
            'median_interval': time_diffs.median(),
            'min_interval': time_diffs.min(),
            'max_interval': time_diffs.max(),
            'gaps': []
        }
        
        # Check for gaps (intervals significantly larger than median)
        if expected_frequency:
            expected_delta = pd.Timedelta(expected_frequency)
            gaps = time_diffs[time_diffs > expected_delta * 2]
            
            if len(gaps) > 0:
                results['gaps'] = [
                    {
                        'start': df_sorted.iloc[i-1][datetime_column],
                        'end': df_sorted.iloc[i][datetime_column],
                        'duration': gap
                    }
                    for i, gap in gaps.items()
                ]
                
                logger.warning(f"Found {len(gaps)} temporal gaps")
                
                if raise_on_error:
                    raise DataIntegrityError(
                        f"Found {len(gaps)} temporal gaps in data. "
                        f"Largest gap: {results['max_interval']}"
                    )
        
        # Check for reverse time (should not happen after sorting, but check anyway)
        negative_diffs = (time_diffs < pd.Timedelta(0)).sum()
        if negative_diffs > 0:
            message = f"Found {negative_diffs} timestamps going backward in time"
            if raise_on_error:
                raise DataIntegrityError(message)
            logger.warning(message)
        
        return results
    
    @staticmethod
    def check_value_ranges(
        df: pd.DataFrame,
        ranges: Dict[str, tuple[float, float]],
        raise_on_error: bool = False
    ) -> Dict[str, int]:
        """
        Check if values are within acceptable ranges.
        
        Args:
            df: DataFrame to check
            ranges: Dictionary mapping column names to (min, max) tuples
            raise_on_error: Raise error if out-of-range values found
            
        Returns:
            Dictionary mapping column names to count of out-of-range values
            
        Raises:
            DataIntegrityError: If out-of-range values found and raise_on_error=True
        """
        out_of_range = {}
        
        for col, (min_val, max_val) in ranges.items():
            if col not in df.columns:
                continue
            
            # Count values outside range
            below_min = (df[col] < min_val).sum()
            above_max = (df[col] > max_val).sum()
            total_oor = below_min + above_max
            
            if total_oor > 0:
                out_of_range[col] = total_oor
                logger.warning(
                    f"Column '{col}': {total_oor} values out of range "
                    f"[{min_val}, {max_val}] ({below_min} below, {above_max} above)"
                )
        
        if out_of_range and raise_on_error:
            raise DataIntegrityError(
                f"Found out-of-range values in {len(out_of_range)} columns. "
                f"Run quality control to flag these values."
            )
        
        return out_of_range


def validate_input_safely(func):
    """
    Decorator to safely validate function inputs and provide helpful error messages.
    
    Usage:
        @validate_input_safely
        def my_function(file_path: Path, df: pd.DataFrame):
            # Function implementation
            pass
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValidationError as e:
            logger.error(f"Input validation failed in {func.__name__}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}")
            raise ValidationError(
                f"Failed to validate inputs for {func.__name__}: {e}"
            ) from e
    
    return wrapper
