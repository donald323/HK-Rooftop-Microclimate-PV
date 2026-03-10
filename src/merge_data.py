"""
Merge raw microclimate and power data from multiple files.

This module provides functions to merge raw sensor data from multiple CSV and Excel files
into unified datasets. Handles different file formats and data structures specific to
PVIGR monitoring stations.
"""

from typing import List, Optional, Union
from pathlib import Path
import pandas as pd
from tqdm import tqdm

try:
    from .logging_config import get_logger
    from .exceptions import DataParsingError, FileNotFoundError as PVIGRFileNotFoundError, ValidationError
    from .exceptions import DataProcessingError
    from .validation import InputValidator, DataIntegrityChecker
except ImportError:
    from logging_config import get_logger
    from exceptions import DataParsingError, FileNotFoundError as PVIGRFileNotFoundError, ValidationError
    from exceptions import DataProcessingError
    from validation import InputValidator, DataIntegrityChecker

logger = get_logger(__name__)


def parse_txt_microclimate_file(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Parse semicolon-delimited txt files with comma-separated values.
    
    Used for Lib_GR and PV_Lift3 microclimate data. The file format consists of:
    - Line 1: BoardID information (skipped)
    - Line 2: Header line with semicolon-separated groups, comma-separated within groups
    - Lines 3+: Data rows matching the header structure
    
    Args:
        file_path: Path to the text file to parse
        
    Returns:
        DataFrame with parsed data, DateTime column as datetime type
        
    Raises:
        DataParsingError: If file cannot be parsed correctly
        FileNotFoundError: If file does not exist
        
    Example:
        >>> df = parse_txt_microclimate_file("microclimate_station_data.txt")
        >>> print(df.columns)
        Index(['DateTime', 'Temp1', 'Humi1', ...])
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise PVIGRFileNotFoundError(f"File not found: {file_path}")
    
    try:
        with open(file_path, 'r', encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        raise DataParsingError(f"Failed to read file {file_path}: {e}") from e
    
    # Skip BoardID line, get headers from second line
    headers_line = lines[1].strip().rstrip(';')
    header_groups = [h.strip() for h in headers_line.split(';') if h.strip()]
    
    # Expand headers (e.g., "Temp1,Humi1" becomes ["Temp1", "Humi1"])
    column_names = []
    for header in header_groups:
        column_names.extend([col.strip() for col in header.split(',')])
    
    # Parse data lines
    data_rows = []
    for line in lines[2:]:
        line = line.strip().rstrip(';')
        if not line:
            continue
        
        # Split by semicolon first
        groups = line.split(';')
        row_values = []
        
        for group in groups:
            # Split by comma for multi-value groups
            values = group.split(',')
            row_values.extend(values)
        
        if len(row_values) == len(column_names):
            data_rows.append(row_values)
    
    # Create DataFrame
    df = pd.DataFrame(data_rows, columns=column_names)
    
    # Convert DateTime column
    df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')
    
    # Convert numeric columns
    for col in df.columns:
        if col != 'DateTime':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def merge_microclimate_excel_data(microclimate_raw_dir, output_dir):
    """
    Merge microclimate data from Time*.xlsx files for standard microclimate stations.
    
    Args:
        microclimate_raw_dir: Path to raw microclimate data directory
        output_dir: Path to output directory for merged microclimate data
    """
    microclimate_raw_dir = Path(microclimate_raw_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    time_files = sorted(microclimate_raw_dir.glob('Time*.xlsx'))
    if not time_files:
        logger.warning("No Time*.xlsx files found in %s", microclimate_raw_dir)
        return
    
    device_sheets = pd.ExcelFile(time_files[0]).sheet_names
    logger.info("Found %d time files and %d devices", len(time_files), len(device_sheets))
    
    for device in device_sheets:
        data_frames = []
        
        for file in time_files:
            try:
                df = pd.read_excel(file, sheet_name=device)
                df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')
                data_frames.append(df)
            except:
                continue
        
        if data_frames:
            merged = pd.concat(data_frames, ignore_index=True)
            merged = merged.sort_values('DateTime').reset_index(drop=True)
            merged = merged.drop_duplicates(subset='DateTime', keep='first')
            
            output_file = output_dir / f'{device}.csv'
            merged.to_csv(output_file, index=False)
            logger.info("Merged %s: %d records", device, len(merged))


def merge_microclimate_txt_data(txt_dir, output_dir, output_name, file_pattern='*.txt'):
    """
    Merge microclimate data from txt files in a specified directory.
    
    Args:
        txt_dir: Path to directory containing txt files
        output_dir: Path to output directory for merged microclimate data
        output_name: Name for the output CSV file (without extension)
        file_pattern: Glob pattern for txt files (default: '*.txt')
    """
    txt_dir = Path(txt_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    if not txt_dir.exists():
        logger.warning("Directory not found: %s", txt_dir)
        return
    
    txt_files = sorted(txt_dir.glob(file_pattern))
    if not txt_files:
        logger.warning("No files found matching %s in %s", file_pattern, txt_dir)
        return
    
    logger.info("Found %d files in %s", len(txt_files), txt_dir.name)
    data_frames = []
    
    for file in txt_files:
        try:
            df = parse_txt_microclimate_file(file)
            data_frames.append(df)
        except Exception as e:
            logger.error("Error reading %s: %s", file.name, e)
            continue
    
    if data_frames:
        merged = pd.concat(data_frames, ignore_index=True)
        merged = merged.sort_values('DateTime').reset_index(drop=True)
        merged = merged.drop_duplicates(subset='DateTime', keep='first')
        
        output_file = output_dir / f'{output_name}.csv'
        merged.to_csv(output_file, index=False)
        logger.info("Merged %s: %d records", output_name, len(merged))


def merge_power_data(power_raw_dir, output_dir):
    """
    Merge power data from multiple CSV files with comprehensive error handling.
    
    Args:
        power_raw_dir: Path to raw power data directory
        output_dir: Path to output directory for merged power data
        
    Raises:
        ValidationError: If input paths are invalid
        DataProcessingError: If merging fails
    """
    # Validate inputs
    power_raw_dir = InputValidator.validate_directory_path(
        power_raw_dir, 
        must_exist=True
    )
    output_dir = InputValidator.validate_directory_path(
        output_dir, 
        must_exist=False, 
        create_if_missing=True
    )
    
    # Get all power CSV files
    power_files = sorted(power_raw_dir.glob('Chart PV_PVIGR_Data*.csv'))
    
    if not power_files:
        raise ValidationError(
            f"No power data files found in {power_raw_dir}. "
            f"Expected files matching pattern: 'Chart PV_PVIGR_Data*.csv'"
        )
    
    logger.info(f"Found {len(power_files)} power files to merge")
    
    data_frames = []
    failed_files = []
    
    for file in tqdm(power_files, desc="Merging power files", unit="file"):
        try:
            df = pd.read_csv(file)
            
            if df.empty:
                logger.warning(f"File {file.name} is empty, skipping")
                failed_files.append((file.name, "Empty file"))
                continue
            
            # Detect datetime column (could be 'DateTime', 'Time', or first column)
            datetime_col = None
            for col in ['DateTime', 'Time', 'Timestamp', df.columns[0]]:
                if col in df.columns:
                    datetime_col = col
                    break
            
            if not datetime_col:
                logger.error(f"No datetime column found in {file.name}")
                failed_files.append((file.name, "No datetime column"))
                continue
            
            # Convert datetime with error handling
            try:
                df['DateTime'] = pd.to_datetime(df[datetime_col], errors='coerce')
                invalid_dates = df['DateTime'].isna().sum()
                if invalid_dates > 0:
                    logger.warning(
                        f"File {file.name}: {invalid_dates} invalid datetime values"
                    )
                    # Remove rows with invalid dates
                    df = df[df['DateTime'].notna()]
                
                if datetime_col != 'DateTime':
                    df = df.drop(columns=[datetime_col])
                
                data_frames.append(df)
                
            except Exception as e:
                logger.error(f"Failed to parse datetime in {file.name}: {e}")
                failed_files.append((file.name, f"Datetime parsing error: {e}"))
                continue
                
        except Exception as e:
            logger.error(f"Error reading {file.name}: {e}")
            failed_files.append((file.name, str(e)))
            continue
    
    # Check if we have any data
    if not data_frames:
        error_msg = "No valid power data files could be processed."
        if failed_files:
            error_msg += f"\nFailed files ({len(failed_files)}):\n"
            for fname, reason in failed_files[:5]:  # Show first 5
                error_msg += f"  - {fname}: {reason}\n"
        raise DataProcessingError(error_msg)
    
    # Log summary of failures
    if failed_files:
        logger.warning(
            f"Successfully processed {len(data_frames)}/{len(power_files)} files. "
            f"{len(failed_files)} files failed."
        )
    
    # Combine and sort
    try:
        logger.info("Combining data frames...")
        merged = pd.concat(data_frames, ignore_index=True)
        merged = merged.sort_values('DateTime').reset_index(drop=True)
        
        # Check for and remove duplicates
        merged, duplicate_count = DataIntegrityChecker.check_duplicates(
            merged, 
            subset='DateTime', 
            raise_on_error=False
        )
        
        if duplicate_count > 0:
            logger.info(f"Removed {duplicate_count} duplicate timestamps")
        
        # Validate temporal consistency
        temporal_info = DataIntegrityChecker.check_temporal_consistency(
            merged,
            'DateTime',
            raise_on_error=False
        )
        
        # Save to CSV
        output_file = output_dir / 'power_merged.csv'
        logger.info(f"Saving merged data to {output_file}...")
        merged.to_csv(output_file, index=False)
        
        logger.info(f"Power data merged successfully:")
        logger.info(f"  Total records: {len(merged):,}")
        logger.info(f"  Date range: {merged['DateTime'].min()} to {merged['DateTime'].max()}")
        logger.info(f"  Output file: {output_file}")
        
    except Exception as e:
        raise DataProcessingError(
            f"Failed to merge power data: {e}"
        ) from e
    
    logger.info("Power data merge complete")


def merge_weather_data(weather_dir, output_dir):
    """
    Merge weather station CSV files into a unified weather dataset.
    
    Handles multiple weather parameters from HKUST Automated Weather Station (A_USTAWS).
    Combines Date and Time columns into unified DateTime index.
    
    Args:
        weather_dir: Path to raw weather data directory
        output_dir: Path to output directory for merged weather data
        
    Raises:
        ValidationError: If input paths are invalid
        DataProcessingError: If merging fails
    """
    # Validate inputs
    weather_dir = InputValidator.validate_directory_path(
        weather_dir,
        must_exist=True
    )
    output_dir = InputValidator.validate_directory_path(
        output_dir,
        must_exist=False,
        create_if_missing=True
    )
    
    # Define expected weather files with their target column names
    weather_files_config = {
        'Air_temperature.csv': ('Degree Celsius', 'Air_Temp_C'),
        'GHI.csv': ('w/m2', 'GHI_Wm2'),
        'Pressure.csv': ('Pascal', 'Pressure_Pa'),
        'Relative Humidity.csv': ('%', 'RelativeHumidity_pct'),
        'Visibility.csv': ('m', 'Visibility_m'),
        'Wind Velocity.csv': (('m/s', 'Degree'), ('WindSpeed_ms', 'WindDirection_deg'))
    }
    
    logger.info("Merging weather station data...")
    merged_data = None
    files_processed = []
    
    for filename, column_info in weather_files_config.items():
        file_path = weather_dir / filename
        
        if not file_path.exists():
            logger.warning(f"Weather file not found: {filename}, skipping")
            continue
        
        try:
            df = pd.read_csv(file_path)
            
            # Combine Date and Time columns into DateTime
            if 'Date' in df.columns and 'Time' in df.columns:
                df['DateTime'] = pd.to_datetime(
                    df['Date'] + ' ' + df['Time'],
                    format='%Y/%m/%d %H:%M:%S',
                    errors='coerce'
                )
            else:
                logger.error(f"Missing Date/Time columns in {filename}")
                continue
            
            # Handle Wind Velocity special case (has both speed and direction)
            if filename == 'Wind Velocity.csv':
                old_cols, new_cols = column_info
                df_subset = df[['DateTime', old_cols[0], old_cols[1]]].copy()
                df_subset.columns = ['DateTime', new_cols[0], new_cols[1]]
            else:
                # Standard case: single value column
                old_col, new_col = column_info
                if old_col not in df.columns:
                    logger.error(f"Expected column '{old_col}' not found in {filename}")
                    continue
                df_subset = df[['DateTime', old_col]].copy()
                df_subset.columns = ['DateTime', new_col]
            
            # Remove invalid datetime rows
            invalid_count = df_subset['DateTime'].isna().sum()
            if invalid_count > 0:
                logger.warning(f"{filename}: {invalid_count} invalid datetime values removed")
                df_subset = df_subset[df_subset['DateTime'].notna()]
            
            # Merge with existing data
            if merged_data is None:
                merged_data = df_subset
            else:
                merged_data = pd.merge(
                    merged_data,
                    df_subset,
                    on='DateTime',
                    how='outer'
                )
            
            files_processed.append(filename)
            logger.info(f"Processed {filename}: {len(df_subset)} records")
            
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            continue
    
    if merged_data is None or merged_data.empty:
        raise DataProcessingError("No weather data could be merged")
    
    # Sort by datetime and remove duplicates
    merged_data = merged_data.sort_values('DateTime').reset_index(drop=True)
    merged_data, duplicate_count = DataIntegrityChecker.check_duplicates(
        merged_data,
        subset='DateTime',
        raise_on_error=False
    )
    
    if duplicate_count > 0:
        logger.info(f"Removed {duplicate_count} duplicate timestamps")
    
    # Save merged weather data
    output_file = output_dir / 'weather_merged.csv'
    merged_data.to_csv(output_file, index=False)
    
    logger.info(f"Weather data merged successfully:")
    logger.info(f"  Files processed: {len(files_processed)}/{len(weather_files_config)}")
    logger.info(f"  Total records: {len(merged_data):,}")
    logger.info(f"  Date range: {merged_data['DateTime'].min()} to {merged_data['DateTime'].max()}")
    logger.info(f"  Output file: {output_file}")


def display_summary(merged_dir):
    """
    Display summary of merged data.
    
    Args:
        merged_dir: Path to merged data directory
    """
    merged_dir = Path(merged_dir)
    
    logger.info("="*60)
    logger.info("MERGED DATA SUMMARY")
    logger.info("="*60)
    
    # Microclimate data summary
    microclimate_dir = merged_dir / 'microclimate'
    if microclimate_dir.exists():
        microclimate_files = list(microclimate_dir.glob('*.csv'))
        logger.info("Microclimate devices: %d", len(microclimate_files))
        for file in sorted(microclimate_files):
            df = pd.read_csv(file, nrows=1)
            df_full = pd.read_csv(file)
            logger.info("  %s: %d records, %d columns", file.stem, len(df_full), len(df.columns))
    
    # Weather data summary
    weather_file = merged_dir / 'weather' / 'weather_merged.csv'
    if weather_file.exists():
        df = pd.read_csv(weather_file)
        logger.info("Weather station data: %d records, %d columns", len(df), len(df.columns))
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        logger.info("  Range: %s to %s", df['DateTime'].min(), df['DateTime'].max())
    
    # Power data summary
    power_file = merged_dir / 'power' / 'power_merged.csv'
    if power_file.exists():
        df = pd.read_csv(power_file)
        logger.info("Power data: %d records, %d columns", len(df), len(df.columns))
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        logger.info("  Range: %s to %s", df['DateTime'].min(), df['DateTime'].max())
    
    logger.info("="*60)
