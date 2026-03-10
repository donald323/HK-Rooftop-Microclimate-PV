"""
Extract data for specific date ranges with column selection and renaming.
"""

import pandas as pd
import json
from pathlib import Path
from tqdm import tqdm

try:
    from .logging_config import get_logger
except ImportError:
    from logging_config import get_logger

logger = get_logger(__name__)


class DataExtractor:
    """
    Data extraction with column selection, renaming, and task-based processing.
    """
    
    def __init__(self, task_config=None, config_file='../config/extraction_config.json'):
        """
        Initialize the extractor with task configuration.
        
        Args:
            task_config: Dict containing task configuration, task name string, or None for all tasks
            config_file: Path to extraction config file
        """
        self.config_file = Path(__file__).parent / config_file
        self.full_config = self._load_full_config()
        
        # Use pipeline output directory if running from main pipeline
        import os
        if 'PIPELINE_OUTPUT_ROOT' in os.environ:
            OUTPUT_ROOT = Path(os.environ['PIPELINE_OUTPUT_ROOT'])
            self.qc_dir = OUTPUT_ROOT / 'quality_controlled'
            self.downsampled_dir = OUTPUT_ROOT / 'downsampled'
            self.interpolated_dir = OUTPUT_ROOT / 'interpolated'
            self.output_dir = OUTPUT_ROOT / 'extracted'
        else:
            # Load global settings from config
            self.qc_dir = Path(self.full_config.get('qc_directory', '../data/quality_controlled'))
            self.downsampled_dir = Path(self.full_config.get('input_directory', '../data/downsampled'))
            self.interpolated_dir = Path(self.full_config.get('interpolated_directory', '../data/interpolated'))
            self.output_dir = Path(self.full_config.get('output_directory', '../data/extracted'))
        
        # Maintain backward compatibility
        self.input_dir = self.downsampled_dir
        
        # Load date range settings
        date_config = self.full_config.get('settings', {}).get('date_range', {})
        self.start_date = date_config.get('start_date')
        self.end_date = date_config.get('end_date')
        
        # Load task-specific config
        if task_config is None:
            # Load all tasks except metadata
            self.config = {k: v for k, v in self.full_config.items() 
                          if k not in ['description', 'input_directory', 'interpolated_directory', 
                                       'output_directory', 'settings']}
        elif isinstance(task_config, dict):
            self.config = task_config
        elif isinstance(task_config, str):
            self.config = self._load_task_from_config(task_config)
        else:
            raise ValueError("task_config must be a dict, task name string, or None")
    
    def _load_full_config(self):
        """Load the complete configuration file."""
        with open(self.config_file, 'r') as f:
            return json.load(f)
    
    def _load_task_from_config(self, task_name):
        """Load a specific task from the config file."""
        if task_name not in self.full_config:
            available = [k for k in self.full_config.keys() 
                        if k not in ['description', 'input_directory', 'interpolated_directory', 
                                    'output_directory', 'settings']]
            raise ValueError(f"Task '{task_name}' not found. Available: {available}")
        return {task_name: self.full_config[task_name]}
    
    def _check_interpolation_enabled(self):
        """Check if interpolation is enabled by reading interpolation config."""
        try:
            interp_config_path = self.config_file.parent / 'interpolation_config.json'
            with open(interp_config_path, 'r') as f:
                interp_config = json.load(f)
            return interp_config.get('enabled', False)
        except FileNotFoundError:
            logger.warning("Interpolation config not found, assuming disabled")
            return False
    
    def _check_downsampling_enabled(self):
        """Check if downsampling is enabled by reading downsampling config."""
        try:
            downsample_config_path = self.config_file.parent / 'downsampling_config.json'
            with open(downsample_config_path, 'r') as f:
                downsample_config = json.load(f)
            return downsample_config.get('enabled', True)
        except FileNotFoundError:
            logger.warning("Downsampling config not found, assuming enabled")
            return True
    
    def get_input_directory(self):
        """Get the appropriate input directory with cascading fallback: interpolated → downsampled → quality_controlled."""
        # Priority 1: Check if interpolation is enabled and the directory exists
        if self._check_interpolation_enabled() and self.interpolated_dir.exists():
            logger.info("Using interpolated data (contains synthetic values)")
            return self.interpolated_dir
        
        # Priority 2: Check if downsampling is enabled and the directory exists
        if self._check_downsampling_enabled() and self.downsampled_dir.exists():
            if self._check_interpolation_enabled():
                logger.warning(f"Interpolation enabled but directory not found: {self.interpolated_dir}")
                logger.info("Falling back to downsampled data (measured values only)")
            else:
                logger.info("Using downsampled data (measured values only)")
            return self.downsampled_dir
        
        # Priority 3: Fall back to quality_controlled data
        if self.qc_dir.exists():
            if self._check_downsampling_enabled():
                logger.warning(f"Downsampling enabled but directory not found: {self.downsampled_dir}")
            logger.info("Falling back to quality_controlled data (no downsampling)")
            return self.qc_dir
        
        # If nothing exists, return the expected downsampled_dir and let downstream handle the error
        logger.error("No valid input directory found. Expected at least quality_controlled directory.")
        return self.downsampled_dir
    
    def extract_file(self, input_file, output_file, start_date=None, end_date=None, 
                     column_mapping=None, merge_from=None):
        """
        Extract data from a single file with column selection and optional merging.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            start_date: Start date (string in 'YYYY-MM-DD' format)
            end_date: End date (string in 'YYYY-MM-DD' format)
            column_mapping: Dict mapping old column names to new names, or "all" for all columns
            merge_from: Dict with 'input' file and 'columns' to merge from another file
            
        Returns:
            DataFrame or None if file not found
        """
        input_file = Path(input_file)
        output_file = Path(output_file)
        
        if not input_file.exists():
            return None
        
        # Read and filter by date
        df = pd.read_csv(input_file)
        df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')
        mask = (df['DateTime'] >= start_date) & (df['DateTime'] <= end_date)
        df = df[mask].reset_index(drop=True)
        
        # Column selection and renaming
        if column_mapping == "all":
            result_df = df
        elif column_mapping:
            old_cols = list(column_mapping.keys())
            existing_cols = [col for col in old_cols if col in df.columns]
            if not existing_cols:
                return None
            
            result_df = df[existing_cols].copy()
            result_df = result_df.rename(columns=column_mapping)
        else:
            result_df = df
        
        # Merge from another file if specified (simplified for pre-processed data)
        if merge_from:
            # Handle both single merge and multiple merges
            merge_sources = merge_from if isinstance(merge_from, list) else [merge_from]
            
            for merge_source in merge_sources:
                merge_file = input_file.parent / Path(merge_source['input']).name
                if merge_file.exists():
                    merge_df = pd.read_csv(merge_file)
                    merge_df['DateTime'] = pd.to_datetime(merge_df['DateTime'], errors='coerce')
                    
                    # Apply date filtering to merge data if specified
                    if start_date and end_date:
                        mask = (merge_df['DateTime'] >= start_date) & (merge_df['DateTime'] <= end_date)
                        merge_df = merge_df[mask].reset_index(drop=True)
                    
                    merge_cols = merge_source['columns']
                    existing_merge_cols = [col for col in merge_cols.keys() if col in merge_df.columns]
                    if existing_merge_cols:
                        merge_selected = merge_df[['DateTime'] + existing_merge_cols].copy()
                        merge_selected = merge_selected.rename(columns=merge_cols)
                        result_df = pd.merge(result_df, merge_selected, on='DateTime', how='inner')
        
        # Save and return stats
        output_file.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_file, index=False)
        return result_df
    
    def extract(self, downsampled_dir=None, output_dir=None, start_date=None, end_date=None):
        """
        Extract and organize data from downsampled, quality-controlled files.
        
        Args:
            downsampled_dir: Base directory for input files (defaults to config setting)
            output_dir: Base directory for output files (defaults to config setting)
            start_date: Start date for extraction (defaults to config setting)
            end_date: End date for extraction (defaults to config setting)
            
        Returns:
            List of extracted DataFrames
        """
        # Use config defaults if not provided
        if downsampled_dir is None:
            downsampled_dir = self.get_input_directory()
        if output_dir is None:
            output_dir = self.output_dir
        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date
        
        downsampled_dir = Path(downsampled_dir)
        output_dir = Path(output_dir)
        results = []
        
        for task_name, task_config in self.config.items():
            
            for file_config in task_config['files']:
                input_path = downsampled_dir / file_config['input']
                output_path = output_dir / file_config['output']
                
                # Use file-specific date range if provided, otherwise use global
                file_start = file_config.get('date_range', {}).get('start_date', start_date)
                file_end = file_config.get('date_range', {}).get('end_date', end_date)
                
                df = self.extract_file(
                    input_file=input_path,
                    output_file=output_path,
                    start_date=file_start,
                    end_date=file_end,
                    column_mapping=file_config.get('columns'),
                    merge_from=file_config.get('merge_from')
                )
                if df is not None:
                    results.append({
                        'task': task_name,
                        'records': len(df),
                        'output_file': output_path,
                        'dataframe': df
                    })
        
        return results
    
    def get_task_name(self):
        """Get the name of the configured task."""
        return list(self.config.keys())[0] if self.config else None


# Utility function
def list_available_tasks(config_file='../config/extraction_config.json'):
    """List all available tasks in the configuration file."""
    config_path = Path(__file__).parent / config_file
    with open(config_path, 'r') as f:
        config = json.load(f)
    return list(config.keys())


def split_files_by_date(input_directory, split_date, file_pattern='*.csv', 
                        datetime_column='DateTime', part1_suffix='_part1', 
                        part2_suffix='_part2', output_directory=None, 
                        recursive=True, remove_original=False):
    """
    Split CSV files into two time periods at a specific date.
    
    Generalized utility function for temporal file splitting after extraction.
    Useful for creating separate datasets for different analysis periods.
    
    Args:
        input_directory: Directory containing files to split
        split_date: Date string in 'YYYY-MM-DD' format or datetime object
        file_pattern: Glob pattern to match files (default: '*.csv')
        datetime_column: Name of datetime column (default: 'DateTime')
        part1_suffix: Suffix for files before split_date (default: '_part1')
        part2_suffix: Suffix for files from split_date onwards (default: '_part2')
        output_directory: Output directory (defaults to same as input)
        recursive: Search subdirectories recursively (default: True)
        remove_original: Remove original files after splitting (default: False)
        
    Returns:
        List of dicts with split file information including:
            - original_file: Path to original file
            - part1_file: Path to first period file (None if empty)
            - part2_file: Path to second period file (None if empty)
            - part1_records: Number of records before split_date
            - part2_records: Number of records from split_date onwards
            - split_date: Split date used
    """
    input_directory = Path(input_directory)
    split_date = pd.to_datetime(split_date)
    output_directory = Path(output_directory) if output_directory else None
    results = []
    
    # Find all matching files
    if recursive:
        matched_files = list(input_directory.rglob(file_pattern))
    else:
        matched_files = list(input_directory.glob(file_pattern))
    
    if not matched_files:
        logger.warning(f"No files found matching pattern '{file_pattern}' in {input_directory}")
        return results
    
    logger.info(f"Found {len(matched_files)} files to split at {split_date.date()}")
    
    for file_path in tqdm(matched_files, desc="Splitting files"):
        try:
            # Read data
            df = pd.read_csv(file_path)
            
            if datetime_column not in df.columns:
                logger.warning(f"Column '{datetime_column}' not found in {file_path.name}, skipping")
                continue
            
            df[datetime_column] = pd.to_datetime(df[datetime_column], errors='coerce')
            
            # Split into two periods
            df_part1 = df[df[datetime_column] < split_date].reset_index(drop=True)
            df_part2 = df[df[datetime_column] >= split_date].reset_index(drop=True)
            
            # Determine output location
            if output_directory:
                output_parent = output_directory
                output_parent.mkdir(parents=True, exist_ok=True)
            else:
                output_parent = file_path.parent
            
            # Generate output file names
            stem = file_path.stem
            part1_name = f"{stem}_{part1_suffix}.csv"
            part2_name = f"{stem}_{part2_suffix}.csv"
            
            part1_path = output_parent / part1_name
            part2_path = output_parent / part2_name
            
            # Save split files
            part1_file = None
            part2_file = None
            
            if not df_part1.empty:
                df_part1.to_csv(part1_path, index=False)
                part1_file = part1_path
            
            if not df_part2.empty:
                df_part2.to_csv(part2_path, index=False)
                part2_file = part2_path
            
            # Remove original file if requested
            if remove_original and (part1_file or part2_file):
                file_path.unlink()
            
            results.append({
                'original_file': file_path,
                'part1_file': part1_file,
                'part2_file': part2_file,
                'part1_records': len(df_part1),
                'part2_records': len(df_part2),
                'split_date': split_date
            })
            
        except Exception as e:
            logger.error(f"Failed to split {file_path.name}: {e}")
            continue
    
    logger.info(f"Successfully split {len(results)} files into two time periods")
    return results


def split_columns_by_date(file_path, split_date, column_patterns=None, 
                         datetime_column='DateTime', part1_suffix='_part1', 
                         part2_suffix='_part2', keep_original=False, output_file=None):
    """
    Split specific columns into two time periods within the same file.
    
    Creates two versions of each matching column: one with values before split_date
    (NaN after), and one with values from split_date onwards (NaN before).
    Useful for temporal analysis when separate files are not needed.
    
    Args:
        file_path: Path to CSV file to modify
        split_date: Date string in 'YYYY-MM-DD' format or datetime object
        column_patterns: List of patterns to match column names (default: ['WedTri', 'WT'])
        datetime_column: Name of datetime column (default: 'DateTime')
        part1_suffix: Suffix for columns before split_date (default: '_part1')
        part2_suffix: Suffix for columns from split_date onwards (default: '_part2')
        keep_original: Keep original columns (default: False)
        output_file: Path to save modified file (defaults to overwriting input)
        
    Returns:
        Dict with split information including:
            - file: Path to output file
            - split_columns: List of column names that were split
            - total_records: Total number of records
            - part1_records: Number of records before split_date
            - part2_records: Number of records from split_date onwards
            - split_date: Split date used
    """
    file_path = Path(file_path)
    split_date = pd.to_datetime(split_date)
    output_file = Path(output_file) if output_file else file_path
    
    if column_patterns is None:
        column_patterns = ['WedTri', 'WT']
    
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return None
    
    try:
        # Read data
        df = pd.read_csv(file_path)
        
        if datetime_column not in df.columns:
            logger.error(f"Column '{datetime_column}' not found in {file_path.name}")
            return None
        
        df[datetime_column] = pd.to_datetime(df[datetime_column], errors='coerce')
        
        # Find matching columns
        matching_cols = []
        for col in df.columns:
            if col == datetime_column:
                continue
            for pattern in column_patterns:
                if pattern in col:
                    matching_cols.append(col)
                    break
        
        if not matching_cols:
            logger.warning(f"No columns matching patterns {column_patterns} in {file_path.name}")
            return None
        
        logger.info(f"Splitting {len(matching_cols)} columns in {file_path.name} at {split_date.date()}")
        
        # Create split columns
        for col in matching_cols:
            # Part 1: values before split_date, NaN after
            part1_col = f"{col}{part1_suffix}"
            df[part1_col] = df[col].where(df[datetime_column] < split_date)
            
            # Part 2: values from split_date onwards, NaN before
            part2_col = f"{col}{part2_suffix}"
            df[part2_col] = df[col].where(df[datetime_column] >= split_date)
        
        # Remove original columns if requested
        if not keep_original:
            df = df.drop(columns=matching_cols)
        
        # Calculate record counts
        part1_count = (df[datetime_column] < split_date).sum()
        part2_count = (df[datetime_column] >= split_date).sum()
        
        # Save modified file
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False)
        logger.info(f"Saved modified file: {output_file}")
        
        return {
            'file': output_file,
            'split_columns': matching_cols,
            'total_records': len(df),
            'part1_records': part1_count,
            'part2_records': part2_count,
            'split_date': split_date
        }
        
    except Exception as e:
        logger.error(f"Failed to split columns in {file_path.name}: {e}")
        return None

