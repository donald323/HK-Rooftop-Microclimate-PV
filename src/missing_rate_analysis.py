"""
Missing Rate Analysis Module

Provides functions for calculating and visualizing missing data rates
in extracted PVIGR datasets.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Union

try:
    from .plot_style import apply_plot_style
except ImportError:
    from plot_style import apply_plot_style


def abbreviate_sensor_name(name: str) -> str:
    """
    Use abbreviations for common terms in sensor names.
    
    Parameters
    ----------
    name : str
        Sensor name to abbreviate
    
    Returns
    -------
    str
        Abbreviated sensor name
    """
    replacements = {
        '(Changed to Sedum lineare)': 'Changed',
        '(No Change)': 'NoChange',
        'Temperature': 'Temp',
        'Humidity': 'RH',
        'WindSpeed': 'Ws',
        'WindDirection': 'Wdir',
        'Radiation': 'Rad',
        '_200cm': ' SH200',
        '_180cm': ' SH180',
        '_050cm': ' SH050',
        '_090cm': ' Hgt090',
        '_060cm': ' Hgt060',
        '_075cm': ' Hgt075',
        'PVHgt_': '',
        '_PVHgt': ''
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    
    # Normalize multiple consecutive underscores to single underscore
    import re
    name = re.sub(r'_+', '_', name)
    
    # Remove consecutive duplicate parts separated by underscores
    parts = name.split('_')
    deduplicated = [parts[0]] if parts else []
    for i in range(1, len(parts)):
        if parts[i] != parts[i-1]:
            deduplicated.append(parts[i])
    name = '_'.join(deduplicated)
    
    return name


def smart_wrap(name: str, max_length: int = 30) -> str:
    """
    Wrap sensor name at underscores, keeping related parts together.
    
    Parameters
    ----------
    name : str
        Sensor name to wrap
    max_length : int, default=30
        Maximum characters per line before wrapping
    
    Returns
    -------
    str
        Wrapped sensor name with line breaks
    """
    if len(name) <= max_length:
        return name
    
    parts = name.split('_')
    lines = []
    current = []
    current_len = 0
    
    for part in parts:
        # Calculate length including underscore separator
        part_len = len(part) + (1 if current else 0)
        
        if current_len + part_len > max_length and current:
            lines.append('_'.join(current))
            current = [part]
            current_len = len(part)
        else:
            current.append(part)
            current_len += len(part) + (1 if len(current) > 1 else 0)
    
    if current:
        lines.append('_'.join(current))
    
    return '\n'.join(lines)


def load_extracted_data(folder_path: Union[str, Path], data_type: str) -> pd.DataFrame:
    """
    Load extracted data from a specific folder and data type.
    
    Loads all CSV files from the data type directory and combines them.
    
    Parameters
    ----------
    folder_path : str or Path
        Path to the processed data folder (e.g., '20251227_022635_downsample_false_interpolate_false')
    data_type : str
        Type of data to load: 'power', 'microclimate'
    
    Returns
    -------
    pd.DataFrame
        DataFrame with DateTime as index and sensor columns
    
    Raises
    ------
    FileNotFoundError
        If the data file or directory does not exist
    ValueError
        If data_type is not valid
    """
    valid_types = ['power', 'microclimate']
    if data_type not in valid_types:
        raise ValueError(f"data_type must be one of {valid_types}, got '{data_type}'")
    
    folder_path = Path(folder_path)
    data_dir = folder_path / 'extracted' / data_type
    
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    # Get all CSV files in the directory
    csv_files = list(data_dir.glob('*.csv'))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {data_dir}")
    
    # Load and concatenate all CSV files
    dfs = []
    for csv_file in sorted(csv_files):
        try:
            temp_df = pd.read_csv(csv_file, parse_dates=['DateTime'])
            
            # Set DateTime as index if not already
            if 'DateTime' in temp_df.columns:
                temp_df = temp_df.set_index('DateTime')
            
            # Prefix column names with filename (without extension) to avoid conflicts
            file_prefix = csv_file.stem.replace('_yr', '').replace('_1yr', '')
            temp_df.columns = [f"{file_prefix}_{col}" if col != 'DateTime' else col 
                              for col in temp_df.columns]
            dfs.append(temp_df)
        except Exception as e:
            print(f"Warning: Failed to load {csv_file.name}: {e}")
            continue
    
    if not dfs:
        raise ValueError(f"No valid CSV files could be loaded from: {data_dir}")
    
    # Combine all dataframes using concat
    df = pd.concat(dfs, axis=1)
    
    return df


def calculate_monthly_missing_rate(df: pd.DataFrame, time_range: tuple = None) -> pd.DataFrame:
    """
    Calculate the missing data rate for each column by month.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with datetime index and sensor data columns
    time_range : tuple of (int, int), optional
        Hour range to filter data (start_hour, end_hour). 
        For example, (6, 18) filters data between 6:00 and 18:00.
        If None, uses all hours.
    
    Returns
    -------
    pd.DataFrame
        DataFrame with months as rows (YYYY-MM format) and columns as sensors.
        Values represent the missing rate (0.0 = no missing, 1.0 = all missing)
    """
    # Filter by time range if specified
    if time_range is not None:
        start_hour, end_hour = time_range
        df = df[(df.index.hour >= start_hour) & (df.index.hour < end_hour)]
    
    # Create year-month column for grouping
    df_with_month = df.copy()
    df_with_month['year_month'] = df_with_month.index.to_period('M')
    
    # Group by month and calculate missing rate for each column
    missing_rates = []
    
    for period, group in df_with_month.groupby('year_month'):
        # Drop the year_month column before calculating missing rate
        group_data = group.drop(columns=['year_month'])
        
        # Calculate missing rate: number of NaN / total rows
        missing_rate = group_data.isna().sum() / len(group_data)
        missing_rate.name = str(period)
        missing_rates.append(missing_rate)
    
    # Combine into DataFrame with months as rows
    missing_rate_df = pd.DataFrame(missing_rates)
    
    return missing_rate_df


def create_missing_rate_heatmap(missing_rate_df: pd.DataFrame, title: str, 
                                output_path: Union[str, Path]) -> None:
    """
    Create and save a heatmap visualization of missing data rates.
    
    Parameters
    ----------
    missing_rate_df : pd.DataFrame
        DataFrame with months as rows and sensors as columns, 
        values representing missing rates (0.0 to 1.0)
    title : str
        Title for the heatmap
    output_path : str or Path
        Path where the figure will be saved
    
    Returns
    -------
    None
        Saves the figure to the specified path
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    apply_plot_style()
    
    # Transpose the dataframe to switch axes (sensors as rows, months as columns)
    missing_rate_df = missing_rate_df.T
    
    # Apply abbreviations and smart wrapping to sensor names
    processed_index = [smart_wrap(abbreviate_sensor_name(name), max_length=30) 
                      for name in missing_rate_df.index]
    missing_rate_df.index = processed_index
    
    # Calculate dynamic figure size
    n_sensors = len(missing_rate_df)
    n_months = len(missing_rate_df.columns)
    
    # Scale: ~0.4 inches per sensor, ~0.8 inches per month (increased for better spacing)
    fig_height = max(8, n_sensors * 0.4)
    fig_width = max(12, n_months * 0.8)
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    
    # Convert to percentage for display
    missing_rate_pct = missing_rate_df * 100
    
    # Create heatmap without annotations
    sns.heatmap(
        missing_rate_pct,
        annot=False,
        cmap='Reds',
        vmin=0,
        vmax=100,
        cbar_kws={'label': 'Missing Rate (%)'},
        linewidths=0.5,
        linecolor='white',
        ax=ax
    )
    
    # Set labels and title
    ax.set_xlabel('Month', fontweight='bold', fontfamily='Times New Roman')
    ax.set_ylabel('Sensor', fontweight='bold', fontfamily='Times New Roman')
    ax.set_title(title, fontweight='bold', fontsize=24, fontfamily='Times New Roman', pad=20)
    
    # Rotate x-axis labels for readability
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', rotation_mode='anchor', fontfamily='Times New Roman')
    plt.setp(ax.get_yticklabels(), rotation=0, fontfamily='Times New Roman')
    
    # Set colorbar font to Times New Roman
    cbar = ax.collections[0].colorbar
    cbar.ax.set_ylabel('Missing Rate (%)', fontfamily='Times New Roman')
    plt.setp(cbar.ax.get_yticklabels(), fontfamily='Times New Roman')
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save figure
    plt.savefig(output_path)
    plt.close()


def analyze_all_folders(extracted_dir: Union[str, Path], output_dir: Union[str, Path], 
                       max_sensors_per_plot: int = 10) -> dict:
    """
    Analyze missing rates for all data types in an extracted directory.
    
    Parameters
    ----------
    extracted_dir : str or Path
        Path to the extracted data directory containing task subfolders
    output_dir : str or Path
        Directory where heatmap figures will be saved
    max_sensors_per_plot : int, default=10
        Maximum number of sensors to display per heatmap plot
    
    Returns
    -------
    dict
        Dictionary with analysis results including successfully processed files
        and any errors encountered
    """
    extracted_dir = Path(extracted_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = {'successful': [], 'failed': []}
    
    if not extracted_dir.exists():
        print(f"ERROR: Extracted directory not found: {extracted_dir}")
        return results
    
    # Automatically discover all subdirectories in extracted_dir
    data_dirs = sorted([d for d in extracted_dir.iterdir() if d.is_dir()])
    
    if not data_dirs:
        print(f"ERROR: No subdirectories found in {extracted_dir}")
        return results
    
    print(f"Processing data from: {extracted_dir}")
    print(f"Found {len(data_dirs)} data type(s): {', '.join([d.name for d in data_dirs])}\n")
    
    for data_dir in data_dirs:
        data_type = data_dir.name
        
        try:
            print(f"  {data_type}...", end=" ")
            
            # Load all CSV files from this task directory
            csv_files = list(data_dir.glob('*.csv'))
            if not csv_files:
                print("SKIP (no CSV files)")
                continue
            
            # Load and combine ALL data first
            dfs = []
            for csv_file in sorted(csv_files):
                temp_df = pd.read_csv(csv_file, parse_dates=['DateTime'])
                if 'DateTime' in temp_df.columns:
                    temp_df = temp_df.set_index('DateTime')
                
                # Rename columns with file prefix
                file_prefix = csv_file.stem.replace('_yr', '').replace('_1yr', '')
                temp_df.columns = [f"{file_prefix}_{col}" for col in temp_df.columns]
                
                dfs.append(temp_df)
            
            # Combine dataframes - use outer join to handle all timestamps
            df = pd.concat(dfs, axis=1, join='outer').sort_index()
            
            # Group columns by pattern
            file_groups = []
            
            if data_type == 'microclimate':
                # Separate soil moisture from climate data
                soil_moisture_cols = [col for col in df.columns if 'SoilMoisture' in col or 'SM_' in col]
                wt_cols = [col for col in df.columns if '_WT' in col and '_WT-to-Sed' not in col and col not in soil_moisture_cols]
                wt_to_sed_cols = [col for col in df.columns if '_WT-to-Sed' in col and col not in soil_moisture_cols]
                other_cols = [col for col in df.columns if col not in soil_moisture_cols and col not in wt_cols and col not in wt_to_sed_cols]
                
                if wt_cols:
                    file_groups.append(('microclimate_wt', df[wt_cols], 'Microclimate (WT - Before Dec 2024)'))
                if wt_to_sed_cols:
                    file_groups.append(('microclimate_wt_to_sed', df[wt_to_sed_cols], 'Microclimate (WT-to-Sed - After Dec 2024)'))
                if other_cols:
                    file_groups.append(('microclimate_other', df[other_cols], 'Microclimate (Other)'))
                if soil_moisture_cols:
                    file_groups.append(('microclimate_soil_moisture', df[soil_moisture_cols], 'Microclimate (Soil Moisture)'))
            
            elif data_type == 'power':
                # Group by suffix patterns
                wt_cols = [col for col in df.columns if '_WT' in col and '_WT-to-Sed' not in col]
                wt_to_sed_cols = [col for col in df.columns if '_WT-to-Sed' in col]
                other_cols = [col for col in df.columns if col not in wt_cols and col not in wt_to_sed_cols]
                
                if wt_cols:
                    file_groups.append(('power_wt', df[wt_cols], 'Power (WT - Before Dec 2024)'))
                if wt_to_sed_cols:
                    file_groups.append(('power_wt_to_sed', df[wt_to_sed_cols], 'Power (WT-to-Sed - After Dec 2024)'))
                if other_cols:
                    file_groups.append(('power_other', df[other_cols], 'Power (Other)'))
            
            else:
                # For other data types, keep as single group
                file_groups = [(data_type, df, data_type.replace('_', ' ').title())]
            
            # Process each file group
            for group_name, group_df, group_title in file_groups:
                
                # Filter by time range for WT and WT-to-Sed groups
                if 'wt' in group_name and 'wt_to_sed' not in group_name:
                    # WT groups: only show data before Dec 1, 2024
                    group_df = group_df[group_df.index < pd.Timestamp('2024-12-01')]
                elif 'wt_to_sed' in group_name:
                    # WT-to-Sed groups: only show data from Dec 1, 2024 onwards
                    group_df = group_df[group_df.index >= pd.Timestamp('2024-12-01')]
                elif 'soil_moisture' in group_name:
                    # Soil moisture: only show dates where data actually exists
                    # Remove rows where all columns are NaN (outside collection period)
                    group_df = group_df.dropna(how='all')
                
                # Calculate missing rates
                missing_rate_df = calculate_monthly_missing_rate(group_df, time_range=(6, 18))
                
                # Split into multiple plots if needed
                data_type_title = group_title
                sensors = missing_rate_df.columns.tolist()
                total_sensors = len(sensors)
                num_plots = (total_sensors + max_sensors_per_plot - 1) // max_sensors_per_plot
                
                for plot_idx in range(num_plots):
                    start_idx = plot_idx * max_sensors_per_plot
                    end_idx = min(start_idx + max_sensors_per_plot, total_sensors)
                    sensor_subset = sensors[start_idx:end_idx]
                    
                    missing_rate_subset = missing_rate_df[sensor_subset]
                    
                    # Create safe filename by replacing spaces with underscores
                    safe_filename = group_name.replace(' ', '_').lower()
                    
                    if num_plots > 1:
                        title = f"{data_type_title} Missing Rate (Part {plot_idx + 1}/{num_plots})"
                        output_filename = f"{safe_filename}_missing_rate_part{plot_idx + 1}.png"
                    else:
                        title = f"{data_type_title} Missing Rate"
                        output_filename = f"{safe_filename}_missing_rate.png"
                    
                    output_path = output_dir / output_filename
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    create_missing_rate_heatmap(missing_rate_subset, title, output_path)
                    
                    results['successful'].append({
                        'data_type': group_name,
                        'output_path': str(output_path),
                        'part': f"{plot_idx + 1}/{num_plots}" if num_plots > 1 else "1/1"
                    })
            
            print("OK")
            
        except Exception as e:
            print(f"FAIL ({type(e).__name__}: {str(e)})")
            results['failed'].append({
                'data_type': data_type,
                'error': f"{type(e).__name__}: {str(e)}"
            })
    
    return results

def calculate_task_missing_rate(extracted_dir: Union[str, Path], 
                                data_type: str,
                                time_range: tuple = None) -> dict:
    """
    Calculate overall missing rate summary for a single task.
    
    Parameters
    ----------
    extracted_dir : str or Path
        Path to the extracted data directory
    data_type : str
        Type of data: 'power', 'microclimate'
    time_range : tuple of (int, int), optional
        Hour range to filter data (start_hour, end_hour). 
        For example, (6, 18) filters data between 6:00 and 18:00.
        If None, uses all hours.
    
    Returns
    -------
    dict
        Dictionary with task statistics including missing rate
    """
    extracted_dir = Path(extracted_dir)
    data_dir = extracted_dir / data_type
    
    if not data_dir.exists():
        return None
    
    csv_files = list(data_dir.glob('*.csv'))
    if not csv_files:
        return None
    
    total_values = 0
    missing_values = 0
    total_sensors = set()
    total_records = 0
    
    for csv_file in sorted(csv_files):
        temp_df = pd.read_csv(csv_file, parse_dates=['DateTime'])
        if 'DateTime' in temp_df.columns:
            temp_df = temp_df.set_index('DateTime')
        
        if time_range is not None:
            start_hour, end_hour = time_range
            temp_df = temp_df[(temp_df.index.hour >= start_hour) & (temp_df.index.hour < end_hour)]
        
        total_sensors.update(temp_df.columns)
        total_records = max(total_records, len(temp_df))
        total_values += temp_df.size
        missing_values += temp_df.isna().sum().sum()
    
    missing_rate = (missing_values / total_values) * 100 if total_values > 0 else 0
    
    return {
        'Task': data_type.replace('_', ' ').title(),
        'Total Sensors': len(total_sensors),
        'Total Records': total_records,
        'Missing Rate (%)': round(missing_rate, 2),
        'Files': len(csv_files)
    }
