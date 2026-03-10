# High temporal-resolution microclimate and photovoltaic power dataset for sustainable rooftop design in subtropical humid city

This repository is dedicated for the files related to the pipeline of processing the microclimate and PV power generation data for different roof designs, and weather data at The Hong Kong University of Science and Technology.

## Overview

The pipeline is divided into modularized stages that can be run individually. The default order of the stages are as follows: 

**Data Processing:** Merge → Quality Control → Downsample (Optional) → Interpolate (Optional) → Extract  
**Data Analysis:** Missing Rate Analysis

![Data Curation Workflow](Data%20Curation.jpg)

## Data Sources

The pipeline integrates three primary data sources:

### 1. Microclimate Data
High-frequency on-site sensor measurements from PVIGR experimental stations capturing local environmental conditions beneath and around PV panels. Includes air temperature, relative humidity, wind speed/direction, solar irradiance, and soil moisture at multiple heights and locations.

**Characteristics:**
- **Temporal Resolution:** Variable (typically 1-5 minute intervals)
- **Data Format:** Excel (.xlsx) and text (.txt) files from multiple sensor stations

### 2. PV Power Data
Solar panel power generation measurements from multiple PV installations with various orientations, tilt angles, and vegetation types (PVIGR systems).

**Characteristics:**
- **Temporal Resolution:** Variable (typically minute-level)
- **Data Format:** CSV files from SolarEdge monitoring system

### 3. Weather Station Data
Hourly meteorological observations from the HKUST Automated Weather Station providing broader atmospheric context and regional weather conditions.

**Characteristics:**
- **Temporal Resolution:** Hourly
- **Data Format:** CSV files with Date/Time columns
- **Source:** HKUST Automated Weather Station
- **Variables:**
  - **Air_Temp_C**: Air temperature (°C)
  - **GHI_Wm2**: Global Horizontal Irradiance (W/m²)
  - **Precipitation_mm**: Hourly precipitation (mm)
  - **Pressure_Pa**: Atmospheric pressure (Pascal)
  - **RH_pct**: Relative humidity (%)
  - **Visibility_m**: Visibility distance (meters)
  - **WS_ms**: Wind speed (m/s)
  - **WDir_deg**: Wind direction (degrees)

## Installation

```bash
pip install -r requirements.txt
```

## Usage

Run the complete pipeline with raw data downloaded from [here](xxx) and extracted to the root directory of this repository:

```bash
jupyter notebook notebooks/00_main_pipeline.ipynb
```

## Configuration

### Core Pipeline Configuration

**`config/downsampling_config.json`** - Temporal resolution settings
- `enabled`: Toggle downsampling stage (true/false)
- `frequency`: Target temporal resolution (e.g., "1h", "30min", "1D")
- `input_directory`: Path to quality-controlled data
- `output_directory`: Path for downsampled output

**`config/interpolation_config.json`** - Gap-filling parameters
- `enabled`: Toggle interpolation stage (true/false)
- `data_types`: Per-variable interpolation settings
  - `method`: Interpolation method (linear, cubic, nearest, etc.)
  - `max_gap`: Maximum gap size to fill (in time units)
  - `applies_to`: Column name patterns to match

**`config/extraction_config.json`** - Output dataset definitions
- `date_range`: Start and end dates for extraction
- `data_types`: Datasets to extract (power, microclimate, weather)
- `columns`: Specific columns to include in each dataset
- `output_options`: File naming and format settings

### Quality Control Configuration

**`config/qc_boundaries.json`** - Physical range limits
- Variable-specific upper and lower bounds
- Applies to: temperature, humidity, wind speed, solar radiation, pressure, PV power
- Time-dependent boundaries for PV power (day/night)

**`config/constant_value_detection.json`** - Detection for exact same values in all timestamps within a window
- `enabled`: Toggle constant value detection (true/false)
- `window_days`: Number of consecutive days to trigger detection
- `tolerance`: Standard deviation threshold for constant values
- `applies_to`: Variable categories (temperature, humidity, wind, power, radiation)

**`config/sensor_failures.json`** - Known failure periods
- Documented sensor malfunction periods
- Automatically sets affected data to NaN
- Format: `{"file": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD", "sensors": ["pattern"]}}`

**Note:** Quality control processing order is: 1) Constant value detection → 2) Sensor failure periods → 3) Boundary checks

### Microclimate Column Naming Convention

**Microclimate Data Column Format:**
```
[Variable]_[Height]_[Context]
```

**Variable Types:**
- **`Ta`** - Air temperature (°C)
- **`RH`** - Relative humidity (%)
- **`Ws`** - Wind speed (m/s)
- **`Wdir`** - Wind direction (degrees)
- **`Rad`** - Solar irradiance (W/m²)
- **`SM`** - Soil moisture (%)

**Height Notation:**
- **`###cm`** - Sensor height above ground (e.g., 050cm, 180cm, 200cm)

**Context:**
- **`PVHgt_###cm`** - Separation height of the target PV module from the ground
  - Example: `Ta_180cm_PVHgt_090cm` - Air temperperature at 180cm height above ground for the PV module with separation height of 90 cm

**Examples:**
- `Ta_180cm` - Air temperature at 180cm height
- `RH_050cm` - Relative humidity at 50cm height
- `Ws_200cm` - Wind speed at 200cm height (standard meteorological height)
- `Ta_050cm_PVHgt_060cm` - Air temperature at 50cm under a PV module with separation height from the ground of 60cm
- `SM_Sedum_PVHgt_060cm` - Soil moisture beneath Sedum vegetation under the PV module with separation height from the ground of 60cm
- `Rad_22deg` - Solar irradiance at 22° tilt angle

### Power Column Naming Convention

**Power Data Column Format:**
```
[Type]_[Location]_PVAz[###]_PVTilt[##]_Hgt[###]_[Notes]
```

**Components:**
- **Type**: System type
  - `PV` - Photovoltaic panels (non-PVIGR)
  - `WT` - Wedelia Trilobata PVIGR
  - `Sed` - Sedum PVIGR
  - `Zoy` - Zoysia PVIGR
- **Location**: Site identifier (PV systems only)
  - `PVR_1` - PV Rooftop site 1
  - `nrPVIGRN` - Near PVIGRN station
  - `nrSed` - Near PVIGR-Sedum
  - `nrZoy` - Near PVIGR-Zoysia
  - `Lift2` - Near Lift 2
- **PVAz[###]**: Panel orientation (azimuth angle in degrees, 0-360°)
- **PVTilt[##]**: Panel tilt angle (degrees from horizontal, 0-90°)
- **Hgt[###]**: Installation height (centimeters above ground)
- **Notes**: Optional descriptors (PV systems only)
  - `SenTree` - With sensor tree
  - `Land` - Landscape layout
  - `Vert` - Vertical layout
  - `Shd` - Relatively more shading

**Examples:**
- `PV_PVR_1_PVAz242_PVTilt10_Hgt096_SenTree` - PV panel at PVR_1 site, 242° orientation, 10° tilt, 96cm height, with sensor tree
- `PV_nrSed_PVAz179_PVTilt22_Hgt092_Land_Shd` - PV panel near Sedum, 179° orientation, 22° tilt, 92cm height, landscape layout with more shading
- `WT_PVAz180_PVTilt22_Hgt060` - Wedelia Trilobata PVIGR, 180° orientation, 22° tilt, 60cm height

### Weather Station Column Naming Convention

**Weather Data Column Format (USTAWS):**
```
[Variable]_[Unit]
```

All weather station data follows a standardized format with variable name followed by unit abbreviation.

**Variables:**
- **`Air_Temp_C`** - Air temperature (°C)
- **`GHI_Wm2`** - Global Horizontal Irradiance (W/m²)
- **`Pressure_Pa`** - Atmospheric pressure (Pascal)
- **`RH_pct`** - Relative humidity (%)
- **`Visibility_m`** - Visibility distance (meters)
- **`WS_ms`** - Wind speed (m/s)
- **`WDir_deg`** - Wind direction (degrees from north, 0-360°)

**Examples:**
- `Air_Temp_C` - 25.7°C
- `GHI_Wm2` - 480.9 W/m²
- `WS_ms` - 2.41 m/s
- `WDir_deg` - 189.8° (south-southwest wind)

## Repository Structure

```
├── config/                             # Configuration files
│   ├── constant_value_detection.json   # Detection parameters for stuck sensors
│   ├── extraction_config.json          # Dataset extraction settings
│   ├── interpolation_config.json       # Gap-filling parameters
│   ├── qc_boundaries.json              # Physical range limits for QC
│   ├── downsampling_config.json        # Temporal resolution settings
│   ├── sensor_failures.json            # Known sensor malfunction periods
├── notebooks/                          # Jupyter notebooks
│   ├── 00_main_pipeline.ipynb          # Complete pipeline orchestrator
│   ├── 01_merge_raw_data.ipynb         # Stage 1: Data merging
│   ├── 02_quality_control.ipynb        # Stage 2: QC validation
│   ├── 03_downsampling.ipynb           # Stage 3: Temporal downsampling
│   ├── 04_interpolation.ipynb          # Stage 4: Gap filling
│   ├── 05_extract_data.ipynb           # Stage 5: Dataset extraction
│   └── 06_missing_rate_analysis.ipynb  # Stage 6: Data completeness analysis
├── raw_data                            # Downloaded and extracted raw data
├── src/                                # Source code modules
│   ├── __init__.py                     # Initialization
│   ├── downsampling.py                 # Temporal downsampling
│   ├── exceptions.py                   # Custom exception classes
│   ├── extract_data.py                 # Dataset extraction
│   ├── interpolation.py                # Gap-filling algorithms
│   ├── logging_config.py               # Logging configuration
│   ├── merge_data.py                   # Time series merging logic
│   ├── missing_rate_analysis.py        # Missing rate calculations
│   ├── notebook_utils.py               # Notebook helper utilities
│   ├── plot_style.py                   # Visualization styling
│   ├── quality_control.py              # QC validation engine
│   └── validation.py                   # Data validatiset extraction
│   ├── plot_style.py                   # Visualization styling
│   └── utils.py                        # Common utilities
```
## Related Publications:
- [To be Confirmed]

## Contact

**Corresponding Authors:** Mengying Li, Zhe Wang  
**Email:** mengying.li@polyu.edu.hk, cezhewang@ust.hk  
**Institution:** The Hong Kong Polytechnic University, The Hong Kong University of Science and Technology  

For bug reports and feature requests, please use the [GitHub Issues](https://github.com/your-org/pvigr-data-processing/issues) page.