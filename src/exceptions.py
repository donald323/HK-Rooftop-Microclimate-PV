"""
Custom exception classes for PVIGR data processing.

This module defines domain-specific exceptions for better error handling
and more informative error messages throughout the pipeline.
"""


class PVIGRError(Exception):
    """Base exception class for all PVIGR data processing errors."""
    pass


class ConfigurationError(PVIGRError):
    """Raised when configuration files are invalid or missing."""
    pass


class DataValidationError(PVIGRError):
    """Raised when data fails validation checks."""
    pass


class ValidationError(PVIGRError):
    """Raised when input validation fails."""
    pass


class DataIntegrityError(PVIGRError):
    """Raised when data integrity checks fail (duplicates, missing data, etc.)."""
    pass


class DataProcessingError(PVIGRError):
    """Raised when general data processing operations fail."""
    pass


class FileNotFoundError(PVIGRError):
    """Raised when required input files are not found."""
    pass


class DataParsingError(PVIGRError):
    """Raised when data cannot be parsed correctly."""
    pass


class QualityControlError(PVIGRError):
    """Raised when quality control operations fail."""
    pass


class DownsamplingError(PVIGRError):
    """Raised when downsampling operations fail."""
    pass


class InterpolationError(PVIGRError):
    """Raised when interpolation operations fail."""
    pass


class ExtractionError(PVIGRError):
    """Raised when data extraction operations fail."""
    pass


class ColumnMappingError(PVIGRError):
    """Raised when column mapping fails (missing columns, etc.)."""
    pass


class DateRangeError(PVIGRError):
    """Raised when date range validation fails."""
    pass
