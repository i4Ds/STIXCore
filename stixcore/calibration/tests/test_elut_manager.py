from pathlib import Path
from datetime import datetime

import numpy as np
import pytest
from intervaltree import IntervalTree
from stixpy.calibration.detector import get_sci_channels
from stixpy.io.readers import read_elut

from astropy.table import QTable

from stixcore.calibration.elut_manager import ELUTManager


class TestELUTManagerBasics:
    """Basic integration tests for ELUTManager using real data."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        # Reset singleton instance for each test
        ELUTManager._instances = {}

    def test_initialization_default_path(self):
        """Test ELUTManager initialization with default data path."""
        manager = ELUTManager()

        assert manager.data_root.name == "elut"
        assert manager.data_root.exists()
        assert manager.elut_index_file.exists()
        assert isinstance(manager.elut_index, IntervalTree)
        assert len(manager.elut_index) > 0

    def test_elut_index_loading(self):
        """Test that ELUT index is loaded correctly."""
        manager = ELUTManager()

        assert hasattr(manager, 'elut_index')
        assert len(manager.elut_index) > 0

        # Check that index contains expected structure
        # Each entry should have start_date, end_date, elut_file
        start, end, elut = list(manager.elut_index.at(manager.elut_index.begin()))[0]
        assert isinstance(start, datetime)
        assert isinstance(end, datetime)
        assert isinstance(elut, Path)

    def test_get_available_eluts(self):
        """Test getting list of available ELUTs."""
        manager = ELUTManager()

        available_eluts = manager.get_available_eluts()

        assert isinstance(available_eluts, IntervalTree)
        assert len(available_eluts) > 0

    def test_find_elut_file_known_date(self):
        """Test finding ELUT file for a known date."""
        manager = ELUTManager()

        # Use a date that should be covered by existing ELUTs
        test_date = datetime(2022, 8, 1)

        elut_file = manager._find_elut_file(test_date)

        assert elut_file is not None
        assert isinstance(elut_file, Path)
        assert elut_file.suffix == '.csv'

        # Verify the file exists
        elut_path = manager.data_root / elut_file
        assert elut_path.exists()

    def test_get_elut_known_date(self):
        """Test getting ELUT for a known date."""
        manager = ELUTManager()

        # Use a date that should be covered by existing ELUTs
        test_date = datetime(2022, 8, 1)

        elut_table, sci_channels = manager.get_elut(test_date)

        assert elut_table is not None
        assert sci_channels is not None

        # Basic validation of returned objects
        assert isinstance(elut_table, object)
        assert isinstance(sci_channels, QTable)

    def test_cache_functionality(self):
        """Test that caching works correctly."""
        manager = ELUTManager()

        test_date = datetime(2022, 8, 1)

        # First call should populate cache
        initial_cache_size = manager.cache_size
        result1 = manager.get_elut(test_date)
        assert manager.cache_size == initial_cache_size + 1

        # Second call should use cache
        result2 = manager.get_elut(test_date)
        assert manager.cache_size == initial_cache_size + 1

        # Results should be identical (cached)
        assert result1 is result2

    def test_clear_cache(self):
        """Test cache clearing functionality."""
        manager = ELUTManager()

        # Load an ELUT to populate cache
        test_date = datetime(2022, 8, 1)
        manager.get_elut(test_date)

        assert manager.cache_size > 0

        # Clear cache
        manager.clear_cache()

        assert manager.cache_size == 0

    def test_cache_size_property(self):
        """Test cache size property."""
        manager = ELUTManager()

        initial_size = manager.cache_size
        assert isinstance(initial_size, int)
        assert initial_size >= 0

        # Load an ELUT
        test_date = datetime(2022, 8, 1)
        manager.get_elut(test_date)

        assert manager.cache_size == initial_size + 1

    def test_different_dates_different_eluts(self):
        """Test that different dates can return different ELUTs."""
        manager = ELUTManager()

        # Use dates that should map to different ELUT files
        date1 = datetime(2021, 6, 1)  # Should use one ELUT
        date2 = datetime(2024, 8, 1)  # Should use different ELUT

        elut_file1 = manager._find_elut_file(date1)
        elut_file2 = manager._find_elut_file(date2)

        assert elut_file1 is not None
        assert elut_file2 is not None
        assert elut_file1 != elut_file2

    def test_read_elut_directly(self):
        """Test reading ELUT file directly."""
        manager = ELUTManager()

        date1 = datetime(2021, 6, 1)  # Should use one ELUT

        sci_channels_o = get_sci_channels(date1)
        elut_file1 = manager._find_elut_file(date1)
        elut_table_o = read_elut(elut_file1, sci_channels_o)

        elut_table_m, sci_channels_m = manager.get_elut(date1)
        # Ensure it can be retrieved via manager
        assert np.all(elut_table_o.e_actual == elut_table_m.e_actual)
        assert np.all(sci_channels_o == sci_channels_m)

    def test_error_handling_invalid_date(self):
        """Test error handling for dates outside available range."""
        manager = ELUTManager()

        # Use a date far in the past that shouldn't have ELUT data
        very_old_date = datetime(1990, 1, 1)

        with pytest.raises(ValueError, match="No ELUT"):
            manager.get_elut(very_old_date)

    def test_error_handling_nonexistent_file(self):
        """Test error handling when trying to read non-existent ELUT file."""
        manager = ELUTManager()

        from stixpy.calibration.detector import get_sci_channels
        sci_channels = get_sci_channels(datetime(2022, 8, 1))

        with pytest.raises(FileNotFoundError, match="ELUT file not found"):
            manager.read_elut("nonexistent_file.csv", sci_channels)

    def test_instance_attribute_exists(self):
        """Test that singleton instance is accessible."""
        assert hasattr(ELUTManager, 'instance')
        assert isinstance(ELUTManager.instance, ELUTManager)
