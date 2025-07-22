import json
import shutil
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, mock_open

import pytest

from stixcore.ecc.manager import ECCManager


class TestECCManager:
    """Test cases for the ECCManager class."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        # Reset singleton instance for each test
        ECCManager._instances = {}

        # Create temporary directory for test data
        self.temp_dir = Path(tempfile.mkdtemp())
        self.ecc_dir = self.temp_dir / "ecc"
        self.ecc_dir.mkdir()

        # Create test configuration directories
        self.config1_dir = self.ecc_dir / "ecc_cfg_1"
        self.config2_dir = self.ecc_dir / "ecc_cfg_2"
        self.config1_dir.mkdir()
        self.config2_dir.mkdir()

        # Create test files in configuration directories
        (self.config1_dir / "config.json").write_text('{"test": "config1"}')
        (self.config2_dir / "config.json").write_text('{"test": "config2"}')
        (self.config1_dir / "params.txt").write_text("test parameters 1")
        (self.config2_dir / "params.txt").write_text("test parameters 2")

        # Test configuration index data
        self.test_index = [
            {
                "configuration": "ecc_cfg_1",
                "description": "Test ECC configuration 1",
                "validityPeriodUTC": [
                    "2020-01-01T00:00:00.000+00:00",
                    "2022-01-01T00:00:00.000+00:00"
                ]
            },
            {
                "configuration": "ecc_cfg_2",
                "description": "Test ECC configuration 2",
                "validityPeriodUTC": [
                    "2022-01-01T00:00:00.000+00:00",
                    "2024-01-01T00:00:00.000+00:00"
                ]
            }
        ]

    def teardown_method(self):
        """Clean up after each test method."""
        # Clean up temporary directory
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_initialization_with_data_root(self):
        """Test ECCManager initialization with custom data root."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            assert manager.data_root == self.ecc_dir
            assert len(manager.configurations) == 2
            assert manager.configurations[0]["configuration"] == "ecc_cfg_1"

    def test_initialization_without_data_root(self):
        """Test ECCManager initialization with default data root."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager()

            # Should use default path
            expected_path = Path(__file__).parent.parent.parent / "config" / "data"\
                / "common" / "ecc"
            assert manager.data_root == expected_path

    def test_data_root_setter_valid_path(self):
        """Test setting data_root with valid path."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            new_dir = self.temp_dir / "new_ecc"
            new_dir.mkdir()

            manager.data_root = new_dir
            assert manager.data_root == new_dir

    def test_data_root_setter_invalid_path(self):
        """Test setting data_root with invalid path."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            invalid_path = self.temp_dir / "nonexistent"

            with pytest.raises(FileNotFoundError):
                manager.data_root = invalid_path

    def test_load_index_success(self):
        """Test successful loading of index file."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            assert len(manager.configurations) == 2
            assert manager.configurations[0]["configuration"] == "ecc_cfg_1"
            assert manager.configurations[1]["configuration"] == "ecc_cfg_2"

    def test_load_index_file_not_found(self):
        """Test handling of missing index file."""
        with patch('stixcore.ecc.manager.open', side_effect=FileNotFoundError):
            manager = ECCManager(data_root=self.ecc_dir)

            assert manager.configurations == []

    def test_load_index_json_decode_error(self):
        """Test handling of malformed JSON in index file."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data='{"invalid": json}')):
            manager = ECCManager(data_root=self.ecc_dir)

            assert manager.configurations == []

    def test_find_configuration_no_date(self):
        """Test finding configuration without specifying date."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            config = manager.find_configuration()
            assert config == "ecc_cfg_1"

    def test_find_configuration_with_date(self):
        """Test finding configuration with specific date."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            # Date within first configuration validity period
            date1 = datetime(2021, 6, 15)
            config1 = manager.find_configuration(date1)
            assert config1 == "ecc_cfg_1"

            # Date within second configuration validity period
            date2 = datetime(2023, 6, 15)
            config2 = manager.find_configuration(date2)
            assert config2 == "ecc_cfg_2"

    def test_find_configuration_no_match(self):
        """Test finding configuration with date outside all validity periods."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            # Date outside all validity periods
            date = datetime(2025, 6, 15)
            config = manager.find_configuration(date)
            assert config is None

    def test_find_configuration_empty_configurations(self):
        """Test finding configuration when no configurations are loaded."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data='[]')):
            manager = ECCManager(data_root=self.ecc_dir)

            config = manager.find_configuration()
            assert config is None

    def test_get_configurations(self):
        """Test getting all configurations."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            configs = manager.get_configurations()
            assert len(configs) == 2
            assert configs[0]["configuration"] == "ecc_cfg_1"
            assert configs[1]["configuration"] == "ecc_cfg_2"

            # Ensure it returns a copy
            configs[0]["configuration"] = "modified"
            assert manager.get_configurations()[0]["configuration"] == "ecc_cfg_1"

    def test_has_configuration_exists(self):
        """Test checking if configuration exists."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            assert manager.has_configuration("ecc_cfg_1") is True
            assert manager.has_configuration("ecc_cfg_2") is True

    def test_has_configuration_not_exists(self):
        """Test checking if non-existent configuration exists."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            assert manager.has_configuration("ecc_cfg_nonexistent") is False

    def test_get_configuration_path(self):
        """Test getting configuration path."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            path = manager.get_configuration_path("ecc_cfg_1")
            assert path == self.ecc_dir / "ecc_cfg_1"

    def test_create_context_success(self):
        """Test successful context creation."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            date = datetime(2021, 6, 15)
            context_path = manager.create_context(date)

            try:
                assert context_path.exists()
                assert context_path.is_dir()

                # Check that configuration files were copied
                config_dir = context_path
                assert config_dir.exists()
                assert (config_dir / "config.json").exists()
                assert (config_dir / "params.txt").exists()

                # Verify file contents
                config_content = (config_dir / "config.json").read_text()
                assert json.loads(config_content)["test"] == "config1"

            finally:
                # Clean up
                if context_path.exists():
                    shutil.rmtree(context_path)

    def test_create_context_no_configuration_found(self):
        """Test context creation when no configuration is found."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            # Date outside all validity periods
            date = datetime(2025, 6, 15)

            with pytest.raises(ValueError, match="No ECC configuration found for date"):
                manager.create_context(date)

    def test_create_context_configuration_directory_not_found(self):
        """Test context creation when configuration directory doesn't exist."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            # Remove the configuration directory
            shutil.rmtree(self.config1_dir)

            date = datetime(2021, 6, 15)

            with pytest.raises(FileNotFoundError, match="Configuration directory not found"):
                manager.create_context(date)

    def test_cleanup_context(self):
        """Test context cleanup."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            date = datetime(2021, 6, 15)
            context_path = manager.create_context(date)

            assert context_path.exists()

            manager.cleanup_context(context_path)

            assert not context_path.exists()

    def test_cleanup_context_nonexistent(self):
        """Test cleanup of non-existent context."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            nonexistent_path = self.temp_dir / "nonexistent"

            # Should not raise an exception
            manager.cleanup_context(nonexistent_path)

    def test_singleton_instance_attribute(self):
        """Test that singleton instance is accessible via class attribute."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            # The instance should be created automatically
            assert hasattr(ECCManager, 'instance')
            assert isinstance(ECCManager.instance, ECCManager)

    def test_context_manager_success(self):
        """Test successful context manager usage."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            date = datetime(2021, 6, 15)

            with manager.context(date) as context_path:
                # Context should be created successfully
                assert context_path.exists()
                assert context_path.is_dir()

                # Check that configuration files were copied
                config_dir = context_path
                assert config_dir.exists()
                assert (config_dir / "config.json").exists()
                assert (config_dir / "params.txt").exists()

                # Verify file contents
                config_content = (config_dir / "config.json").read_text()
                assert json.loads(config_content)["test"] == "config1"

                # Store path for later verification
                temp_path = context_path

            # After exiting context, directory should be cleaned up
            assert not temp_path.exists()

    def test_context_manager_no_date(self):
        """Test context manager without specifying date."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            with manager.context() as context_path:
                # Should use first configuration
                assert context_path.exists()
                config_dir = context_path
                assert config_dir.exists()
                assert (config_dir / "params.txt").exists()

                temp_path = context_path

            # Cleanup should happen automatically
            assert not temp_path.exists()

    def test_context_manager_exception_during_usage(self):
        """Test context manager cleanup when exception occurs during usage."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            date = datetime(2021, 6, 15)
            temp_path = None

            try:
                with manager.context(date) as context_path:
                    temp_path = context_path
                    assert context_path.exists()
                    # Simulate an exception during usage
                    raise ValueError("Test exception")
            except ValueError:
                # Exception should be propagated
                pass

            # Cleanup should still happen despite exception
            assert temp_path is not None
            assert not temp_path.exists()

    def test_context_manager_no_configuration_found(self):
        """Test context manager when no configuration is found."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            # Date outside all validity periods
            date = datetime(2025, 6, 15)

            with pytest.raises(ValueError,
                               match="No ECC configuration found for date"):
                with manager.context(date):
                    pass

    def test_context_manager_configuration_directory_not_found(self):
        """Test context manager when configuration directory doesn't exist."""
        with patch('stixcore.ecc.manager.open', mock_open(read_data=json.dumps(self.test_index))):
            manager = ECCManager(data_root=self.ecc_dir)

            # Remove the configuration directory
            shutil.rmtree(self.config1_dir)

            date = datetime(2021, 6, 15)

            with pytest.raises(FileNotFoundError, match="Configuration directory not found"):
                with manager.context(date):
                    pass
