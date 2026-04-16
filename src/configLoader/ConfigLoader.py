from abc import ABC
from yaml import safe_load
from pathlib import Path
from typing import (
    Any, Dict, Union
)

class ConfigLoader(ABC):
    def __init__(self):
        """Initializes base attributes for a configuration loader."""
        self.fileName: str = None
        self.relFolderPath: Path = None
        self.filePath: Path = None
        self.config: Dict[Any, Any] = None
    
    def loadConfig(self) -> Dict[Any, Any]:
        """Loads and parses the configuration file from disk.

        Returns:
            Dict[Any, Any]: Parsed configuration content loaded from the file.
        """
        with open(file=self.filePath, mode="r") as io:
            return safe_load(io)
    
    def __getitem__(self, index: Union[int, str]):
        """Retrieves a configuration value by key.

        Args:
            index (Union[int, str]): Key used to access the configuration.

        Returns:
            Any: Configuration value associated with the given key, or
            ``None`` if the key does not exist.
        """
        return self.config.get(index)

    def __setitem__(self, index: Union[int, str], value: str):
        """Sets or updates a configuration value.

        Args:
            index (Union[int, str]): Configuration key.
            value (str): Value to assign to the given key.
        """
        self.config[index] = value

    def __str__(self):
        """Returns a human-readable representation of the configuration loader.

        Returns:
            str: String containing file metadata and loaded configuration.
        """
        return (
            f"File Name: {self.fileName}, "
            f"Relative Path: {self.relFolderPath}, "
            f"File Path: {self.filePath}, "
            f"Config: {self.config}"
        )