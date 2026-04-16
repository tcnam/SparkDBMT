from pathlib import Path
from typing import Any, Dict

from src.configLoader.ConfigLoader import ConfigLoader
from src.const import const


class ModelConfigLoader(ConfigLoader):
    """Configuration loader for ETC (environment and endpoint) YAML files.

    This loader is responsible for reading static ETC configuration files
    (e.g. host environment info, API endpoint info) from the predefined
    ETC directory and exposing their contents through the ``ConfigLoader``
    interface.
    """

    def __init__(self, **kwargs):
        """Initializes the ETC configuration loader.

        Args:
            fileName (str): Name of the ETC configuration file to load
                (e.g. ``api_endpoint_info.yml``, ``host_env_info.yml``).
        """
        super().__init__()
        self.fileName: str = kwargs.get("fileName", "")
        self.relFolderPath: Path = Path(kwargs.get("relFolderPath", ""))
        self.modelFilePath: Path = self.relFolderPath.joinpath(f"{self.fileName}.sql")
        self.filePath: Path = self.relFolderPath.joinpath(f"{self.fileName}.yaml")
        self.config: Dict[Any, Any] = self.loadConfig()