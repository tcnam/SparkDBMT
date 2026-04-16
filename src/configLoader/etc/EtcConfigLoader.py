from pathlib import Path
from typing import Any, Dict

from src.configLoader.ConfigLoader import ConfigLoader
from src.const import const

class EtcConfigLoader(ConfigLoader):
    def __init__(self, **kwargs):
        """Initializes the ETC configuration loader.

        Args:
            fileName (str): Name of the ETC configuration file to load
                (e.g. ``api_endpoint_info.yml``, ``host_env_info.yml``).
        """
        super().__init__()
        self.fileName: str = kwargs.get("fileName", "")
        self.relFolderPath: Path = const.etcDir
        self.filePath: Path = self.relFolderPath.joinpath(self.fileName)
        self.config: Dict[Any, Any] = self.loadConfig()