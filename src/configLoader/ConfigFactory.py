from typing import Dict

from src.configLoader.ConfigLoader import ConfigLoader
from src.configLoader.project.ProjectConfigLoader import ProjectConfigLoader
from src.configLoader.etc.EtcConfigLoader import EtcConfigLoader
from src.configLoader.model.ModelConfigLoader import ModelConfigLoader


configLoaderFactories: Dict[str, ConfigLoader] = {
    "etc": EtcConfigLoader
    ,"project": ProjectConfigLoader
    ,"model": ModelConfigLoader
}


def createConfigLoader(type: str, **kwargs):
    """Factory function for creating configuration loader instances.

    This function instantiates the appropriate `ConfigLoader` subclass
    based on the provided configuration type. It centralizes configuration
    loader creation and simplifies dependency management across the ETL
    pipeline.

    Args:
        type (str): Type of configuration loader to create. Supported values
            are:
            - ``"etc"``: Environment and endpoint metadata configuration
            - ``"auth"``: Authentication configuration
            - ``"api"``: API request configuration
        **kwargs: Arbitrary keyword arguments forwarded to the selected
            configuration loader constructor.

    Returns:
        ConfigLoader: An instance of the requested configuration loader.

    Raises:
        KeyError: If the specified configuration type is not supported.
    """
    return configLoaderFactories[type](**kwargs)

