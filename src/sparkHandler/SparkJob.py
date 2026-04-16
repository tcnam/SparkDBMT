from logging import Logger, INFO
from pathlib import Path
from typing import Any, Dict, Tuple, Union


class SparkJob:
    def __init__(self, **kwargs):
        self.paramVars: Dict[str, Union[str, int]] = kwargs.get("paramVars", {})
        self.statement: str = kwargs.get("statement", "")
    
    def prepareStatement(self):
        result: str = self.statement
        for param in self.paramVars:
            result = result.replace(param, self.paramVars.get(param))
        return result
    