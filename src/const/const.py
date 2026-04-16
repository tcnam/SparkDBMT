from pathlib import Path
from typing import Dict, Union

srcDir: Path = Path(__file__).resolve().parent.parent
etcDir: Path = srcDir.joinpath("etc")

jarDir: Path = srcDir.parent.joinpath("jars")

