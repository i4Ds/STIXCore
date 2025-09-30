import re
from enum import Enum
from pathlib import Path
from datetime import datetime
from collections import defaultdict

from stixcore.products.product import GenericProduct
from stixcore.util.logging import get_logger

__all__ = ["TestForProcessingResult", "SingleProductProcessingStepMixin", "SingleProcessingStepResult"]

logger = get_logger(__name__)


class TestForProcessingResult(Enum):
    # do nothing with the candidate
    NotSuitable = 0
    # add to ignore list so it will not be tested again
    ToIgnore = 1
    # go on with the processing
    Suitable = 2


class SingleProcessingStepResult:
    def __init__(self, name: str, level: str, type: str, version: int, out_path: Path, in_path: Path, date: datetime):
        """Creates a SingleProcessingStepResult

        Parameters
        ----------
        name : str
            the name of the generated product
        level : str
            the level of the generated product
        type : str
            the type of the generated product
        version : int
            the version of the generated product
        out_path : Path
            path to the generated file
        in_path : Path
            the path of the used input file
        date : datetime
            when was the processing performed
        """
        self.name = name
        self.level = level
        self.type = type
        self.version = version
        self.out_path = out_path
        self.in_path = in_path
        self.date = date


class SingleProductProcessingStepMixin:
    INPUT_PATTERN = "*.fits"
    VERSION_PATTERN = re.compile(r"(.*_V)([0-9]+)U?([\._].*)")

    @property
    def ProductInputPattern(cls):
        return cls.INPUT_PATTERN

    @property
    def test_for_processing(self, path: Path) -> TestForProcessingResult:
        pass

    def find_processing_candidates(self) -> list[Path]:
        pass

    def get_version(cls, candidates: list[Path], version="latest") -> list[Path]:
        if not version == "latest":
            version = int(str(version).lower().replace("v", ""))

        index = defaultdict(dict)
        for f in candidates:
            f_name = f.name
            match = cls.VERSION_PATTERN.match(f_name)
            if match:
                f_key = f"{match.group(1)}__{match.group(3)}"
                f_version = int(match.group(2))
            else:
                f_key = f_name
                f_version = -1

            index[f_key][f_version] = f

        version_files = []
        for f_key in index:
            versions = index[f_key].keys()
            v = max(versions) if version == "latest" else version
            if v in versions:
                version_files.append(index[f_key][v])

        return version_files

    def process(self, product: GenericProduct) -> GenericProduct:
        pass

    def write_fits(self, product: GenericProduct, folder: Path) -> Path:
        pass
