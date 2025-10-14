"""Module for the different processing levels."""

from pathlib import Path

from matplotlib import pyplot as plt

from stixcore.io.product_processors.fits.processors import FitsL2Processor
from stixcore.util.logging import get_logger

__all__ = [
    "PlotProcessor",
]


logger = get_logger(__name__)


class PlotProcessor(FitsL2Processor):
    """A file product processor for plot images"""

    def __init__(self, archive_path):
        """Creates a new PlotProcessor object.

        Parameters
        ----------
        archive_path : Path
            the output root path where the files should be created
        """
        super().__init__(archive_path)

    def generate_filename(self, product, *, version=0, suffix=".svg"):
        """Generates a SOAR conform filename based on product characteristics.

        Parameters
        ----------
        product : Product
            The data product the file name should be generated for
        version : int, optional
            The file version, by default 0 = detect from codebase
        suffix : str, optional
            file name suffix like svg, png, ..., by default '.svg'

        Returns
        -------
        Path
            a Path object with full name and path
        """
        p = Path(super().generate_filename(product=product, version=version, header=True, status="C"))
        return p.with_suffix(suffix).name

    def generate_primary_header(self, filename, product, *, version=0):
        """Transforms the fits header into a more generic header dict
        that might be used in other output file formats

        Parameters
        ----------
        filename : Path
            The envisoned file name and path for the product
        product : Product
            The products holding the data
        version : int, optional
            the processing version, by default 0 = detect from codebase

        Returns
        -------
        dict
            a dict of header keywords and values
        """
        l1, l2 = super().generate_primary_header(filename, product, version=version)
        header = dict()
        for k, v in l1:
            header[k] = v
        for kv in l2:
            header[kv[0]] = kv[1]
        return header

    def write_plot(self, product, *, version=0):
        """
        Write products into a plot image file.

        Parameters
        ----------
        product : `stixcore.product.level2`

        version : `int`
            the version modifier for the filename
            default 0 = detect from codebase.

        Returns
        -------
        list
            of created file as `pathlib.Path`

        """
        if version == 0:
            version = product.get_processing_version()

        filename = self.generate_filename(product=product, version=version)

        # headers = self.generate_primary_header(filename, product, version=version)
        # headers['parent'] = get_complete_file_name(product.parent_file_path.name)

        parts = [product.level, product.utc_timerange.center.strftime("%Y/%m/%d"), product.type.upper()]
        # for science data use start date
        if product.type in ["sci", "flarelist"]:
            parts[1] = product.utc_timerange.start.strftime("%Y/%m/%d")
        path = self.archive_path.joinpath(*[str(x) for x in parts])
        path.mkdir(parents=True, exist_ok=True)

        plot_path = path / filename

        fig = product.get_plot()
        fig.savefig(plot_path, format="svg")
        plt.close(fig)
        return plot_path
