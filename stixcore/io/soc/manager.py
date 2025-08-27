"""Module to encapsulate the SOC file reading and writing."""

from pathlib import Path
from binascii import unhexlify
from xml.etree import ElementTree as Et

from stixcore.tmtc.packets import TMTC
from stixcore.util.logging import get_logger

__all__ = ["SOCManager", "SOCPacketFile"]


logger = get_logger(__name__)


class SOCPacketFile:
    """Represents a SOC xml file handler that can contain TM or TC data."""

    def __init__(self, file):
        """Create a handler for SOC file.

        Parameters
        ----------
        file : `Path`
            The path to the SOC xml file.

        Raises
        ------
        ValueError
            If the file does not exists.
        ValueError
            If the file does not contain the expected XML structure.
        """
        self.file = Path(file)
        if not self.file.exists():
            raise ValueError(f"file path not found: {file}")
        stat = self.file.stat()
        self.size = stat.st_size
        self.file_date = stat.st_ctime
        self.spacecraft_date = None  # TODO some file name conventions?
        self.tmtc = TMTC.All

        with open(self.file) as f:
            header = f.read(1000)

            if "Response" not in header:
                raise ValueError("xml does not contain any 'Response'")
            if "PktRawResponse" in header:
                self.tmtc = TMTC.TM
            elif "PktTcReportResponse" in header:
                self.tmtc = TMTC.TC
            else:
                raise ValueError("xml does not contain any TM or TC response")

    def get_packet_binaries(self):
        """Get sequence of binary packet data form the SOC file.

        Yields
        -------
        ´bytes´
            the next binary data of hexadecimal representation form the SOC file.
        """
        root = Et.parse(str(self.file)).getroot()
        if self.tmtc == TMTC.TC:
            for i, node in enumerate(root.iter("PktTcReportListElement")):
                # TODO add TC packer reading
                pass
        elif self.tmtc == TMTC.TM:
            for node in root.iter("PktRawResponseElement"):
                packet_id = int(node.attrib["packetID"])
                packet = list(node)[0]
                packet_binary = unhexlify(packet.text)
                # Not sure why guess and extra moc header
                yield packet_id, packet_binary[76:]

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.file}')"


class SOCManager:
    """Manages the SOC data exchange directory and provides excess and search methods."""

    def __init__(self, data_root):
        """Create the manager for a given data path root.

        Parameters
        ----------
        data_root : `str` | `pathlib.Path`
            Path to the directory with all SOC data files for processing
        """
        self.data_root = Path(data_root)
        if not self.data_root.exists():
            raise ValueError(f"path not found: {data_root}")
        logger.info(f"Create SOCManager @ {self.data_root}")

    def get_files(self, tmtc=TMTC.All):
        """Get all SOC files from the input directory that contain TM or TC data.

        Parameters
        ----------
        tmtc : `TMTC`, optional
            Filter for TC | TM or ALL, by default TMTC.All

        Yields
        ------
        `SOCPacketFile`
            the next found SOC input file.
        """
        filter = str(tmtc)

        if tmtc == TMTC.All:
            filter = "*.xml"
        elif tmtc == TMTC.TC:
            filter = "*PktTcReport*.xml"
        elif tmtc == TMTC.TM:
            filter = "*PktTmRaw*.xml"

        for filename in sorted(list(self.data_root.glob(filter))):
            try:
                file = SOCPacketFile(filename)
                yield file
            except ValueError:
                pass

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.data_root}')"
