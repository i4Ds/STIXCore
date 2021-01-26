import os
from pathlib import Path
from binascii import unhexlify
from xml.etree import ElementTree as Et
from collections import defaultdict

from stixcore.tmtc.packets import TMTC, TMPacket
from stixcore.util.logging import get_logger

__all__ = ['SOCManager', 'SOCPacketFile']


logger = get_logger(__name__)


class SOCPacketFile:
    def __init__(self, file):
        self.file = Path(file)
        if not self.file.exists():
            raise ValueError(f"file path not found: {file}")
        stat = self.file.stat()
        self.size = stat.st_size
        self.file_date = stat.st_ctime
        self.spacecraft_date = None  # TODO some file name conventions?
        self.tmtc = TMTC.All
        self.packet_data = defaultdict(list)

        with open(self.file, "r") as f:
            header = f.read(1000)

            if 'Response' not in header:
                raise ValueError("xml does not contain any 'Response'")
            if 'PktRawResponse' in header:
                self.tmtc = TMTC.TM
            elif 'PktTcReportResponse' in header:
                self.tmtc = TMTC.TC
            else:
                raise ValueError("xml does not contain any TM or TC response")

    def free_packet_data(self):
        self.packet_data = defaultdict(list)

    def read_packet_data_header(self):
        root = Et.parse(str(self.file)).getroot()
        if self.tmtc == TMTC.TC:
            for i, node in enumerate(root.iter('PktTcReportListElement')):
                # TODO add TC packer reading
                pass
        elif self.tmtc == TMTC.TM:
            for i, node in enumerate(root.iter('Packet')):
                packet_binary = unhexlify(node.text)
                # Not sure why guess and extra moc header
                tm_packet = TMPacket(packet_binary[76:])
                self.packet_data[tm_packet.key].append(tm_packet)


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
        files = list()

        filter = "*.xml"
        if tmtc == TMTC.TC:
            filter = "*PktTcReport*.xml"
        elif tmtc == TMTC.TM:
            filter = "*PktTmRaw*.xml"

        for filename in sorted(list(self.data_root.glob(filter)), key=os.path.getmtime):
            try:
                file = SOCPacketFile(filename)
                if file.tmtc.value & tmtc.value:
                    files.append(file)
            except ValueError:
                pass
        return files
