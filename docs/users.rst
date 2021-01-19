Users Guide
===========

Installation
------------


TM/TC
-----

Pipeline operations
"""""""""""""""""""
The normal work flow for parsing TM/TC is to use the pipeline to process the TM/TC data as obtained
from the EDDS. The primary interface for creating packets is the `stixcore.tmtc.Packet` class which
can create specific packet for all supported packet types

Low Level Operations
""""""""""""""""""""
Low level operation are possible using other classes a typical use case would be debugging some some
unexpect behaviours or values in the the TM/TC. The code below demonstrates how to parse manually
parse and extract the packet data from the XML file provided from the EDDS.

.. code-block:: python

    from binascii import unhexlify
    from xml.etree import ElementTree as Et

    from stixcore.tmtc import Packet
    from stixcore.tmtc.packets import TMPacket

    packet_data = []
    tree = Et.parse('path/to/moc/aaaaPktTmRawbbbbb.xml')
    root = tree.getroot()
    for i, node in enumerate(root.iter('Packet')):
        packet_binary = unhexlify(node.text)
        # Not sure why guess and extra moc header
        packet_data.append(packet_binary[76:])

Next the source header, data header and ssid or PI1_VAL can be parsed and filtered. In the following
code we filter for for server 21, subervice 6 and SSID or 21

.. code-block:: python

    res = []
    for pd in packet_data:
        tmpacket = TMPacket(pd)
        if (tmpacket.data_header.service_type == 21 and tmpacket.data_header.service_subtype == 6
            and tmpacket.pi1_val == 21):
            res.append(tmpacket)

Finally the only selected packets can be fully parsed
