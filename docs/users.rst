Users Guide
===========

Installation
------------

While under initial development STIXcore is installable form github using `pip` once we have a stable
version it will be published on pypi.

We recommend installing STIXcore in an isolated virtual environment.

.. code-block:: bash

    pip install git+https://github.com/i4ds/STIXcore

TM/TC
-----

Pipeline operations
"""""""""""""""""""
The normal work flow for parsing TM/TC is to use the pipeline to process the TM/TC data as obtained
from the EDDS. The primary interface for creating packets is the `~stixcore.tmtc.Packet` packet
factory which can create specific packet classes (`~stixcore.tmtc.tm`) given the raw packet data. For
example the code below will create a `~stixcore.tmtc.tm.tm_1.TM_1_1` packet.

.. code-block:: python

    from binascii import unhexlify
    from stixcore.tmtc import Packet

    apacket = Packet(unhexlify('0da1c17f000d1001016e27598746f4261dacca30'))

Low Level Operations
""""""""""""""""""""
Low level operation are possible using other classes a typical use case would be debugging some some
unexpected behaviours or values in the the TM/TC. The code below demonstrates how to parse manually
parse and extract the packet data from the XML file provided from the EDDS.

.. code-block:: python

    from binascii import unhexlify
    from xml.etree import ElementTree as Et



    packet_data = []
    tree = Et.parse('path/to/moc/aaaaPktTmRawbbbbb.xml')
    root = tree.getroot()
    for i, node in enumerate(root.iter('Packet')):
        packet_binary = unhexlify(node.text)
        # Not sure why guess and extra moc header
        packet_data.append(packet_binary[76:])

Next the source header, data header and ssid or PI1_VAL can be parsed and filtered. In the following
code we filter for service 21, sub-service 6 and SSID or PI1_VAL 21

.. code-block:: python

    from stixcore.tmtc.packets import TMPacket

    tm = []
    for pd in packet_data:
        tmpacket = TMPacket(pd)
        if (tmpacket.data_header.service_type == 21
            and tmpacket.data_header.service_subtype == 6 and tmpacket.pi1_val == 21):
            tm.append(tmpacket)

Finally the selected packets can be fully parsed since we have filtered the packets down to TM_21_6
SSID or PI1VAl 21 we can simply pass the partially parsed packets to the
`~stixcore.tmtc.tm.tm_21.TM_21_6_21` to fully parse the rest of the packet.

.. code-block:: python

    from stixcore.tmtc.tm import tm_21

    tm_21_6_21 = []
    for tmpackets in res:
       tm_21_6_21.append(tm_21.TM_21_6_21(res[0]))

    tm_12_6_21[0]
    TM_21_6_21(SourcePacketHeader(version=0, packet_type=0, header_flag=1, process_id=91,
                                  packet_category=12, sequence_flag=1, sequence_count=8962, data_length=3959),
               TMDataHeader(pus_version=1, service_type=21, service_subtype=6, destination_id=0,
                            scet_coarse=660106792, scet_fine=44349, datetime=0660106792:44349))
