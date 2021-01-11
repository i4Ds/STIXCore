from stixcore.calibration.compression import decompress as algo_decompress


def work_decompress(val, skm):
    return algo_decompress(val, s=skm[0], k=skm[1], m=skm[2])


def decompress(packet):
    decompression_parameter = packet.get_decompression_parameter()
    if not decompression_parameter:
        return 0
    c = 1
    for param_name, (sn, kn, mn) in decompression_parameter.items():
        skm = (sn if isinstance(sn, int) else packet.data.get(sn),
               packet.data.get(kn),
               packet.data.get(mn))
        c += packet.data.work(param_name, work_decompress, skm)
    return c
