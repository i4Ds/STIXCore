from stixcore.calibration.compression import decompress as algo_decompress


def work_decompress(val, skm):
    return algo_decompress(val, s=skm[0], k=skm[1], m=skm[2])


def decompress(packet):
    params = packet.get_decompression_parameter()
    if not params:
        return
    for param_name, (sn, kn, mn) in params.items():
        skm = (packet.data.get_first(sn), packet.data.get_first(kn), packet.data.get_first(mn))
        c = packet.data.work(param_name, work_decompress, skm)
        print(c, param_name, work_decompress, skm)
