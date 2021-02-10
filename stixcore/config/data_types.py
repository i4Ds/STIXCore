__all__ = ['EnergyChannel']


class EnergyChannel:
    """Represent a STIX energy channel.

    Attributes
    ----------
    channel_edge : ´int´
        chanel idx
    energy_edge : ´int´
        edge on energy axis
    e_lower : ´float´
        start of the energy band in keV
    e_upper : ´float´
        end of the energy band in keV
    bin_width : ´float´
        width of the energy band in keV
    dE_E : ´float´
        TODO
    """

    def __init__(self, *, channel_edge, energy_edge, e_lower, e_upper, bin_width, dE_E):
        self.channel_edge = channel_edge
        self.energy_edge = energy_edge
        self.e_lower = e_lower
        self.e_upper = e_upper
        self.bin_width = bin_width
        self.dE_E = dE_E

    def __repr__(self):
        return f'{self.__class__.__name__}(channel_edge={self.channel_edge}, ' +\
               f'energy_edge={self.energy_edge}, e_lower={self.e_lower}, e_upper={self.e_upper},' +\
               f' bin_width={self.bin_width}, dE_E={self.dE_E})'
