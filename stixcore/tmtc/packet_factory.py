
from stixcore.tmtc.packets import GenericPacket, GenericTMPacket, SourcePacketHeader

__all__ = ['Packet', 'BaseFactory']


class BaseFactory:
    """
    An abstract base factory
    """
    def __init__(self, registry=None):
        """
        Method for running the factory.

        Arguments args and kwargs are passed through to the validation
        function and to the constructor for the final type.

        Parameters
        ----------
        registry
        """
        if registry is None:
            self.registry = dict()
        else:
            self.registry = registry

    def __call__(self, data, idb):
        return self._check_registered(data, idb)

    def _check_registered(self, data, idb):
        """
        Implementation of a basic check to see if arguments match against the registered classes.

        Parameters
        ----------
        data
            Input data

        Returns
        -------
        The registered class if found.
        """
        candidates = list()

        for key in self.registry:
            # Call the registered validation function for each registered class
            if self.registry[key](data):
                candidates.append(key)

        n_matches = len(candidates)

        if n_matches == 0:
            raise NoMatchError("No types match specified arguments")
        elif n_matches > 1:
            raise MultipleMatchError("Too many candidate types identified ")

        # Only one is found
        PacketType = candidates[0]

        return PacketType(data, idb)

    def register(self, PacketType, validation_function):
        if validation_function is not None:
            if not callable(validation_function):
                raise AttributeError("Keyword argument 'validation_function' must be callable.")
            self.registry[PacketType] = validation_function

    def unregister(self, PacketType):
        self.registry.pop(PacketType)


class TMTCPacketFactory(BaseFactory):
    """
    Factory from TM/TC packets returning either TM or TC Packets
    """
    def __init__(self, registry=None):
        # if not isinstance(idb, IDB):
        #    raise AttributeError("idb is not of type IDB")
        super().__init__(registry=registry)
        self.tm_packet_factory = TMPacketFactory(registry=GenericTMPacket._registry)

    def __call__(self, data, idb):
        sph = SourcePacketHeader(data, idb)
        packet = self._check_registered(sph, idb)
        return self.tm_packet_factory(packet, idb)


class TMPacketFactory(BaseFactory):
    """
    Factory from TM packet return the correct type of based on the packet data and registered.
    """
    def __call__(self, data, idb):
        return self._check_registered(data, idb)


# Main function
Packet = TMTCPacketFactory(registry=GenericPacket._registry)


class NoMatchError(Exception):
    """
    Exception for when no candidate class is found.
    """


class MultipleMatchError(Exception):
    """
    Exception for when too many candidate classes are found.
    """


class ValidationFunctionError(AttributeError):
    """
    Exception for when no candidate class is found.
    """
