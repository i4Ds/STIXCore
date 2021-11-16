"""Module for the interacting with SSWIDL"""


import os
import abc

from stixcore.config.config import CONFIG

__all__ = ['SSWIDLProcessor', 'SSWIDLTask']


class BaseTask:
    def __init__(self, script, params):
        self.script = '' + script
        self.params = {'gsw_path': "/opt/STIX-GSW",
                       'work_dir': os.getcwd()}
        self.params.update(params)
        self._results = list()

    @property
    def results(self):
        return self._results

    @property
    def key(self):
        return type(self)

    @abc.abstractmethod
    def run(self):
        pass

    @abc.abstractmethod
    def pack_params(self):
        return self.params

    @abc.abstractmethod
    def postprocessing(self, result, args):
        return result

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if isinstance(other, BaseTask):
            return self.key == other.key
        return NotImplemented


if CONFIG.getboolean("IDLBridge", "enabled", fallback=False):
    import hissw

    class SSWIDLTask(BaseTask):
        def __init__(self, script, params):
            super().__init__(script, params)

            self.script = '''

    !PATH=!PATH+':'+Expand_Path('+{{ gsw_path }}')
    setenv, "IDL_PROJECT_NAME=stix ppl"
    setenv, "IDL_WORKSPACE_PATH={{ gsw_path }}"

    d = get_delim()

    ''' + script

        def run(self):
            ssw = hissw.Environment(ssw_home="/usr/local/ssw",
                                    idl_home="/usr/local/idl/idl88",
                                    ssw_packages=["goes", "hessi", "spex", "xray",
                                                  "sunspice", "spice", "stix"])

            results = ssw.run(self.script, args=self.pack_params())
            self._results = self.postprocessing(results)

else:
    class SSWIDLTask(BaseTask):

        def __init__(self, script, params):
            super().__init__(script, params)

        def run(self):
            p = self.pack_params()
            results = p
            self._results = self.postprocessing(results)
            return self._results


class SSWIDLProcessor(dict):
    def __init__(self, fits_processor):
        # TODO how to check without: ImportError: cannot import name 'FitsL2Processor'
        # from partially initialized module 'stixcore.io.fits.processors'
        # (most likely due to a circular import)
        # if not isinstance(fits_processor, FitsL2Processor):
        #    raise ValueError("processor must be of type FitsL2Processor")
        self.fits_processor = fits_processor

    def __getitem__(self, key):
        if key not in self:
            dict.__setitem__(self, key, key())
        val = dict.__getitem__(self, key)
        return val

    def __setitem__(self, key, val):
        dict.__setitem__(self, key, val)

    def process(self):
        files = []
        for task in self.values():
            files.extend(task.run())
        return files
