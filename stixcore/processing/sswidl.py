"""Module for the interacting with SSWIDL"""

import os
import abc
from pathlib import Path

from stixcore.config.config import CONFIG
from stixcore.util.logging import get_logger

__all__ = ['SSWIDLProcessor', 'SSWIDLTask']

logger = get_logger(__name__)


class BaseTask:
    def __init__(self, *, script="", work_dir=".", params=None):
        self.script = '' + script
        self.gsw_path = Path(CONFIG.get("IDLBridge", "gsw_path", fallback="."))
        self.work_dir = self.gsw_path / work_dir

        self.params = {'gsw_path': str(self.gsw_path),
                       'work_dir': str(self.work_dir)}
        if params is not None:
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
    def postprocessing(self, result, fits_processor):
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
        def __init__(self, *, script="", work_dir=".", params=None):
            super().__init__(script=script, work_dir=work_dir, params=params)

            self.script = '''
    ; handle normal errors
    catch, error
    if error ne 0 then begin
        catch, /cancel
        print, 'A normal error occured: ' + !error_state.msg
    endif
    !PATH=!PATH+':'+Expand_Path('+{{ gsw_path }}')
    setenv, "IDL_PROJECT_NAME=stix ppl"
    setenv, "IDL_WORKSPACE_PATH={{ gsw_path }}"

    d = get_delim()

    ''' + script

        def run(self, fits_processor):
            cur_path = os.getcwd()
            os.chdir(self.work_dir)
            ssw = hissw.Environment(ssw_home="/usr/local/ssw",
                                    idl_home="/usr/local/idl/idl88",
                                    ssw_packages=["goes", "hessi", "spex", "xray",
                                                  "sunspice", "spice", "stix"])

            results = dict()
            try:
                results = ssw.run(self.script, args=self.pack_params())
            except Exception as e:
                logger.error(e)
            os.chdir(cur_path)
            self._results = self.postprocessing(results, fits_processor)
            return self._results


else:
    class SSWIDLTask(BaseTask):

        def __init__(self, *, script='', work_dir='.', params=None):
            super().__init__(script=script, work_dir=work_dir, params=params)

        def run(self, fits_processor):
            p = self.pack_params()
            results = p
            self._results = self.postprocessing(results, fits_processor)
            return self._results


class SSWIDLProcessor(dict):
    def __init__(self, fits_processor):
        # TODO how to check without: ImportError: cannot import name 'FitsL2Processor'
        # from partially initialized module 'stixcore.io.fits.processors'
        # (most likely due to a circular import)
        # if not isinstance(fits_processor, FitsL2Processor):
        #    raise ValueError("processor must be of type FitsL2Processor")
        self.fits_processor = fits_processor
        self.opentasks = 0

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
            files.extend(task.run(self.fits_processor))
        return files
