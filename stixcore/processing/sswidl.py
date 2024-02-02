"""Module for the interacting with SSWIDL"""

import os
import abc
from pathlib import Path

from stixcore.config.config import CONFIG
from stixcore.util.logging import get_logger

__all__ = ['BaseTask', 'SSWIDLProcessor', 'SSWIDLTask']

logger = get_logger(__name__)


class BaseTask:
    """A processing task to do something later on."""
    def __init__(self, *, script="", work_dir=".", params=None):
        """Create a task.

        Parameters
        ----------
        script : str, optional
            a script part, by default ""
        work_dir : str, optional
            the directory where the tasks should be executed in, by default "."
        params : any, optional
            additional params, by default None
        """
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
        """Holds the taks results

        Returns
        -------
        list
            a list of all results
        """
        return self._results

    @property
    def key(self):
        """Provides a key for the collection (batch) of tasks

        Returns
        -------
        type
            the class of the tasks
        """
        return type(self)

    @abc.abstractmethod
    def run(self):
        """Run the task and store the result internally."""

    @abc.abstractmethod
    def pack_params(self):
        """Preprocessing step applying any 'formatting' to the gathered parameter.

        Returns
        -------
        any
            the pre processed parameters
        """
        return self.params

    @abc.abstractmethod
    def postprocessing(self, result, fits_processor):
        """Postprocessing step to applying any 'formatting' to the gathered result.

        Parameters
        ----------
        result : any
            the result of the processing task
        fits_processor : FitsProcessor
            a fits processor to write out product as fits

        Returns
        -------
        any
            the post processed result
        """
        return result

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if isinstance(other, BaseTask):
            return self.key == other.key
        return NotImplemented


# only enable the IDL bridge if enabled in config
if CONFIG.getboolean("IDLBridge", "enabled", fallback=False):
    import hissw

    class SSWIDLTask(BaseTask):
        """A task that will use IDL to process any data."""
        def __init__(self, *, script="", work_dir=".", params=None):
            """Create a tasks that will use IDL to process data.

            Parameters
            ----------
            script : str, optional
                the IDL script to run, by default ""
            work_dir : str, optional
                the directory where the tasks should be executed in, by default "."
            params : _type_, optional
                additional params, by default None
            """
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
    setenv, "SSW_STIX={{ gsw_path }}/stix"

    d = get_delim()

    ''' + script

        def run(self, fits_processor):
            """Run the task and store the result internally."""
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
    # on systems where IDL is not enabled the IDL TASK will do nothing
    # the result will be the input
    class SSWIDLTask(BaseTask):

        def __init__(self, *, script='', work_dir='.', params=None):
            super().__init__(script=script, work_dir=work_dir, params=params)

        def run(self, fits_processor):
            """Run the task and store the result internally."""
            p = self.pack_params()
            results = p
            self._results = self.postprocessing(results, fits_processor)
            return self._results


class SSWIDLProcessor(dict):
    """A collector class for IDL processing tasks to run later."""
    def __init__(self, fits_processor):
        """_summary_

        Parameters
        ----------
        fits_processor : FitsL2Processor
            a fits processor to write out product as fits
        """
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
        """Runs all collected tasks with the processor.

        Returns
        -------
        list
            a list of all generated fits files
        """
        files = []
        for task in self.values():
            files.extend(task.run(self.fits_processor))
        return files
