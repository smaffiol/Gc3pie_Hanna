#! /usr/bin/env python

import os
from os.path import abspath, basename
import gc3libs
import gc3libs.exceptions
from gc3libs import Application
from gc3libs.cmdline import SessionBasedScript, existing_directory, existing_file
import gc3libs.utils

# Constants
PARAMETERS_PER_RUN = 11
MATLAB_CMD = "matlab -nosplash -nodisplay -nodesktop -r \"addpath(./{matlab_source}); try {main_function}({args},{run}); end; quit\""
# DEFAULT_REMOTE_OUTPUT_FOLDER = "Results/"
# DEFAULT_CLOUD_OUTPUT_FOLDER = ":~/Results/"
# CLOUDNAME = "ubuntu@172.23.72.113:~/Results/"

# Utility functions
if __name__ == '__main__':
    from Matlabtasks import OomScript

    OomScript().run()


def _enumerate_csv(input_csv):
    """
    For each line of the input .csv file
    return list of parameters
    """
    with open(input_csv) as fd:
        for num, params in enumerate(fd, 1):
            # skip first line as input .csv contains headers
            if num == 1:
                continue
            if len(params.split(',')) != PARAMETERS_PER_RUN:
                gc3libs.log.error("Wrong number of input parameters in line {0}."
                                  "Expectd {1}, found {2}. Skipping line.".format(num,
                                                                                  PARAMETERS_PER_RUN,
                                                                                  params))
            else:
                yield str(num), params.strip()


# SessionBasedScript
class OomScript(SessionBasedScript):
    """
    Get tasks
    """

    def __init__(self):
        super(OomScript, self).__init__(version='1.0')

    def setup_options(self):
        self.add_param("-d", "--mfuncs", metavar="PATH",
                       type=existing_directory,
                       dest="matlab_source_folder", default=None,
                       help="Location of the Matlab scripts and "
                            "related Matlab functions. Default: %(default)s.")

        self.add_param("-F", "--datatransfer", dest="transfer_data",
                       action="store_true", default=False,
                       help="Transfer input data to compute nodes. "
                            "If False, data will be assumed be already visible on "
                            "compute nodes - e.g. shared filesystem. "
                            "Default: %(default)s.")

    def setup_args(self):
        self.add_param('matlab_function', type=str,
                       help="Matlab function name")

        self.add_param('csv_input_file', type=existing_file,
                       help="Input .csv file with all parameters to be passed"
                            " to the Matlab function.")

    def parse_args(self):
        """
        local result folder should exist
        """
        if self.params.transfer_data and not os.path.isdir(local_result_folder):
            os.mkdir(os.path.join(local_result_folder))


    def new_tasks(self, extra):
        """
        For each line of the input .csv file generate
        an execution Task
        """
        tasks = []
        for (run, parameter) in _enumerate_csv(self.params.csv_input_file):
            jobname = "run{0}".format(run)
            extra_args = extra.copy()
            extra_args['jobname'] = jobname
            extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', jobname)

            tasks.append(MatlabApp(self.params.matlab_function,
                                   parameter,
                                   self.params.matlab_source_folder,
                                   run,
                                   **extra_args))
        return tasks


# Application
class MatlabApp(Application):
    """Run a MATLAB source file."""
    application_name = 'matlab'

    def __init__(self, mfunc, params, matlabfolder, run, **extra_args):

        inputs = []
        outputs = []
        executables = []

        timefile = "timefile{0}.txt".format(run)
        popfile = "popfile{0}.txt".format(run)
        meanfile = "meanresults{0}.txt".format(run)
        self.out = [meanfile, timefile, popfile]

        if extra_args['transfer_data']:
            # Include Matlab source folder
            inputs.append(matlabfolder)
            outputs.extend(self.out)

        code_func_name = basename(mfunc)[:-len('.m')] # remove `.m` extension

        cmd = MATLAB_CMD.format(matlab_source=os.path.basename(os.path.abspath(matlabfolder)),
                                main_function=code_func_name,
                                args=params,
                                run=run)

        gc3libs.log.info("Creating new Application execution: '{0}'".format(cmd))

        Application.__init__(
            self,
            arguments=cmd,
            inputs=inputs,
            outputs=outputs,
            stdout="matlab.log",
            join=True,
            **extra_args
        )
