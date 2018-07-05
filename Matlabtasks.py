#! /usr/bin/env python
import os
import sys
import time
import tempfile

import tarfile
import shutil
import pandas

from pkg_resources import Requirement, resource_filename
from os.path import abspath, basename
import gc3libs
import gc3libs.exceptions
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedScript, executable_file, existing_file
import gc3libs.utils
from gc3libs.quantity import Memory, kB, MB, GB, Duration, hours, minutes, seconds
from gc3libs.workflow import RetryableTask, SequentialTaskCollection, ParallelTaskCollection

DEFAULT_REMOTE_OUTPUT_FOLDER="Results/"
DEFAULT_CLOUD_OUTPUT_FOLDER=":~/Results/"
CLOUDNAME = "ubuntu@172.23.72.113:~/Results/"

if __name__ == '__main__':
    from Matlabtasks import OomScript
    OomScript().run()


class OomScript(SessionBasedScript):
	"""
	Get tasks
	"""
	def __init__(self):
		super(OomScript, self).__init__(version='1.0')
		
	def setup_options(self):
		self.add_param("-d", "--mfuncs", metavar="PATH", type=str,
                       dest="matlab_source_folder", default=None,
                       help="Location of the Matlab scripts and "
                       "related Matlab functions. Default: %(default)s.")	

	def setup_args(self):
		self.add_param('matlab_function', type=str,
				help="Matlab function name")	

		self.add_param('csv_input_file', type=existing_file,
                help="Input .csv file with all parameters to be passed"
                " to the Matlab function.") 
                
	def _enumerate_csv(self, input_csv):
		"""
		For each line of the input .csv file
		return list of parameters 
		"""
		parameters = pandas.read_csv(input_csv)
		for i,p in enumerate(parameters.values):
			yield p.tolist()                           
	
	def new_tasks(self, extra):
		"""
		For each line of the input .csv file generate
		an execution Task
		"""
		tasks = []
		l = 0;
		for parameter in self._enumerate_csv(self.params.csv_input_file):
			parameter_str = '.'.join(str(x) for x in parameter)
			parlength=len(parameter)
			if not parlength==11:
				raise gc3libs.exceptions.InvalidUsage("Parameter length not correct" )
			l = l+1
			run = l
			jobname = "run%s" % str(l)
			extra_args = extra.copy()
			extra_args['jobname'] = jobname
			
			#Everything in results folder on remote computer
			extra_args['output_dir'] = CLOUDNAME #Not working
			#extra_args['output_dir'] = extra_args['output_dir'].replace('NAME', DEFAULT_REMOTE_OUTPUT_FOLDER) #save on local machine#
			extra_args['output_dir'] = "%s%s" % (extra_args['output_dir'], jobname)
			

			tasks.append(MatlabApp(self.params.matlab_function, parameter,self.params.matlab_source_folder,run,
			**extra_args))
		return [ParallelTaskCollection(tasks, **extra)]
		

			          
class MatlabApp(Application):
	"""Run a MATLAB source file."""
	application_name = 'matlab'

	def __init__(self, mfunc, parameter_list,matlabfolder, run, **extra_args):
		
	
		timefile = "%s%s%s" % ('timefile',str(run),'.txt')
		popfile = "%s%s%s" % ('popfile',str(run),'.txt')
		meanfile = "%s%s%s" % ('meanresults',str(run),'.txt')
		out = [meanfile,timefile, popfile]
		
		code_file_name = basename(mfunc)
		code_func_name = code_file_name[:-len('.m')]  # remove `.m` extension
		
		command = "%s%s%s%s%s%s" % (code_func_name,'(', ','.join(str(x) for x in parameter_list), ',', str(run), ')')  
		command2= "%s %s %s" % ("try", command, "; end, quit")
		
		argument1 = ["matlab", "-nodesktop", "-nojvm", "-r", command2]
		
		
		Application.__init__(
			self,
			arguments=argument1,
			inputs=[matlabfolder],
			outputs=out,
			stdout="matlab.log",
			join=True,
			**extra_args
			)

            
            
            
            
            
            
            
            