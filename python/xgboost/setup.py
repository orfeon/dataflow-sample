from distutils.command.build import build as _build
import subprocess
import setuptools


class build(_build):
    sub_commands = _build.sub_commands + [('CustomCommands', None)]

CUSTOM_COMMANDS = [(["sudo","apt-get","update"],"."),
                   (["sudo","apt-get","install","git","build-essential","libatlas-base-dev","-y"],"."), #"python-dev","gfortran"
                   (["git","clone","--recursive","https://github.com/dmlc/xgboost"],"."),
                   (["sudo","make"],"xgboost"),
                   (["sudo","python","setup.py","install"],"xgboost/python-package"),
                   (["sudo","pip","install","xgboost"],".")]


class CustomCommands(setuptools.Command):

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def RunCustomCommand(self, command_list):
        print 'Running command: %s' % command_list[0]
        p = subprocess.Popen(command_list[0], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=command_list[1])
        stdout_data, _ = p.communicate()
        print 'Command output: %s' % stdout_data
        if p.returncode != 0:
            raise RuntimeError('Command %s failed: exit code: %s' % (command_list[0], p.returncode))

    def run(self):
        for command in CUSTOM_COMMANDS:
            self.RunCustomCommand(command)

REQUIRED_PACKAGES = []


setuptools.setup(
    name='xgboost',
    version='0.0.1',
    description='xgboost workflow packages.',
    packages=setuptools.find_packages(),
    cmdclass={
        'build': build,
        'CustomCommands': CustomCommands,
        }
    )
