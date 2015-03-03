__author__ = 'JanTore'
from distutils.core import setup
import py2exe
import numpy
import mlpy
import scipy
import wordcloud
includes = [wordcloud]
setup(options = {
            "py2exe":{
                "dll_excludes": ["MSVCP90.dll", "HID.DLL", "w9xpopen.exe"]
        }
    }, zipfile = None, console=['sessionClustering.py'])

