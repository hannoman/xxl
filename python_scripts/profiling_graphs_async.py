import itertools as itt
import matplotlib.pyplot as plt
from java_parse import *
import subprocess
import os
import asyncio.subprocess
import sys


# """This is the command to make the java program run with the directoy it has to be run in (this won't get much simpler...): 

# C:\stuff\git\xxl_dk_fork>java -cp "C:\stuff\git\xxl_dk_fork\target\classes;C:\Users\hannoman\.m2\repository\com\google\uzaygezen\uzaygezen-core\0.2\uzaygezen-core-0.2.jar;C:\Users\hannoman\.m2\repository\org\apache\commons\commons-lang3\3.1\commons-lang3-3.1.jar;C:\Users\hannoman\.m2\repository\com\google\guava\guava\14.0-rc1\guava-14.0-rc1.jar;C:\Users\hannoman\.m2\repository\log4j\log4j\1.2.17\log4j-1.2.17.jar;C:\Users\hannoman\.m2\repository\org\apache\commons\commons-math3\3.6.1\commons-math3-3.6.1.jar" xxl.core.indexStructures.Test_TestableMap
# """

# get into right directory
execute_dir = r"""C:\stuff\git\xxl_dk_fork"""
execute_cmd = r"""java -cp "C:\stuff\git\xxl_dk_fork\target\classes;C:\Users\hannoman\.m2\repository\com\google\uzaygezen\uzaygezen-core\0.2\uzaygezen-core-0.2.jar;C:\Users\hannoman\.m2\repository\org\apache\commons\commons-lang3\3.1\commons-lang3-3.1.jar;C:\Users\hannoman\.m2\repository\com\google\guava\guava\14.0-rc1\guava-14.0-rc1.jar;C:\Users\hannoman\.m2\repository\log4j\log4j\1.2.17\log4j-1.2.17.jar;C:\Users\hannoman\.m2\repository\org\apache\commons\commons-math3\3.6.1\commons-math3-3.6.1.jar" xxl.core.indexStructures.Test_TestableMap"""

os.chdir(execute_dir)
print("changed to working directory: \t", os.getcwd())
print("Executing command: \"", execute_cmd, "\"", sep="")
print("=========== SUBPROCESS STARTED ===========")
#-- single execution variant
# res = subprocess.getoutput(execute_cmd)
# print("-- Result: ")
# print(res)

# java_proc = subprocess.Popen(execute_cmd, stdout=subprocess.PIPE, universal_newlines=1, bufsize=1, shell=1)

# res = ""
# i = 0
# try:
    # while True:
    # # for i in itt.count():
        # pout, perr = java_proc.communicate()
        # print(str(i) +"> "+ pout)
        # res += pout
        # i += 1
# except ValueError:
    # print("=========== SUBPROCESS FINISHED ===========")

# print("Reads done:", i)
# rres = res.rstrip().split("\n")[-1]
# # print("- data to parse:\n"+ rres)
    
# # input("\n[Press Enter to continue.]")


#--- async

create = asyncio.create_subprocess_exec(execute_cmd, stdout=asyncio.subprocess.PIPE)
# @asyncio.coroutine
# def get_date():
    # # Create the subprocess, redirect the standard output into a pipe
    # create = asyncio.create_subprocess_exec(execute_cmd, stdout=asyncio.subprocess.PIPE)
    # proc = yield from create

    # # Read one line of output
    # data = yield from proc.stdout.readline()
    # line = data.decode('ascii').rstrip()

    # # Wait for the subprocess exit
    # yield from proc.wait()
    # return line

# if sys.platform == "win32":
    # loop = asyncio.ProactorEventLoop()
    # asyncio.set_event_loop(loop)
# else:
    # loop = asyncio.get_event_loop()

# rline = loop.run_until_complete(get_date())
# print("Line from output:", rline)
# loop.close()


#-- parsing of the retrieved data
# singleDictParser = mapParser(greedyNumberParser, listParser(greedyNumberParser))
# pres = pairParser(singleDictParser, singleDictParser)(rres)[1]
# tree_times, comp_times = pres
# print("\n-- Tree times:")
# print(tree_times)

# print("\n-- Comparison times:")
# print(comp_times)


#-- plotting
