import itertools as itt
import matplotlib.pyplot as plt
from java_parse import *
import subprocess
import os

# """This is the command to make the java program run with the directoy it has to be run in (this won't get much simpler...): 

# C:\stuff\git\xxl_dk_fork>java -cp "C:\stuff\git\xxl_dk_fork\target\classes;C:\Users\hannoman\.m2\repository\com\google\uzaygezen\uzaygezen-core\0.2\uzaygezen-core-0.2.jar;C:\Users\hannoman\.m2\repository\org\apache\commons\commons-lang3\3.1\commons-lang3-3.1.jar;C:\Users\hannoman\.m2\repository\com\google\guava\guava\14.0-rc1\guava-14.0-rc1.jar;C:\Users\hannoman\.m2\repository\log4j\log4j\1.2.17\log4j-1.2.17.jar;C:\Users\hannoman\.m2\repository\org\apache\commons\commons-math3\3.6.1\commons-math3-3.6.1.jar" xxl.core.indexStructures.Test_TestableMap
# """

# get into right directory
execute_dir = r"""C:\stuff\git\xxl_dk_fork""" # @home
#execute_dir = r"""C:\Users\krappel.INFORMATIK\eclipse-git\xxl_dk_fork"""

execute_cmd = r"""java -cp "C:\stuff\git\xxl_dk_fork\target\classes;C:\Users\hannoman\.m2\repository\com\google\uzaygezen\uzaygezen-core\0.2\uzaygezen-core-0.2.jar;C:\Users\hannoman\.m2\repository\org\apache\commons\commons-lang3\3.1\commons-lang3-3.1.jar;C:\Users\hannoman\.m2\repository\com\google\guava\guava\14.0-rc1\guava-14.0-rc1.jar;C:\Users\hannoman\.m2\repository\log4j\log4j\1.2.17\log4j-1.2.17.jar;C:\Users\hannoman\.m2\repository\org\apache\commons\commons-math3\3.6.1\commons-math3-3.6.1.jar" xxl.core.indexStructures.Test_TestableMap""" # @home
#execute_cmd = r"""java -cp "C:\Users\krappel.INFORMATIK\eclipse-git\xxl_dk_fork\target\classes;C:\Users\krappel.INFORMATIK\.m2\repository\com\google\uzaygezen\uzaygezen-core\0.2\uzaygezen-core-0.2.jar;C:\Users\krappel.INFORMATIK\.m2\repository\org\apache\commons\commons-lang3\3.1\commons-lang3-3.1.jar;C:\Users\krappel.INFORMATIK\.m2\repository\com\google\guava\guava\14.0-rc1\guava-14.0-rc1.jar;C:\Users\krappel.INFORMATIK\.m2\repository\log4j\log4j\1.2.17\log4j-1.2.17.jar;C:\Users\krappel.INFORMATIK\.m2\repository\org\apache\commons\commons-math3\3.6.1\commons-math3-3.6.1.jar" xxl.core.indexStructures.Test_TestableMap"""

os.chdir(execute_dir)
print("changed to working directory: \t", os.getcwd())
print("Executing command: \"", execute_cmd, "\"", sep="")
print("=========== SUBPROCESS STARTED ===========")
#-- single execution variant
# res = subprocess.getoutput(execute_cmd)
# print("-- Result: ")
# print(res)
java_proc = subprocess.Popen(execute_cmd, stdout=subprocess.PIPE, universal_newlines=1, bufsize=1, shell=0)

res = ""
i = 0
try:
    while True:
    # for i in itt.count():
        pout, perr = java_proc.communicate()
        print(str(i) +"> "+ pout)
        res += pout
        i += 1
except ValueError:
    print("=========== SUBPROCESS FINISHED ===========")

print(res)
print("Reads done:", i)
rres = res.rstrip().split("\n")[-1]
# print("- data to parse:\n"+ rres)
    
# input("\n[Press Enter to continue.]")

#-- parsing of the got data
singleDictParser = mapParser(greedyNumberParser, listParser(greedyNumberParser))
pres = pairParser(singleDictParser, singleDictParser)(rres)[1]
tree_times, comp_times = pres
print("\n-- Tree times:")
print(tree_times)

print("\n-- Comparison times:")
print(comp_times)


#-- plotting

def plot_scatter_times(times,ax, **plotargs):
    times_flat = [(x,y) for x,ys in times.items() for y in ys]
    xs,ys = zip(*times_flat)
    ax.scatter(xs, ys, **plotargs)

fig = plt.figure()
ax = plt.gca()

plot_scatter_times(tree_times, ax, marker="o")
plot_scatter_times(comp_times, ax, marker="s")

plt.show()
plt.savefig('lastfig.pdf', format='pdf')
print("Last figure saved to: "+ str(os.getcwd()) +"\"lastfig.pdf\"")