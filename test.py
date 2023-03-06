import os
import sys
print("hellow")
os.execv(sys.executable, ['python'] + sys.argv)