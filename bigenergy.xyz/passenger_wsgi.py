import sys, os
INTERP = os.path.join(os.environ['HOME'], 'bigenergy.xyz', 'bin', 'python')
if sys.executable != INTERP:
    os.execl(INTERP, INTERP, *sys.argv)
sys.path.append(os.getcwd())


sys.path.append('productionApp')
from productionApp.app import app as application
