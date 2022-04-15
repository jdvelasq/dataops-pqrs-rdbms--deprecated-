import sys

import simulator

if __name__ == "__main__":
    if len(sys.argv) == 1:
        simulator.restart()
    else:
        simulator.restart(sys.argv[1])
