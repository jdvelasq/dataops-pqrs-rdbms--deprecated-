import sys

import simulator

if __name__ == "__main__":
    if len(sys.argv) == 1:
        simulator.process_next_weeks(1)
    else:
        n_weeks = int(sys.argv[1])
        simulator.process_next_weeks(n_weeks)
