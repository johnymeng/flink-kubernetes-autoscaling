#!/usr/bin/env python3
"""Plot benchmark results for this experiment."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from plot_run import main


if __name__ == "__main__":
    main(default_query="q9")
