import pathlib
import sys

# Ensure project root is importable as a module namespace
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))