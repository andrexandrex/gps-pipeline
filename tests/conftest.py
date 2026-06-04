import sys
import os

# In Lambda, handler.py and common/ sit in the same directory.
# Locally, we replicate that by adding src/lambdas to sys.path so
# `from common.logger import ...` resolves the same way.
_LAMBDAS_DIR = os.path.join(os.path.dirname(__file__), "..", "src", "lambdas")
_SRC_DIR = os.path.join(os.path.dirname(__file__), "..", "src")

for _p in (_SRC_DIR, _LAMBDAS_DIR):
    _p = os.path.abspath(_p)
    if _p not in sys.path:
        sys.path.insert(0, _p)
