"""
Lightweight stub of the Great Expectations package.

The real library is not bundled in this offline environment, so we provide a
minimal surface area to support expectation authoring in ``app.ge_expectations``
and the accompanying tests. When the actual dependency is installed, its module
resolution will take precedence on the import path.
"""

from .dataset import PandasDataset  # noqa: F401

__version__ = "0.0.0-stub"
