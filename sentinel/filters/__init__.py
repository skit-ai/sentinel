import os
from inspect import isclass
from pkgutil import iter_modules
from importlib import import_module

from sentinel.analyses.base import AnalysisBase


package_dir = os.path.dirname(os.path.realpath(__file__))

for _, module_name, _ in iter_modules([package_dir]):
    # import the module and iterate through its attributes
    module = import_module(f"{__name__}.{module_name}")

    for attribute_name in dir(module):
        attribute = getattr(module, attribute_name)

        if isclass(attribute) and issubclass(attribute, AnalysisBase):
            globals()[attribute_name] = attribute
