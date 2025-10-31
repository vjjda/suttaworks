# Path: src/db_updater/post_tasks/parallels/__init__.py
import pkgutil
import importlib

# Dynamically import all modules in this package
__all__ = []
for loader, module_name, is_pkg in pkgutil.walk_packages(__path__):
    if not is_pkg:
        module = importlib.import_module(f".{module_name}", __package__)
        if hasattr(module, "__all__"):
            # Import all from the module and add to package's __all__
            globals().update({name: getattr(module, name) for name in module.__all__})
            __all__.extend(module.__all__)
