import sys
import importlib

# Libraries to check
libraries = ['requests', 'websocket', 'dotenv', 'websockets']

def check_library(lib_name):
    try:
        if lib_name == 'dotenv':
            importlib.import_module('dotenv')
        else:
            importlib.import_module(lib_name)
        print(f"{lib_name}: ✓ Installed successfully")
        # Print version if possible
        try:
            module = __import__(lib_name)
            print(f"  Version: {module.__version__}")
        except (AttributeError, ImportError):
            pass
    except ImportError:
        print(f"{lib_name}: ✗ Not installed")

# Check each library
for library in libraries:
    check_library(library)

# Additional Python and path information
print("\nPython Information:")
print(f"Python Version: {sys.version}")
print("\nPython Path:")
for path in sys.path:
    print(path)