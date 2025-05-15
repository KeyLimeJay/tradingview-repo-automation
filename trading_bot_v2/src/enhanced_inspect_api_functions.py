#!/usr/bin/env python3

import inspect
import sys
import os
import json
import datetime
import textwrap
import re
from functools import partial

# Adjust the path to where your trading bot code is located
sys.path.append('/opt/otcxn/tradingview-repo-automation/trading_bot_v2')

# Configuration
OUTPUT_FILE = f"api_functions_analysis_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
SAVE_TO_FILE = True  # Set to False to only print to console

def write_output(text, file=None):
    """Write text to both console and file if specified"""
    print(text)
    if file:
        file.write(text + "\n")

def analyze_function(func, output_writer):
    """Analyze a function and print detailed information about it"""
    # Get basic info
    output_writer(f"\n{'=' * 50}")
    output_writer(f"FUNCTION: {func.__name__}")
    output_writer(f"{'=' * 50}")
    
    # Get parameters
    sig = inspect.signature(func)
    params = sig.parameters
    output_writer(f"\nPARAMETERS ({len(params)}):")
    for name, param in params.items():
        default = f" = {param.default}" if param.default is not param.empty else ""
        annotation = f": {param.annotation}" if param.annotation is not param.empty else ""
        output_writer(f"  - {name}{annotation}{default}")
    
    # Get docstring and parse it
    doc = inspect.getdoc(func)
    output_writer(f"\nDOCSTRING:")
    if doc:
        # Parse docstring to extract parameter descriptions
        output_writer(textwrap.indent(doc, "  "))
        
        # Try to extract parameter descriptions from docstring
        param_pattern = r"(?:Args|Parameters):(.*?)(?:\n\n|\n[A-Z]|$)"
        param_match = re.search(param_pattern, doc, re.DOTALL)
        if param_match:
            param_section = param_match.group(1)
            param_desc_pattern = r"\s+(\w+):\s*(.*?)(?=\n\s+\w+:|\n\n|\n[A-Z]|$)"
            param_descs = re.findall(param_desc_pattern, param_section, re.DOTALL)
            
            if param_descs:
                output_writer("\nPARAMETER DESCRIPTIONS:")
                for name, desc in param_descs:
                    output_writer(f"  - {name}: {desc.strip()}")
    else:
        output_writer("  No docstring available")
    
    # Check for decorators
    source_lines = inspect.getsourcelines(func)[0]
    decorator_lines = []
    for line in source_lines:
        if line.strip().startswith('@'):
            decorator_lines.append(line.strip())
        elif not line.strip().startswith('def '):
            continue
        else:
            break
    
    if decorator_lines:
        output_writer("\nDECORATORS:")
        for dec in decorator_lines:
            output_writer(f"  {dec}")
    
    # Try to find calls to other functions within this function
    source = inspect.getsource(func)
    # This is a simple regex-based approach that might miss some calls or include false positives
    function_calls = re.findall(r'(\w+)\(', source)
    # Remove duplicates and filter out common Python functions
    function_calls = set(function_calls) - {'if', 'for', 'print', 'str', 'int', 'float', 'len', 'dict', 'list', 'set', 'tuple', 'any', 'all'}
    
    if function_calls:
        output_writer("\nPOTENTIAL FUNCTION CALLS:")
        for call in sorted(function_calls):
            output_writer(f"  - {call}()")
    
    # Source code
    output_writer("\nSOURCE CODE:")
    output_writer(textwrap.indent(inspect.getsource(func), "  "))

def find_dependent_functions(module, func_name, explored=None):
    """Recursively find functions that are called by the given function"""
    if explored is None:
        explored = set()
    
    if func_name in explored:
        return set()
    
    explored.add(func_name)
    
    # Get the function object
    try:
        func = getattr(module, func_name)
    except (AttributeError, TypeError):
        return set()
    
    # Get source code
    try:
        source = inspect.getsource(func)
    except (TypeError, OSError):
        return set()
    
    # Find function calls
    function_calls = set(re.findall(r'(\w+)\(', source))
    
    # Filter known Python functions and already explored
    ignore_list = {'if', 'for', 'print', 'str', 'int', 'float', 'len', 'dict', 'list', 
                  'set', 'tuple', 'any', 'all', 'super', 'min', 'max', 'sum', 'range',
                  'map', 'filter', 'sorted', 'enumerate', 'zip', 'isinstance', 'type'}
    function_calls = function_calls - ignore_list - explored
    
    # Add dependencies recursively
    deps = set(function_calls)
    for call in function_calls:
        deps.update(find_dependent_functions(module, call, explored))
    
    return deps

try:
    output_file = None
    if SAVE_TO_FILE:
        output_file = open(OUTPUT_FILE, 'w')
        write = partial(write_output, file=output_file)
    else:
        write = print
    
    write(f"API FUNCTIONS ANALYSIS - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    write(f"{'=' * 80}")
    
    # Import the module
    import src.trading_utils as trading_utils
    from src.trading_utils import place_order, place_repo_order
    
    # List all function names in the module for reference
    all_functions = [name for name, obj in inspect.getmembers(trading_utils, inspect.isfunction)]
    write(f"\nAll functions in trading_utils module ({len(all_functions)}):")
    for i, name in enumerate(sorted(all_functions)):
        write(f"  {i+1:2d}. {name}")

    # Check if the functions have been wrapped
    write("\nChecking for function wrapping...")
    if hasattr(place_order, '__wrapped__'):
        write("  place_order is wrapped with a decorator")
    else:
        write("  place_order is not wrapped")
        
    if hasattr(place_repo_order, '__wrapped__'):
        write("  place_repo_order is wrapped with a decorator")
    else:
        write("  place_repo_order is not wrapped")
    
    # Analyze main API functions
    analyze_function(place_order, write)
    analyze_function(place_repo_order, write)
    
    # Find functions that place_order depends on
    write("\n\nDEPENDENCY ANALYSIS")
    write("=" * 50)
    
    write("\nplace_order dependencies:")
    dependencies = find_dependent_functions(trading_utils, "place_order")
    for dep in sorted(dependencies):
        write(f"  - {dep}()")
        
    write("\nplace_repo_order dependencies:")
    dependencies = find_dependent_functions(trading_utils, "place_repo_order")
    for dep in sorted(dependencies):
        write(f"  - {dep}()")
    
    # Find and analyze all functions related to orders, repos, or API
    write("\n\nRELATED FUNCTIONS ANALYSIS")
    write("=" * 50)
    
    relevant_functions = []
    for name, func in inspect.getmembers(trading_utils, inspect.isfunction):
        if "order" in name.lower() or "repo" in name.lower() or "api" in name.lower():
            relevant_functions.append((name, func))
    
    write(f"\nFound {len(relevant_functions)} relevant functions:")
    for name, _ in relevant_functions:
        write(f"  - {name}")
    
    # Analyze each relevant function
    for name, func in relevant_functions:
        analyze_function(func, write)
    
    # Check for global variables that might affect API behavior
    write("\n\nGLOBAL VARIABLES")
    write("=" * 50)
    
    global_vars = []
    for name, value in inspect.getmembers(trading_utils):
        if not name.startswith('__') and not inspect.isfunction(value) and not inspect.isclass(value) and not inspect.ismodule(value):
            global_vars.append((name, value))
    
    if global_vars:
        write(f"\nFound {len(global_vars)} global variables that might affect API behavior:")
        for name, value in global_vars:
            value_str = str(value)
            if len(value_str) > 100:
                value_str = value_str[:97] + "..."
            write(f"  - {name} = {value_str}")
    else:
        write("\nNo relevant global variables found")
    
    write("\n\nANALYSIS COMPLETE!")
    
except Exception as e:
    error_msg = f"Error: {str(e)}"
    print(error_msg)
    if 'write' in locals():
        write(f"\n{error_msg}")
    import traceback
    traceback.print_exc()

finally:
    if output_file:
        output_file.close()
        print(f"\nAnalysis saved to {OUTPUT_FILE}")