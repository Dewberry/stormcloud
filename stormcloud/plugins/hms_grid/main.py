"""
Main script for process api plugin for HMS GRID file creation
"""

import sys

from papipyplug import parse_input, plugin_logger, print_results

from hms_grid_plugin import PLUGIN_PARAMS, main

if __name__ == "__main__":
    # Start plugin logger
    plugin_logger()

    # Read, parse, and verify input parameters
    input_params = parse_input(sys.argv, PLUGIN_PARAMS)

    # Add main function here
    results = main(input_params)

    # Print Results
    print_results(results)
