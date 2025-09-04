#!/bin/bash

# Copyright (c) 2025 Jivesh Dixit, P.S. II, NCMRWF
# All rights reserved.
#
# This software is licensed under MIT license.
# Contact [jdixit@govcontractor.in].

export PATH="/home/jdixit/anaconda3/envs/xIndices/bin/:$PATH"
module load gnu/nco/5.0.4_with_udunit

scripts=(
     "analysis_extract_precipitation_input_date_all_days_parallel.sh"
     "convert_to_nc_extract_both_forecast.sh"
     "MISO_calculations.py"
     "Plotting_MISO_rotated_unfiltered_new.py"
)


if [ -z "$1" ]; then
    today=$(date +%Y%m%d)
else
    today="$1"
fi


execute_script() {
    script="$1"
    if [[ -f "$script" ]]; then
        echo "Executing $script with date: $today..."
        if [[ "$script" == *.sh ]]; then
            bash "$script" "$today"  # Pass date to shell scripts
        elif [[ "$script" == *.py ]]; then
            python "$script" "$today"  # Pass date to Python scripts
        else
            echo "Unsupported file type: $script"
        fi
    else
        echo "File not found: $script"
    fi
}


for script in "${scripts[@]}"; do
    execute_script "$script"
done

echo "All specified scripts executed."
