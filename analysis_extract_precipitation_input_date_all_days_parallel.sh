#!/bin/bash -f

# Copyright (c) 2025 Jivesh Dixit, P.S. II, NCMRWF
# All rights reserved.
#
# This software is licensed under MIT license.
# Contact [jdixit@govcontractor.in].


set -euo pipefail

ddr="/home/umfcst/NCUM_output/post_0.12x0.18"
output_dir=$(readlink -f "avg_precip_analysis_output")
DAYS_TO_PROCESS=15

mkdir -p "$output_dir"
rm -f error.log

if [[ -n "${1-}" ]]; then
    reference_date="$1"
else
    reference_date=$(date +%Y%m%d)
fi
reference_weekday=$(date -d "$reference_date" +%u)
days_ago=$(( (reference_weekday - 3 + 7) % 7 ))
latest_wednesday=$(date -d "$reference_date - $days_ago days" +%Y%m%d)

end_dates=(
    "$latest_wednesday"
)

process_date_range() {
    local END_DATE="$1"
    local job_id="[Job for ${END_DATE}]"
    echo "$job_id Starting process."

    local job_temp_dir
    job_temp_dir=$(mktemp -d "parallel_job_${END_DATE}_XXXXXX")
    trap 'rm -rf "$job_temp_dir"' RETURN

    local START_DATE
    START_DATE=$(date -d "${END_DATE} -${DAYS_TO_PROCESS} days" +%Y%m%d)
    echo "$job_id Processing data from $START_DATE to $END_DATE"

    local daily_files_to_merge=()

    local current_date
    current_date=$(date -d "$START_DATE" +%Y%m%d)

    while [[ "$current_date" -le "$END_DATE" ]]; do
        local istmp="$current_date"
        local hourly_files_for_this_day=()
        echo "$job_id   - Preparing daily average for date: $istmp"

        for ih in 00 06 12 18; do
            local file="${ddr}/${istmp}/um_ana_0${ih}hr_${istmp}_${ih}Z.grib2"
            local out_nc="${job_temp_dir}/temp_hourly_${istmp}${ih}.nc"

            if [[ -f "$file" ]]; then
                local msgnum
                msgnum=$(wgrib2 "$file" -s | grep -i ':prate:' | cut -d: -f1 | head -n 1)
                if [[ -n "$msgnum" ]]; then
                    wgrib2 "$file" -d "$msgnum" -netcdf "$out_nc" 2>>"${output_dir}/error.log"
                    if [[ -f "$out_nc" ]]; then
                        hourly_files_for_this_day+=("$out_nc")
                    fi
                fi
            fi
        done

        if [[ ${#hourly_files_for_this_day[@]} -ge 2 ]]; then
            local daily_avg="${job_temp_dir}/daily_avg_${istmp}.nc"
            # cdo -O ensmean "${hourly_files_for_this_day[@]}" "$daily_avg"
            cdo -O -mulc,86400 -ensmean "${hourly_files_for_this_day[@]}" "$daily_avg"
            daily_files_to_merge+=("$daily_avg")
        fi

        current_date=$(date -d "$current_date +1 day" +%Y%m%d)
    done

    if [[ ${#daily_files_to_merge[@]} -gt 0 ]]; then
        local final_out="${output_dir}/prate_daily_avg_${START_DATE}_to_${END_DATE}.nc"
        local regridded_out="${output_dir}/prate_daily_avg_${START_DATE}_to_${END_DATE}_regrid.nc"

        echo "$job_id Merging daily averages to create: $final_out"
        cdo -O mergetime "${daily_files_to_merge[@]}" "$final_out"

        echo "$job_id Regridding final output to create: $regridded_out"
        cdo -O remapcon,mygrid "$final_out" "$regridded_out"

        echo "$job_id Final regridded output created: $regridded_out"
    else
        echo "$job_id No data found for this period. No final file generated."
    fi

    echo "$job_id Process finished."
}


export -f process_date_range
export ddr output_dir DAYS_TO_PROCESS

echo "Target dates: ${end_dates[*]}"


for date_to_process in "${end_dates[@]}"; do
    process_date_range "$date_to_process" &
done


