# -*- coding: utf-8 -*-
# Copyright (c) 2025 Jivesh Dixit, P.S. II, NCMRWF
# All rights reserved.
#
# This software is licensed under MIT license.
# Contact [jdixit@govcontractor.in].

import numpy as np
import xarray as xr
import dask.array as da
from dask.diagnostics import ProgressBar
from xeofs.single import EOF, EOFRotator
import scipy.signal as signal
import pandas as pd
from datetime import datetime, timedelta
import os
import sys


precip_clim = xr.open_dataset('precip_clim_hindcast_1993-2015_regrid.nc')['tot_precip'].sel(lat=slice(30.5, -12.5), lon=slice(60.5, 95.5)).mean('lon')


olr_clim = xr.open_dataset('climatology_olr_1x1.nc')['olr'].sortby('lat', ascending=False).sel(lat=slice(40.5, -30.5), lon=slice(30.5, 180.5)).squeeze()
n_days = olr_clim.sizes['time']

start_date = '2000-01-01' if n_days == 366 else '2001-01-01'

correct_times = pd.date_range(start=start_date, periods=n_days, freq='D')
olr_clim = olr_clim.assign_coords(time=('time', correct_times))

olr_clim_grouped = olr_clim.groupby('time.dayofyear').mean('time')

def prepare_data_for_eeof(data: xr.DataArray, tau: int, embedding: int, time_dim: str) -> xr.DataArray:
    """
    Prepares data for Extended EOF analysis by creating a time-lagged matrix.

    This function takes an input DataArray (e.g., time x latitude) and
    creates an expanded DataArray containing time-lagged copies, suitable
    as input for the core EEOF algorithm (which typically performs PCA/EOF
    on this expanded matrix).

    This mimics the data preparation part of the xeofs ExtendedEOF class's
    _fit_algorithm method (excluding the optional initial PCA step).

    Parameters:
        data (xr.DataArray): Input data, must have a time dimension.
                             Expected shape often (time, features), e.g., (time, lat).
        tau (int): Time lag step. The delay between consecutive lagged copies.
                   Must be non-negative.
        embedding (int): Embedding dimension. The total number of time-lagged
                         copies to include (including the original, lag=0).
                         Must be at least 1.
        time_dim (str): The name of the time dimension in the input DataArray.

    Returns:
        xr.DataArray: The time-lagged data array. Shape will be
                      (embedding, n_valid_samples, features), where
                      n_valid_samples = original_n_samples - (embedding - 1) * tau.
                      The 'embedding' dimension will have coordinates corresponding
                      to the applied lags [0, tau, 2*tau, ...].
                      Features dimension(s) (e.g., 'lat') are preserved.
    Raises:
        ValueError: If embedding < 1, tau < 0, or if trimming results in
                    an invalid number of samples.
    """
    if embedding < 1:
        raise ValueError("Embedding dimension must be at least 1.")
    if tau < 0:
        raise ValueError("Time lag tau cannot be negative.")
    if time_dim not in data.dims:
         raise ValueError(f"Time dimension '{time_dim}' not found in data dimensions: {data.dims}")

    n_times_original = data.sizes[time_dim]


    shifts = np.arange(embedding) * tau

    shifted_data_list = []
    for i in shifts:

        shifted_copy = data.shift({time_dim: -int(i)})
        shifted_data_list.append(shifted_copy)


    X_extended = xr.concat(shifted_data_list, dim="embedding")


    X_extended = X_extended.assign_coords({"embedding": shifts})


    n_samples_cut = (embedding - 1) * tau

    if n_samples_cut > 0:

        if n_samples_cut >= n_times_original:
             raise ValueError(
                 f"Cannot cut {n_samples_cut} samples from data with only "
                 f"{n_times_original} time steps. "
                 f"Resulting number of samples would be non-positive. "
                 f"Check tau ({tau}) and embedding ({embedding})."
             )

        X_trimmed = X_extended.isel({time_dim: slice(None, -n_samples_cut)})
    elif n_samples_cut == 0:

         X_trimmed = X_extended
    else:
         raise ValueError("Internal error: Invalid n_samples_cut calculation.")


    return X_trimmed




def get_latest_thursday(input_date):
    # Parse the input date in YYYYMMDD format
    today = datetime.strptime(input_date, '%Y%m%d')
    
    if today.weekday() == 3:  # Thursday
        latest_thursday = today
    else:
        days_since_thursday = (today.weekday() - 3) % 7
        latest_thursday = today - timedelta(days=days_since_thursday)
    
    return latest_thursday

if len(sys.argv) > 1:
    input_date = sys.argv[1]
else:
    input_date = datetime.now().strftime('%Y%m%d')

latest_thursday_date = get_latest_thursday(input_date)

forecast_date = latest_thursday_date.strftime("%Y%m%dT0000Z")



initial = [(datetime.strptime(forecast_date, "%Y%m%dT%H%MZ") - timedelta(days=i)).strftime("%Y%m%dT0000Z") for i in range(4, 0, -1)]
members = ['mem1', 'mem2', 'mem3', 'mem4']

precip_forecast_anom = {}


for num, ini in enumerate(initial):
    precip_forecast_anom[ini] = {}
    initial_date = datetime.strptime(ini, "%Y%m%dT%H%MZ").strftime("%Y%m%d")
    end_date     = (datetime.strptime(ini, "%Y%m%dT%H%MZ") + timedelta(days=18)).strftime("%Y%m%d")
    for member in members:
        forecast_path = os.path.join(os.path.expanduser("~"), "forecast/prediction", f'{ini}/{member}/1/concatenated_{initial_date}_{end_date}_{member}.nc')

        precip_forecast = xr.open_dataset(forecast_path)['tot_precip']\
            .rename({'t':'time', 'latitude':'lat', 'longitude':'lon'}).drop_vars(['surface'], errors='ignore')\
            .sel(time=slice((datetime.strptime(ini, "%Y%m%dT%H%MZ") + timedelta(days=4-num)).strftime("%Y%m%d"), \
            (datetime.strptime(ini, "%Y%m%dT%H%MZ") + timedelta(days=35-num)).strftime("%Y%m%d"))).mean(('lon')).squeeze()
        # precip_forecast = xr.where(precip_forecast<0, 0, precip_forecast)
        day_of_year = precip_forecast['time'].dt.dayofyear
        precip_clim = precip_clim.reindex(lat=precip_forecast.lat, method='nearest')
        precip_forecast_anom[ini][member] = precip_forecast - precip_clim.sel(dayofyear=day_of_year)
        print(precip_forecast_anom[ini][member].isnull().any())



print ('1\n\n\n')




olr_forecast_anom = {}


for num, ini in enumerate(initial):
    olr_forecast_anom[ini] = {}
    initial_date = datetime.strptime(ini, "%Y%m%dT%H%MZ").strftime("%Y%m%d")
    end_date     = (datetime.strptime(ini, "%Y%m%dT%H%MZ") + timedelta(days=18)).strftime("%Y%m%d")
    for member in members:
        forecast_path = os.path.join(os.path.expanduser("~"), "forecast/prediction", f'{ini}/{member}/1/concatenated_{initial_date}_{end_date}_{member}_OLR.nc')

        olr_forecast = xr.open_dataset(forecast_path)['olr']\
            .rename({'t':'time', 'latitude':'lat', 'longitude':'lon'}).drop_vars(['surface'], errors='ignore')\
            .sel(time=slice((datetime.strptime(ini, "%Y%m%dT%H%MZ") + timedelta(days=4-num)).strftime("%Y%m%d"), \
            (datetime.strptime(ini, "%Y%m%dT%H%MZ") + timedelta(days=35-num)).strftime("%Y%m%d"))).mean(('lon')).squeeze()
        # precip_forecast = xr.where(precip_forecast<0, 0, precip_forecast)
        day_of_year = olr_forecast['time'].dt.dayofyear
        olr_clim = olr_clim_grouped.reindex(lat=olr_forecast.lat, method='nearest')
        olr_forecast_anom[ini][member] = olr_forecast - olr_clim_grouped.sel(dayofyear=day_of_year)
        print(olr_forecast_anom[ini][member].isnull().any())


initial_date = (datetime.strptime(forecast_date, "%Y%m%dT%H%MZ") + timedelta(days=-16)).strftime("%Y%m%d")
end_date     = (datetime.strptime(forecast_date, "%Y%m%dT%H%MZ") + timedelta(days=-1)).strftime("%Y%m%d")

precip_lag_1 =  xr.open_dataset(f'avg_precip_analysis_output/prate_daily_avg_{initial_date}_to_{end_date}_regrid.nc')['PRATE_surface'].sel(lat=slice(30.5, -12.5), lon=slice(60.5, 95.5)).mean(('lon'))[2:]

precip_lag_1 = precip_lag_1.reindex(lat=precip_clim.lat, method='nearest')
doy = precip_lag_1['time'].dt.dayofyear
precip_lag = precip_lag_1 - precip_clim.sel(dayofyear=doy)


eeofs = xr.open_dataset('EEOFS_MISO_1997_2016_GPCP_v1.3.nc')['miso_eeofs']

eeofs = eeofs.reindex(lat=precip_clim.lat, method='nearest')

miso_scores_std = xr.open_dataset('Obs_MISO_scores_std_JJAS.nc')["miso_scores_std"]


print ('2\n\n\n')

precip_concat = {}

precip_miso_lagged = {}
miso1_score = {}
miso2_score = {}
for ini in initial:
    precip_concat[ini] = {}
    precip_miso_lagged[ini] = {}
    miso1_score[ini] = {}
    miso2_score[ini] = {}
    for member in members:
        precip_concat[ini][member] = xr.concat((precip_lag, precip_forecast_anom[ini][member]), dim='time')
        print(precip_concat[ini][member].isnull().any())
        precip_miso_lagged[ini][member] = prepare_data_for_eeof(precip_concat[ini][member], tau = 1, embedding = 15, time_dim = 'time')
        # print(precip_miso_lagged[ini][member].isnull().sum()) 
        miso_scores = (eeofs*precip_miso_lagged[ini][member]).sum(dim=['embedding', 'lat'])/miso_scores_std
        # miso1_score.time = precip_forecast_anom[ini][member].time
        miso_scores = miso_scores.assign_coords(time=precip_forecast_anom[ini][member].time)
        miso1_score[ini][member] = miso_scores[0]
        miso2_score[ini][member] = miso_scores[1]
        miso1_score[ini][member].name = f'MISO1_{ini[:8]}_{member}'
        miso2_score[ini][member].name = f'MISO2_{ini[:8]}_{member}'


combined_miso1 = xr.Dataset(
    {f'MISO1_{ini}_{member}': miso1_score[ini][member].drop_vars('dayofyear', errors='ignore') for ini in miso1_score for member in miso1_score[ini]}
)
combined_miso2 = xr.Dataset(
    {f'MISO2_{ini}_{member}': miso2_score[ini][member].drop_vars('dayofyear', errors='ignore') for ini in miso2_score for member in miso2_score[ini]}
)



directory = os.path.join(os.path.expanduser("~"), "forecast", "MISOs")

if not os.path.exists(directory):
    os.makedirs(directory)

file_path_1 = os.path.join(directory, f'MISO1_CNCUM_IC_{initial[0][:8]}-{initial[-1][:8]}_FC_{forecast_date[:8]}.nc')
file_path_2 = os.path.join(directory, f'MISO2_CNCUM_IC_{initial[0][:8]}-{initial[-1][:8]}_FC_{forecast_date[:8]}.nc')



if os.path.exists(file_path_1):
    os.remove(file_path_1)
if os.path.exists(file_path_2):
    os.remove(file_path_2)






combined_miso1.to_netcdf(file_path_1, mode='w')
combined_miso2.to_netcdf(file_path_2, mode='w')