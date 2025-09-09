import xarray as xr
from xeofs.single import ExtendedEOF as EEOF
from xIndices.preprocess_data import rename_dims_to_standard
import matplotlib.pyplot as plt
import xfilter
import numpy as np
import os





dset = xr.open_dataset('gpcp_1997-2016_combined.nc') ### GPCP data

precip = rename_dims_to_standard(dset['precip'].sel(time=slice('1997-01-01', '2016-12-31')))


precip_dep = precip.groupby('time.dayofyear') - precip.groupby('time.dayofyear').mean(('time'))

jjas = precip_dep.time.dt.month.isin(range(6,10))



precip_dep_jjas = precip_dep.sel(time=jjas)

precip_dep_miso = precip_dep_jjas.sel(lat=slice(-12.5, 30.5), lon=slice(60.5, 95.5)).mean(('lon'))

model = EEOF(tau=1, embedding=15, n_modes=2, use_coslat=True)

model.fit(precip_dep_miso, dim=("time"))

eeofs = model.components()

plt.figure(figsize=(12,4), dpi=300)

ax = plt.axes()

eeofs[1, 14].plot(label='mode2', color='k', lw=2, ls='--', ax=ax)
(-eeofs[0, 14]).plot(label='mode1', color='k', lw=2,ax=ax)
plt.legend()
plt.savefig('eeofs_miso_gpcp_1997-2016.png', bbox_inches='tight')
plt.clf()

exp_var = model.explained_variance_ratio()