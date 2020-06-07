import pandas as pd
import numpy as np


def find_closest_density(
        row: pd.Series,
        popul_dens_df: pd.DataFrame,
        delta: float = 0.01,
) -> float:
    """
    This function is intended for using with the pd.DataFrame.apply(..., axis=1) method.
    It takes in one row from the otodom_scraping_df and the whole popul_dens_df frame,
    which contains the ['lon', 'lat', 'density'] columns.

    For that row, the function returns population density in a point that's closest to that row.

    If there's no density in its neighborhood (controlled by the "delta" argument), it returns -1
    to signify that no density was found near by.
    """
    lon, lat = row[['lon', 'lat']]
    lon_interval = (popul_dens_df['lon'] > lon - delta) & (popul_dens_df['lon'] < lon + delta)
    lat_interval = (popul_dens_df['lat'] > lat - delta) & (popul_dens_df['lat'] < lat + delta)
    candidates = popul_dens_df[lon_interval & lat_interval]
    if candidates.shape[0] == 0:
        return -1

    diffs = candidates[['lon', 'lat']].values[:, None] - row[['lon', 'lat']].values[None, :]
    closest_point = np.argmin(pd.np.apply_along_axis(np.linalg.norm, axis=2, arr=diffs))
    return candidates.iloc[closest_point]['density']