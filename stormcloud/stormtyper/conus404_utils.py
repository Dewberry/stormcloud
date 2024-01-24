import logging

# import wrf
import xarray as xr
from scipy.interpolate import interp1d


def destagger_grid(data_variable: xr.DataArray, dim: int = 1) -> xr.DataArray:
    """
    Destagger a given data variable along a specified dimension.
    More on staggered grids here: https://amps-backup.ucar.edu/information/configuration/wrf_grid_structure.html

    Args:
    data_variable (xr.DataArray): The data variable to be destaggered.
    dim (int): The dimension index to destagger along. Default is 1.

    Returns:
    xr.DataArray: The destaggered data variable.
    """
    logging.debug("Destaggering Z grid")
    unstag_var = wrf.destagger(data_variable, dim, meta=True)
    return unstag_var


def interp_pressure_level(
    pressure: xr.DataArray, height: xr.DataArray, target_pressure: float
) -> float:
    """
    Interpolate height at a given pressure level using linear interpolation.

    Args:
    pressure (xr.DataArray): Array of pressure values.
    height (xr.DataArray): Array of height values corresponding to the pressure values.
    target_pressure (float): The pressure level at which height is to be interpolated.

    Returns:
    float: The interpolated height at target pressure.
    """
    interp_func = interp1d(pressure, height, bounds_error=False)
    return interp_func(target_pressure)


def calc_geopot_at_press_lvl(
    pressure_da: xr.DataArray, z_unstag_da: xr.DataArray, pressure_level: float
) -> xr.DataArray:
    """
    Calculate array of geopotential height at a given pressure level. Utilizes xarray's apply_ufunc to apply a custom interpolation function
    across the entire pressure and height dataarrays.

    Args:
    pressure_da (xr.DataArray): DataArray of pressure values.
    z_unstag_da (xr.DataArray): DataArray of unstaggered geopotential heights.
    pressure_level (float): The target pressure level for calculating geopotential height.

    Returns:
    xr.DataArray: The calculated geopotential height at the specified pressure level.
    """
    calc_z_at_press_level = xr.apply_ufunc(
        interp_pressure_level,
        pressure_da,
        z_unstag_da,
        input_core_dims=[["bottom_top"], ["bottom_top"]],
        kwargs={"target_pressure": pressure_level},
        vectorize=True,  # Enable vectorized execution
        output_dtypes=[float],
    )
    return calc_z_at_press_level


def calculate_slp(
    surface_pressure,
    altitude,
    sea_level_temp=288.15,
    lapse_rate=0.0065,
    gravity=9.80665,
    gas_constant=287,
):
    """
    Calculate sea level pressure from surface pressure for arrays of data.
    Surface pressure must be in units hPa and altitude must be in meters.

    Args:
    - surface_pressure: array of surface pressure (in hPa)
    - altitude: array of altitude at which the pressure is measured (in meters)
    - sea_level_temp: standard temperature at sea level in kelvin (default 288.15 K)
    - lapse_rate: standard temperature lapse rate (default 0.0065 K/m)
    - gravity: acceleration due to gravity (default 9.80665 m/s^2)
    - gas_constant: specific gas constant for dry air (default 287 J/kg/K)

    Returns:
    - Array of sea level pressure
    """
    temp_at_altitude = sea_level_temp - lapse_rate * altitude
    slp = surface_pressure * (sea_level_temp / temp_at_altitude) ** (
        gravity / (lapse_rate * gas_constant)
    )
    return slp
