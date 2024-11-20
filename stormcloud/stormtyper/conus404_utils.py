import logging

import xarray as xr
import numpy as np
from scipy.interpolate import interp1d


# Copied over from wrf-python to avoid dependency
def destagger(var, stagger_dim: int = 1, meta=False):
    """Return the variable on the unstaggered grid.

    This function destaggers the variable by taking the average of the
    values located on either side of the grid box.

    Args:

        var (:class:`xarray.DataArray` or :class:`numpy.ndarray`): A variable
            on a staggered grid.

        stagger_dim (:obj:`int`): The dimension index to destagger.
            Negative values can be used to choose dimensions referenced
            from the right hand side (-1 is the rightmost dimension).

        meta (:obj:`bool`, optional): Set to False to disable metadata and
            return :class:`numpy.ndarray` instead of
            :class:`xarray.DataArray`.  Default is False.

    Returns:

        :class:`xarray.DataArray` or :class:`numpy.ndarray`:
        The destaggered variable.  If xarray is enabled and
        the *meta* parameter is True, then the result will be a
        :class:`xarray.DataArray` object.  Otherwise, the result will be a
        :class:`numpy.ndarray` object with no metadata.

    """
    logging.debug("Destaggering Z grid")
    var_shape = var.shape
    num_dims = var.ndim
    stagger_dim_size = var_shape[stagger_dim]

    full_slice = slice(None)
    slice1 = slice(0, stagger_dim_size - 1, 1)
    slice2 = slice(1, stagger_dim_size, 1)

    # default to full slices
    dim_ranges_1 = [full_slice] * num_dims
    dim_ranges_2 = [full_slice] * num_dims

    # for the stagger dim, insert the appropriate slice range
    dim_ranges_1[stagger_dim] = slice1
    dim_ranges_2[stagger_dim] = slice2

    result = 0.5 * (var[tuple(dim_ranges_1)] + var[tuple(dim_ranges_2)])

    return result


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


def calculate_ivt(
    u_wind: xr.DataArray,
    v_wind: xr.DataArray,
    pressure: xr.DataArray,
    mixing_ratio: xr.DataArray,
    vertical_dim: str = "bottom_top",
) -> xr.Dataset:
    """
    Calculate the Integrated Vapor Transport (IVT) from wind components, pressure, and mixing ratio.
    """

    specific_humidity = mixing_ratio / (1 + mixing_ratio)

    # Slice q, u, v to match dp dimensions
    q_slice = specific_humidity.isel(bottom_top=slice(None, -1))
    u_slice = u_wind.isel(bottom_top=slice(None, -1))
    v_slice = v_wind.isel(bottom_top=slice(None, -1))

    dp = pressure.diff(dim=vertical_dim)

    ivt_x = -(1 / 9.81) * (q_slice * u_slice * dp).sum(dim=vertical_dim)
    ivt_y = -(1 / 9.81) * (q_slice * v_slice * dp).sum(dim=vertical_dim)
    ivt = np.sqrt(ivt_x**2 + ivt_y**2)

    ivt_ds = xr.Dataset(
        {
            "IVT": (["Time", "south_north", "west_east"], ivt.data),
        },
        coords={
            "Time": ivt["Time"],
            "XLAT": ivt["XLAT"],
            "XLONG": ivt["XLONG"],
        },
    )
    return ivt_ds
