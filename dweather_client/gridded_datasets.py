from dweather_client.ipfs_queries import SimpleGriddedDataset, RtmaGriddedDataset, Era5LandWind, PrismGriddedDataset, Vhi

"""
Module containing the leaf classes for the gridded dataset inheritance chain.
Do not add non-leaf classes to this file, as it will cause the dict built from this module to break
"Unused" `Vhi` import is intentional and necessary, as list of gridded datasets is built from all classes in this file!
"""

class PrismcTmaxDaily(PrismGriddedDataset):
    dataset = "prismc-tmax-daily"

class PrismcTminDaily(PrismGriddedDataset):
    dataset = "prismc-tmin-daily"

class PrismcPrecipDaily(PrismGriddedDataset):
    dataset = "prismc-precip-daily"

class RtmaDewPointHourly(RtmaGriddedDataset):
    dataset = "rtma_dew_point-hourly"

class RtmaGustHourly(RtmaGriddedDataset):
    dataset = "rtma_gust-hourly"

class RtmaPcpHourly(RtmaGriddedDataset):
    dataset = "rtma_pcp-hourly"

class RtmaTempHourly(RtmaGriddedDataset):
    dataset = "rtma_temp-hourly"

class RtmaWindUHourly(RtmaGriddedDataset):
    dataset = "rtma_wind_u-hourly"

class RtmaWindVHourly(RtmaGriddedDataset):
    dataset = "rtma_wind_v-hourly"

class CpccPrecipUsDaily(SimpleGriddedDataset):
    dataset = "cpcc_precip_us-daily"

class CpccPrecipGlobalDaily(SimpleGriddedDataset):
    dataset = "cpcc_precip_global-daily"

class CpccTempMaxDaily(SimpleGriddedDataset):
    dataset = "cpcc_temp_max-daily"

class CpccTempMinDaily(SimpleGriddedDataset):
    dataset = "cpcc_temp_min-daily"

class ChirpscFinal05Daily(SimpleGriddedDataset):
    dataset = "chirpsc_final_05-daily"

class ChirpscFinal25Daily(SimpleGriddedDataset):
    dataset = "chirpsc_final_25-daily"

class ChirpscPrelim05Daily(SimpleGriddedDataset):
    dataset = "chirpsc_prelim_05-daily"

class Era5Land2mTempHourly(SimpleGriddedDataset):
    dataset = "era5_land_2m_temp-hourly"

class Era5LandPrecipHourly(SimpleGriddedDataset):
    dataset = "era5_land_precip-hourly"

class Era5LandSurfaceSolarRadiationDownwardsHourly(SimpleGriddedDataset):
    dataset = "era5_land_surface_solar_radiation_downwards-hourly"

class Era5LandSnowfallHourly(SimpleGriddedDataset):
    dataset = "era5_land_snowfall-hourly"

class Era5LandWindUHourly(Era5LandWind):
    dataset = "era5_land_wind_u-hourly"

class Era5LandWindVHourly(Era5LandWind):
    dataset = "era5_land_wind_v-hourly"

class Era5SurfaceRunoffHourly(SimpleGriddedDataset):
    dataset = "era5_surface_runoff-hourly"

class Era5Wind100mUHourly(SimpleGriddedDataset):
    dataset = "era5_wind_100m_u-hourly"

class Era5Wind100mVHourly(SimpleGriddedDataset):
    dataset = "era5_wind_100m_v-hourly"

class Era5VolumetricSoilWater(SimpleGriddedDataset):
    dataset = "era5_volumetric_soil_water_layer_1-hourly"
