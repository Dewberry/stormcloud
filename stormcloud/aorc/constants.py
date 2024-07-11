from dataclasses import dataclass

# FTP server host common prefix
FTP_HOST = "https://hydrology.nws.noaa.gov/pub/AORC/V1.1"


@dataclass
class RFCInfo:
    """
    Data Property: Regional Forecast Center (RFC) names and aliases
    """

    alias: str
    name: str


RFC_INFO_LIST = [
    RFCInfo("AB", "ARKANSAS RED BASIN"),
    RFCInfo("CB", "COLORADO BASIN"),
    RFCInfo("CN", "CALIFORNIA NEVADA"),
    RFCInfo("LM", "LOWER MISSISSIPPI"),
    RFCInfo("MA", "MID ATLANTIC"),
    RFCInfo("MB", "MISSOURI BASIN"),
    RFCInfo("NC", "NORTH CENTRAL"),
    RFCInfo("NE", "NORTHEAST"),
    RFCInfo("NW", "NORTHWEST"),
    RFCInfo("OH", "OHIO"),
    RFCInfo("SE", "SOUTHEAST"),
    RFCInfo("WG", "WEST GULF"),
]
