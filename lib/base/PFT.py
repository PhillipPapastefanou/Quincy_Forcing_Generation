from enum import Enum

class Quincy_Orchidee_PFT(Enum):
    # Tropical broad-leaved evergreen
    TrBE = 0,
    # Tropical broad-leaved raingreen
    TrBR = 1,
    # Temperate needle-leaved evergreen
    TeNE = 2,
    # Temperate broad-leaved evergreen
    TeBE = 3,
    # Temperate broad-leaved summergreen
    TeBS = 4,
    # Boreal needle-leaved evergreen
    BNE  = 5,
    # Boreal broad-leaved summergreen
    BBS  = 6,
    # Boreal needle-leaved summergreen
    BNS  = 7,
    # C3 grass
    TeH  = 8,
    # C4 grass
    TrH  = 9,
    # C3 crop grass
    TeHC = 10,
    # C4 crop grass
    TrHC = 11


class Quincy_Orchidee_PFT_List:
    def __init__(self):
        self.Fractions = {}
        for pft in Quincy_Orchidee_PFT:
            self.Fractions[pft] = 0.0

class IGPB_classifications:
    def __abs__(self):
        self.keys = {}
        self.keys['CRO'] = "Croplands"
        self.keys['CSH'] = "Closed Shrublands"
        self.keys['CVM'] = "Cropland/Natural Vegetation Mosaics"
        self.keys['DBF'] = "Deciduous Broadleaf Forests"
        self.keys['EBF'] = "Evergreen Broadleaf Forests"
        self.keys['ENF'] = "Evergreen Needleleaf Forests"
        self.keys['GRA'] = "Grasslands"
        self.keys['MF']  = "Mixed Forests"
        self.keys['OSH'] = "Open Shurblands"
        self.keys['SAV'] = "Savannas"
        self.keys['WET'] = "Permanent Wetlands"
        self.keys['WSA'] = "Woody Savannas"