# Not used currently.
DATA_ROOT_PATH = '/home/uw1/data/SPEC/OSG'
BASE_DOMAIN = "https://spec.org/"


class FileType:
    # upper case string.
    HTML = "HTML"
    PS = "PS"
    PDF = "PDF"
    CSV = "CSV"
    TEXT = "TEXT"
    CONFIG = "CONFIG"


class GroupType:
    OSG = 1  # Open System Group
    HPC = 2  # High Performance Group
    ISG = 3  # International Standards Group
    GWP = 4  # Graphics and Workstation Performance Group
    RG = 5  # Research Group


GroupTypeMaps = {
    GroupType.OSG: "OSG: Open System Group",
    GroupType.HPC: "HPC: High Performance Group",
    GroupType.ISG: "ISG: International Standards Group",
    GroupType.GWP: "GWP: Graphics and Workstation Performance Group",
    GroupType.RG: "RG: Research Group"
}
# BenchmarkGroups = [GroupType.OSG, GroupType.HPC, GroupType.ISG, GroupType.GWP]
BenchmarkGroups = [GroupType.OSG]
