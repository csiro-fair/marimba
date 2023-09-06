import psutil
import numpy as np
from marimba.marimba import marimba
for i in psutil.disk_partitions():
    if i.fstype=='exfat':
        p =psutil.disk_usage(i.mountpoint)
        if np.ceil(p.total/1000000000)==512:
            marimba(["import","/mnt/md127/fielddata0/Marimba/DR2023-01","bruvs", i.mountpoint,"--clean"])
            

