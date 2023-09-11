import pandas as pd
import subprocess
import json
import marimba
import numpy as np
import os
import subprocess
import shlex
from typer.testing import CliRunner
from marimba.marimba import marimba
import typer
import platform
import signal
import atexit

def killcopy():
    os.wait()


#atexit.register(killcopy)

def main(collection_path: str = typer.Argument(..., help="Root path to MarImBA collection."),
         instrument_id: str = typer.Argument(..., help="MarImBA instrument ID."),
         max_processes:int = typer.Option(4, help="Number of concurrent transfers"),
         format_type:str = typer.Option('exfat', help="Card format type"),
         debug:bool = typer.Option(False, help="Card format type")):



    if debug:
        runner = CliRunner()

    processes = set()
    if platform.system() == "Linux":
        mountpoints = pd.DataFrame(json.loads(subprocess.getoutput('lsblk -J -o  NAME,SIZE,FSTYPE,TYPE,MOUNTPOINT'))['blockdevices'])
        mountpoints = mountpoints[~mountpoints.children.isna()]
        mountpoints =pd.DataFrame(mountpoints.children.apply(lambda x: x[0]).to_list())[['name','mountpoint','fstype']]
        mountpoints = mountpoints[mountpoints.fstype==format_type]           
        paths = pd.DataFrame(subprocess.getoutput('udevadm info -q path -n $(ls /dev/s*1)').splitlines(),columns=['Path'])
        paths[['host','dev']]=paths.Path.str.extract(r'(?P<host>host\d+).*block\/(?P<dev>([^\\]+$))')[['host','dev']]
        paths['name'] =paths.dev.str.split('/',expand=True)[1]
        mountpoints =mountpoints.merge(paths, on='name', how='inner')
        commands =[]
        for index, df in mountpoints.groupby('host'):
            args =["import",collection_path,instrument_id]    
            for card in df.mountpoint.to_list():
                args.append(card)
            commands.append(args)
    else:
        commands =[]
        for i in psutil.disk_partitions():
            if i.fstype==format_type:
                p =psutil.disk_usage(i.mountpoint)
                if np.ceil(p.total/1000000000)<=512:
                    #os.system()
                    args =["import",collection_path,instrument_id,i.mountpoint] 
                    commands.append(args)

    for args in commands:
        if debug:
            runner.invoke(marimba,args)
        else:
            if len(processes) >= max_processes:
                os.wait()
            processes.add(subprocess.Popen(shlex.split(f'gnome-terminal -- bash --rcfile /home/mor582/code/marimba/scripts/bashrc -ci "marimba {" ".join(args)}"')))
    os.wait()

typer.run(main)