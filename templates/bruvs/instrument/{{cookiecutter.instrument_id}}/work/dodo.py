import doit
import os
import glob
from doit import get_var
import jinja2
import yaml
from pathlib import Path

cfg = None
CATALOG_DIR = None

cfg = None
CATALOG_DIR = None
def task_read_config():
    global cfg
    global CATALOG_DIR
    config = {"config": get_var('config', 'NO')}
    with open(config['config'], 'r') as ymlfile:
        cfg = yaml.load(ymlfile, yaml.SafeLoader)
    CATALOG_DIR = os.path.dirname(config['config'])

def geturl(key):
    global cfg
    global CATALOG_DIR
    if cfg is None:
        read_config()
    environment = jinja2.Environment()
    template = environment.from_string(cfg['paths'][key])
    return(template.render(CATALOG_DIR=CATALOG_DIR))

def task_create_json():
        for path in Path('src').rglob('.'):

#        for item in glob.glob(geturl('cardstore'),recursive=True):
            if glob.glob(os.path.join(item,"*.MP4")):
                target  = os.path.join(item,cfg['paths']['exifname'])
                file_dep = glob.glob(filter)
                if file_dep:
                    yield { 
                        'name':item,
                        'actions':[f'exiftool -api largefilesupport=1 -u  -json -ext MP4 > {target}'],
                        'targets':[target],
                        'uptodate':[True],
                        'clean':True,
                    } 




if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())