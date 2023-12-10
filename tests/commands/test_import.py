import sys
from marimba.marimba import marimba

if __name__ == '__main__':
    #marimba(["new",'project','/home/mor582/newproj'])
    #marimba(["new",'pipeline','GOPRO1','https://bitbucket.csiro.au/scm/biaa/bruvs-gopro-pipeline.git','--project-dir','/home/mor582/newproj'])
    #marimba(["initialise","WAMSI202301","GOPRO","--scan-cards"])
    #marimba(["import","GOPRO",'--scan-cards','--project-dir','/home/mor582/newproj'])
    marimba(['prepare','GOPRO'])
    #marimba(["new",'project','/home/mor582/newproj'])