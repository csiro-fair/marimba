import doit
import os
import glob
from doit import get_var
import jinja2
import yaml
from doit.action import CmdAction
from doit.tools import title_with_actions
from doit.tools import run_once
import pandas as pd
from doit import create_after
import sqlite3
import psutil
import platform
import subprocess
import json
from pathlib import Path
import numpy as np
from doit.task import clean_targets


cfg = None
CATALOG_DIR = None
DOIT_CONFIG = {'check_file_uptodate': 'timestamp',"continue": True}
format_type = 'exfat'



def geturl(key):
    global cfg
    global CATALOG_DIR
    environment = jinja2.Environment()
    template = environment.from_string(cfg['paths'][key])
    return(template.render(CATALOG_DIR=CATALOG_DIR))

def task_config():
        def loadconfig(config):
            global cfg
            global CATALOG_DIR
            global COLLECTION_DIR
            with open(config, 'r') as ymlfile:
                cfg = yaml.load(ymlfile, yaml.SafeLoader)
            CATALOG_DIR = os.path.dirname(os.path.abspath(config))
            COLLECTION_DIR = Path(CATALOG_DIR).resolve().parents[2]
        config = {"config": get_var('config', f'{os.path.split(__file__)[0]}/config.yml')}
        loadconfig(config['config'])


def task_create_json():
        for path in glob.glob(os.path.join(geturl('cardstore'),'**','.'),recursive=True):
            path = os.path.abspath(path)
            if glob.glob(os.path.join(path,"*.MP4")):
                target  = os.path.join(path,cfg['paths']['exifname'])
                file_dep = glob.glob(os.path.join(path,"*.MP4"))
                command = f'exiftool -api largefilesupport=1 -m -u -q -q -n -CameraSerialNumber -CreateDate -SourceFile -Duration -FileSize -FieldOfView -json -ext MP4 {path} > {target} || :'
                if file_dep:
                     yield { 
                        'name':path,
                        'file_dep':file_dep,
                        'actions':[command],
                        'targets':[target],
                        'uptodate':[run_once],
                        'clean':True,
                    }
@create_after(executed='create_json', target_regex='.*\.json') 
def task_concat_json():

        def concat(dependencies, targets):
            data = pd.concat([ pd.read_json(dep) for dep in dependencies])
            data['Bad'] =data['CreateDate'].isna()
            data['SourceFile'] = data.apply(lambda x: f"{{CATALOG_DIR}}/{os.path.relpath(x['SourceFile'],CATALOG_DIR)}",axis=1)
            data['Directory']=data['SourceFile'].apply(lambda x: os.path.split(x)[0])
            data['FileName'] = data['SourceFile'].apply(os.path.basename)
            data[['ItemId','GroupId']]=data.FileName.str.extract('(?P<item>\d\d)(?P<group>\d\d\d\d).MP4')
            data =data.sort_values(['SourceFile'])
            data['CreateDate'] =pd.to_datetime(data.CreateDate,format='%Y:%m:%d  %H:%M:%S')
            #ok lets try and fix missing data from bad videos
            data['RunTime'] = data.groupby(['CameraSerialNumber','GroupId'])['Duration'].cumsum()
            data =data.sort_values(['SourceFile'])
            data.to_csv(targets[0],index=False)
        exiffiles =glob.glob(os.path.join(geturl('cardstore'),'**',cfg['paths']['exifname']),recursive=True)
        if exiffiles:
            return { 

                'file_dep':exiffiles,
                'actions':[concat],
                'targets':[geturl('exifstore')],
                'uptodate':[True],
                'clean':True,
            } 
        
@create_after(executed='concat_json', target_regex='.*\.json') 
def task_checkbars():
        def checkbars(dependencies, targets):
            data = pd.read_csv(dependencies[0],parse_dates=['CreateDate'])
            stats =data.groupby('CameraSerialNumber')['CreateDate'].agg([('CreateStart', 'min'), ('CreateEnd', 'max')]).reset_index()
            barnumbers = pd.read_csv(targets[0],parse_dates=['BarStartDate','BarEndDate']).drop(['CreateStart','CreateEnd'],axis=1,errors='ignore')                      
            barnumbers = pd.merge(barnumbers,stats) 
            barnumbers.to_csv(targets[0],index=False)

        return { 

            'file_dep':[geturl('exifstore')],
            'actions':[checkbars],
            'targets':[geturl('barstore')],
            'uptodate':[True],
            'clean':True,
        }         

@create_after(executed='checkbars', target_regex='.*\.json') 
def task_make_autodeployments():

        def deployments(dependencies, targets):
            data = pd.read_csv(geturl('exifstore'),parse_dates=['CreateDate'])
            totaltime =pd.to_datetime(data.groupby(['Directory','CreateDate','CameraSerialNumber','GroupId'])['Duration'].sum(), unit='s').dt.strftime("%H:%M:%S").rename('TotalTime')
            totalfilesize =(data.groupby(['Directory','CreateDate','CameraSerialNumber','GroupId'])['FileSize'].sum()/1000000000).rename('TotalSize')
            maxid =data.groupby(['Directory','CreateDate','CameraSerialNumber','GroupId'])['ItemId'].max().rename('MaxId')
            minid =data.groupby(['Directory','CreateDate','CameraSerialNumber','GroupId'])['ItemId'].min().rename('MinId')
            filecount = data.groupby(['Directory','CreateDate','CameraSerialNumber','GroupId'])['ItemId'].count().rename('FileCount')
            groups =data.groupby(['Directory','CreateDate','CameraSerialNumber','GroupId'])[['SourceFile','FieldOfView']].first()
            output =groups.join(filecount).join(minid).join(maxid).join(totalfilesize).join(totaltime)
            barpath = f'{CATALOG_DIR}/camerabars.csv'
            barnumbers = pd.read_csv(barpath,parse_dates=['BarStartDate','BarEndDate']) 
            result = matchbars(output.reset_index(),barnumbers)
            result['CreateDate'] = pd.to_datetime(result['CreateDate'] )
            result['DeploymentId']=result.apply(lambda x: f"{x.CreateDate.strftime('%Y%m%dT%H%M%S')}_{x.Frame}_{x.GoProNumber}_{x.CameraSerialNumber}_{x.GroupId:02}", axis=1)

            manualfile = geturl('timecorrection')
            manual =result.loc[:, ['DeploymentId', 'TotalTime','CreateDate','SourceFile']]
            manual =manual.set_index('DeploymentId')
            if os.path.exists(manualfile):
                 old = pd.read_csv(manualfile,index_col='DeploymentId')
                 manual =manual.join(old['CorrectedTime'])
                 manual.loc[manual.CorrectedTime.isnull(),'CorrectedTime']=manual.loc[manual.CorrectedTime.isnull(),'CreateDate']
            else:
                manual['CorrectedTime'] = manual['CreateDate']
            manual['SourceFile'] = manual['SourceFile'].apply(lambda x: f'=HYPERLINK("file://{x}", "{os.path.basename(x)}")')
            manual.sort_values('DeploymentId').to_csv(manualfile)
            manual.sort_values('DeploymentId').to_excel(manualfile.replace('.csv','.xlsx'))
            result.to_csv(targets[0],index=False)



        target = geturl('autodeployment')
        return { 

            'file_dep':[geturl('exifstore')],
            'actions':[deployments],
            'targets':[target,geturl('timecorrection')],
            'uptodate':[True],
            'clean':True,
        } 

def matchbars(deployments,barnumbers,datecolumn='CreateDate'):
    conn = sqlite3.connect(':memory:')
    #write the tables
    barnumbers.to_sql('bars', conn, index=False)
    deployments[deployments.columns[~deployments.columns.isin(['Frame','HousingNumber','GoProNumber','BarStartDate','BarEndDate'])]].to_sql('deployments', conn, index=False)
    qry = f'''
        select  
            deployments.*,
            bars.Frame,
            bars.HousingNumber,
            bars.GoProNumber,
            bars.BarStartDate,
            bars.BarEndDate
        from
            deployments join bars on
            (deployments.{datecolumn} between bars.BarStartDate and bars.BarEndDate) and
            (deployments.CameraSerialNumber = bars.CameraSerialNumber)
        '''
    result =pd.read_sql_query(qry, conn)
    result['CreateDate'] = pd.to_datetime(result['CreateDate'] )
    result['DeploymentId']=result.apply(lambda x: f"{x.CreateDate.strftime('%Y%m%dT%H%M%S')}_{x.Frame}_{x.GoProNumber}_{x.CameraSerialNumber}", axis=1)
    return result


@create_after(executed='make_autodeployments', target_regex='.*\.json') 
def task_make_matchbars():

        def stagedeployments(dependencies, targets):
            def calculatetimes(df):
                df =df.sort_values('ItemId')
                if len(df)>1:
                    start = df.Duration.cumsum().shift(+1)
                    start.iloc[0] = 0
                    start =pd.to_timedelta(start,unit='S')
                    df['CalculatedStartTime']=(df['CorrectedTime']+start).dt.round('1S')
                else:                 
                    df['CalculatedStartTime']=df['CorrectedTime']
                return df

            def makedeploymentkey(df):
                def makedirs(row):
                    left = 0
                    right = 0
                    if leftcam in row.keys():
                         left = row[leftcam]
                    if rightcam in row.keys():
                         right = row[rightcam]
                    if right==left:
                        result = f"{row['StageId']}_{int(left):02}"
                    else:
                        result = f"{row['StageId']}_{int(left):02}_{int(right):02}"
                    return row['StageId'],result
                leftcam = 'L'+df.Frame.min()[-2:]
                rightcam = 'R'+df.Frame.min()[-2:]
                left = df[df.GoProNumber==leftcam].groupby('CorrectedTime').first().reset_index()[['CorrectedTime','GoProNumber','Frame']].add_suffix('_Left')
                left['MatchTime'] = left['CorrectedTime_Left']
                right = df[df.GoProNumber==rightcam].groupby('CorrectedTime').first().reset_index()[['CorrectedTime','GoProNumber','Frame']].add_suffix('_Right')
                right['MatchTime'] = right['CorrectedTime_Right']
                merged_df = pd.merge_asof(right, left, left_on='MatchTime', right_on='MatchTime', direction='nearest', tolerance=pd.Timedelta(minutes=30),suffixes=( '_right','_left'))
                merged_df =pd.concat([merged_df,left[~left.CorrectedTime_Left.isin(merged_df.CorrectedTime_Left.unique())]])
                merged_df.loc[merged_df.CorrectedTime_Right.isna(),'Frame_Right'] = merged_df.loc[merged_df.CorrectedTime_Right.isna(),'Frame_Left']
                merged_df.loc[merged_df.CorrectedTime_Left.isna(),'CorrectedTime_Left'] = merged_df.loc[merged_df.CorrectedTime_Left.isna(),'CorrectedTime_Right']
                starttime =merged_df.MatchTime.dt.strftime("%Y%m%dT%H%M%S")
                merged_df['StageId']=merged_df.Frame_Right+'_'+starttime
                stageId =pd.concat((merged_df[['CorrectedTime_Left','StageId']].rename(columns={'CorrectedTime_Left':'CorrectedTime'}),merged_df[['CorrectedTime_Right','StageId']].rename(columns={'CorrectedTime_Right':'CorrectedTime'}))).dropna()
                df =pd.merge(df,stageId)
                totals =df.groupby(['StageId','GoProNumber']).size().reset_index().pivot_table(index ='StageId',values=0,columns='GoProNumber').reset_index().fillna(0)
                totals =totals.apply(makedirs,axis=1).apply(pd.Series)
                totals.columns = ['StageId','StageDir']
                df = df.merge(totals)
                return df
            dep = pd.read_csv(geturl('autodeployment'),parse_dates=['CreateDate'])
            exifdata = pd.read_csv(geturl('exifstore'),parse_dates=['CreateDate']).set_index(['CreateDate','CameraSerialNumber','GroupId'])
            correcttimes = pd.read_csv(geturl('timecorrection'),parse_dates=['CreateDate','CorrectedTime'])
            dep =pd.merge(dep,correcttimes[['DeploymentId','CorrectedTime']],on='DeploymentId', how='left').set_index(['CreateDate','CameraSerialNumber','GroupId'])
            combined = dep.join(exifdata,rsuffix='_exif').reset_index()
            combined =combined.drop_duplicates(subset=['CameraSerialNumber','CreateDate','GroupId','ItemId'],keep='last')
            combined = combined.sort_values(['CorrectedTime','GroupId','ItemId'])
            combined =combined.groupby(['CreateDate','CameraSerialNumber','GroupId'],group_keys=False).apply(calculatetimes).reset_index()
            barpath = f'{CATALOG_DIR}/camerabars.csv'
            barnumbers = pd.read_csv(barpath,parse_dates=['BarStartDate','BarEndDate']) 
            result = matchbars(combined,barnumbers,datecolumn='CalculatedStartTime')
            result['CalculatedStartTime'] = pd.to_datetime(result['CalculatedStartTime'])
            result['CorrectedTime'] = pd.to_datetime(result['CorrectedTime'])
            result['StageName'] = result.apply(lambda x: f'{x.Frame}_{x.GoProNumber}_{x.CalculatedStartTime.strftime("%Y%m%dT%H%M%S")}_{x.CameraSerialNumber}_{int(x.GroupId):02d}_{int(x.ItemId):02d}.MP4',axis=1)
            result =result.drop_duplicates(subset=['CameraSerialNumber','CreateDate','GroupId','ItemId'],keep='last')
            result=result.groupby('Frame').apply(makedeploymentkey)
            result.to_csv(targets[0],index=False)
        return { 

            'file_dep':[geturl('autodeployment'),geturl('exifstore'),geturl('barstore')],
            'actions':[stagedeployments],
            'targets':[geturl('stage')],
            'uptodate':[True],
            'clean':True,
        } 

@create_after(executed='make_autodeployments', target_regex='.*\.json') 
def task_stage_data():
        def hardlink(dependencies, targets):
             stage = pd.read_csv(geturl('stage'))
             stage['target'] =stage.apply(lambda x: os.path.join(CATALOG_DIR,'stage',x.StageDir,x.StageName),axis=1)
             stage['SourceFile'] = stage['SourceFile'].apply(lambda x: x.format(**{'CATALOG_DIR':CATALOG_DIR}))
             for index, row in stage.iterrows():
                  if not os.path.exists(row.target):
                    dir =os.path.split(row.target)[0]
                    os.makedirs(dir,exist_ok=True)
                    os.link(row.SourceFile,row.target)

        def delete_empty_folders(dryrun):
            for dirpath, dirnames, filenames in os.walk(os.path.join(CATALOG_DIR,'stage'), topdown=False):
                for dirname in dirnames:
                    full_path = os.path.join(dirpath, dirname)
                    if not os.listdir(full_path): 
                        if dryrun:
                             print(f'Remove dir {full_path}')
                        else:
                            os.rmdir(full_path)

               
        if os.path.exists(geturl('stage')):
            stage = pd.read_csv(geturl('stage'))
            targets =stage.apply(lambda x: os.path.join(CATALOG_DIR,'stage',x.StageDir,x.StageName),axis=1).unique().tolist()
            return { 

                'file_dep':[geturl('stage')],
                'actions':[hardlink],
                'targets':targets,
                'uptodate':[True],
                'clean':[clean_targets,delete_empty_folders],
            } 
        
@create_after(executed='stage_data', target_regex='.*\.json') 
def task_update_stationinformation():
        def finalnames(dependencies, targets):
             stage = pd.read_csv(geturl('stage'),index_col='StageId')
             stage = stage.groupby('StageId')['CalculatedStartTime'].agg([('VideoStart', 'min'), ('VideoEnd', 'max')]).reset_index()
             if os.path.exists(geturl('stationinfo')):
                stations = pd.read_csv(geturl('stationinfo'),index_col='StageId')
             else:
                stations = pd.DataFrame(columns=['StartTime','FinishTime','CollectionId','Station','Operation','Latitude','Longitude','Depth','VideoStart','VideoEnd'])
             stations =stations.join(stage.groupby('StageId').first(),how='outer',rsuffix='_Stage')
             stations['Frame'] = stations.index
             stations[['Frame','CameraTime']] =stations.Frame.str.split('_',expand=True)
             stations.loc[stations.VideoStart_Stage.isna(),'VideoStart_Stage'] = stations.loc[stations.VideoStart_Stage.isna(),'VideoStart']
             stations.loc[stations.VideoStart_Stage.isna(),'VideoEnd_Stage'] = stations.loc[stations.VideoEnd_Stage.isna(),'VideoEnd']
             stations[['VideoStart','VideoEnd']] = stations[['VideoStart_Stage','VideoEnd_Stage']]
             stations.drop(['VideoStart_Stage','VideoEnd_Stage'],axis=1).sort_values('VideoStart').to_csv(geturl('stationinfo'))          
        return { 

            'file_dep':[geturl('stage')],
            'targets': [geturl('stationinfo')],
            'actions':[finalnames],
            'uptodate':[run_once],
            'clean':True,
        } 

@create_after(executed='update_stationinformation', target_regex='.*\.json') 
def task_process_names():
     pass
        

def delete_empty_folders(dryrun):
    for dirpath, dirnames, filenames in os.walk(os.path.join(CATALOG_DIR,'stage'), topdown=False):
        for dirname in dirnames:
            full_path = os.path.join(dirpath, dirname)
            if not os.listdir(full_path): 
                if dryrun:
                        print(f'Remove dir {full_path}')
                else:
                    os.rmdir(full_path)

if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp',"continue": True}
    #print(globals())
    doit.run(globals())