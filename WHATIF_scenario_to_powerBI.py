# -*- coding: utf-8 -*-
"""
    Copyright 2018 Technical University of Denmark
    Copyright 2018 COWI A/S
    
    Authors: 
        Raphaël Payet-Burin
        Mikkel Kromann

    This file is part of WHAT-IF.

    WHAT-IF is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License version 3 (GPLv3) as published by the Free Software Foundation,

    WHAT-IF is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along with WHAT-IF. If not, see http://www.gnu.org/licenses/.

"""

import sys
import os
import pickle
import pandas as pd
dirname = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(dirname, 'bin'))
import locale
#set decimal and separator (for handling of csv files)
langlocale = locale.getdefaultlocale()[0]
locale.setlocale(locale.LC_ALL, langlocale)
CSVDECIMAL = locale.localeconv()['decimal_point']
CSVSEPARATOR = ','
if CSVDECIMAL == ',':
    CSVSEPARATOR = ';'

#%%OPTIONS - MODIFY BY USER
SHEET='scenarios'
FOLDERNAME='2023_07_02_15h04_zim_BAU_ASP_RES_scenarios'
DIFFMODE=0 # if=1 exports relative results to POWERBI, instead of absolute (=0)
IFPRI_IDX=1
IFPRI_REFSCEN='zim_observed'
SCENFILE='Scenarios_to_compare.xlsx'
result_path = os.path.join(dirname,'Results',FOLDERNAME)

#%%#####################################
#               FUNCTIONS

#Export panda dataframe to .csv
def pd_to_csv(folder,file,pdata,iindex=True):
    file = os.path.join(folder,file)
    if isinstance(pdata,pd.DataFrame):
        return pdata.to_csv(file,sep=CSVSEPARATOR,decimal=CSVDECIMAL,index=iindex)
#Transfor dictionary in panda dataframe (for specific dic structure)
def dict_to_pd(dic):
    if dic != {}:
        #creates dataframe from nested dic, moves left column to index, and first index to column
        pdata=pd.concat({k: pd.DataFrame(v).T for k, v in dic.items()}, axis=0)
        if not pdata.empty:
            pdata=pdata.stack().unstack(0)
            return pdata #WARNING: indented so that empty dataframes are ignored, might result in problems if all result exclude that component
        else:
            return pd.DataFrame(columns=pdata.index.levels[0])

def aggregate_scenarios_to_csv(scenarios,vardic,outpath,keytype='tuple',indexname=0,elist=0,refscen=0,renamecol=1):
    ##scenarios: list of scenarios
    ##vardic: dictionary containing all elements for alls scenarios, in the form:
    #vardic ={scen:{elem:{...} for elem in elist} for scen in scenarios}
    ##outpath: folder to export to
    ##keytype: how to read vardic[scen][elem], 
    #'tuple' = dic in the form {(i1,...,in):result for i1 in I1 ... for in in In}
    #'nested' = dic in the form {i1:...{in:result for in in In}... for i1 in I1}
    ##indexname : names of the indexes [i1_name,...,in_name]
    ##elist : list of all elements to be taken from vardic - if =0, all elements are exported
    ##refscen : Use differential (A-B) values of scenarios according to defined reference scenario: 
    #refscen = {scen: relative_scen for scen in scenarios}, relative_scen has to be in vardic.keys()
    ##renamecol: 1 = renames column by elem, 0 = keeps default name (or leaves empty)
    
    #create outpath
    if not os.path.exists(outpath):
        os.makedirs(outpath)
    
    #diff function - does panda-pandaref + keeps panda where pandaref does not exist
    def diff_panda(panda,pandaref):
        pandaf=panda.subtract(pandaref)
        pandaf=pandaf.fillna(panda)
        return pandaf
    
    #generate elist if not passed
    if elist==0:
        elist=[]
        for scen in scenarios:
            for key in vardic[scen].keys():
                if indexname==0 or key in indexname.keys(): #do not add variable if not in the list
                    elist.append(key)        
        elist=set(elist)

    for elem in elist:
        print(elem)
        #scenarios that contain that element
        if keytype=='tuple':
            scenpresent=[scen for scen in scenarios if elem in vardic[scen].keys()] #scenarios that contain that element
        elif keytype=='nested': #ADD could be deleted as vardic[scen][elem] is usually not empty even if no data but is the years
            scenpresent=[scen for scen in scenarios if vardic[scen][elem]!={}] #scenarios that contain that element
            
        if refscen==0:
            #assemble different scenarios
            if keytype=='tuple':
                frames=[pd.Series(vardic[scen][elem],dtype='float64') for scen in scenpresent] 
            elif keytype=='nested':
                frames=[dict_to_pd(vardic[scen][elem]) for scen in scenpresent]
        else:
            #assemble different scenarios and subtract ref scenario
            if keytype=='tuple':
                frames=[diff_panda(pd.Series(vardic[scen][elem]),pd.Series(vardic[refscen[scen]][elem])) for scen in scenpresent]
            elif keytype=='nested':
                frames=[diff_panda(dict_to_pd(vardic[scen][elem]),dict_to_pd(vardic[refscen[scen]][elem])) for scen in scenpresent]
        
        #discard scenarios that do not have the same amount of index levels (e.g. different model options - avoid concatenation problems) - WARNING- they will not appear in the final result
        nlevels=[pdat.index.nlevels for pdat in frames] #number of levels in each dataframe
        nlevelmax=max(set(nlevels), key = nlevels.count) #most occuring number of levels
        framestokeep=[frames[k] for k in range(len(frames)) if nlevels[k]==nlevelmax] #only keep frames with most common number of levels
        scentokeep=[scenpresent[k] for k in range(len(frames)) if nlevels[k]==nlevelmax] #only keep scenarios which frame has the most common number of levels
        #concatane to single dataframe
        if refscen==0:
            pdata=pd.concat(framestokeep,keys=[scen for scen in scentokeep])
        else:
            pdata=pd.concat(framestokeep,keys=[scen+'_'+refscen[scen] for scen in scentokeep])
        if keytype=='tuple':
            pdata=pdata.to_frame() 
        #elif keytype=='nested':
         #   pdata=pd.concat(frames,keys=[scen for scen in scenarios if elem in vardic[scen].keys()])
        
        if not pdata.empty: #don t export e
            #rename columns and indexes
            if indexname != 0:
                indexname[elem].insert(0,'scenario')
                pdata.index.names=indexname[elem]
                if renamecol==1:
                    pdata.columns=[elem]
            iindex=True #for export
        elif pdata.empty and keytype=='nested': #when crop market or power market off
            if indexname != 0:
                indexname[elem].insert(0,'scenario')
                for k in range(len(indexname[elem])):
                    pdata.insert(k,indexname[elem][k],0)
                iindex=False #for export
        elif pdata.empty and keytype=='tuple': #decision variable present but void (probably modelling/data error but not critical)
            if elem in indexname.keys():
                indexname[elem].insert(0,'scenario')
                pdata=pd.DataFrame(columns=indexname[elem]+[elem])
                iindex=False #for export
        #export to csv
        filename=str(elem)+'.csv'
        pd_to_csv(outpath,filename,pdata,iindex=iindex)
    
    #Create empty datasets for not exported Decision variables
    if indexname != 0:
        for elem in indexname.keys():
            if elem not in elist:
                indexname[elem].insert(0,'scenario')
                pdata=pd.DataFrame(columns=indexname[elem]+[elem])
                filename=str(elem)+'.csv'
                pd_to_csv(outpath,filename,pdata,iindex=False)
                #pdata.to_csv(os.path.join(outpath,filename),sep=CSVSEPARATOR,decimal=CSVDECIMAL,index=False)
#%%#####################################
#        COLLECT INFORMATION
        
sceninfo=pd.read_excel(SCENFILE,sheet_name=SHEET, skiprows=1, index_col=[0], engine='openpyxl')
refscen=sceninfo.to_dict()['refscen']
scenarios=[s for s in refscen.keys()]
#scen_to_load=[s for s in refscen.keys()]


#%%#####################################
#               POWERBI

#%%DECISION VARIABLES
#create directory
DVpath=os.path.join(result_path,'DecisionVariables')

#Varaibles index names  #ADD: Variables can have different indexs (eg CULAREA)
VarIndex={
 'AcEXTPROD':['nyear', 'ncmarket', 'ncrop'],
 'AcPROD':['nyear', 'nfzone', 'ncrop'],
 'AcSUPPLY':['nyear', 'ncmarket', 'ncrop'],#,'ncdstep'],
 'AcTRANS':['nyear', 'nctrans', 'ncrop'],
 'AlCULAREA':['nyear', 'nfzone', 'nflied', 'nfieldculture'],
 'xCULAREA':['nyear','nfzone','nculture'],
 'xCULYIELD':['nyear','nfzone','nculture'],
 'xTempFactor':['nyear','nfzone','nculture'],
 'AwSUPPLY':['ntime', 'nfzone', 'nculture'],
 'DUMYCROP':['nyear', 'nfzone', 'ncrop'],
 'DUMYEFLOW':['ntime', 'neflow'],
 'DUMYSTOR':['nres'],
 'DUMYWATER':['ntime','ncatch'],
 'EeGENCAP':['nyear', 'nptech','npmarket'],
 'EeGENPROD':['ntime', 'npload', 'nptech','npmarket'],
 'EeHPPROD':['ntime', 'npload', 'nhpp'],
 'EeOPPROD':['ntime', 'npload', 'nopp'],
 'EeSUPPLY':['ntime', 'npload', 'npmarket'],
 'EeTRANS':['ntime', 'npload', 'ntransline'],
 'EwHPDISCHARGE':['ntime', 'npload', 'nhpp'],
 'WwGWSTORAGE':['ntime', 'naquifer'],
 'WwOUTFLOW':['ntime', 'ncatch'],
 'WwRSTORAGE':['ntime', 'nres'],
 'WwSUPPLY':['ntime', 'nuser'],
 'WwTRANSFER':['ntime', 'ntransfer'],
 'JjPROD':['ntime','njactivity'],
 'IbINVEST':['ninvphase','ninvest'],
 'energy_shadow':['ntime','npload','npmarket'],
 'crop_shadow':['nyear','ncmarket','ncrop'],#,'ncdstep'],
 'water_shadow':['ntime','ncatch']}

#load all decision variables (DV) from model runs
scen_to_load=set([s for s in refscen.keys()]+[s for s in refscen.values() if s==s]) 
ScenarioDV={scen:pickle.load(open(os.path.join(result_path,scen+'_DV.txt'),"rb")) for scen in scen_to_load}
#Assemble and Export to csv
REF = 0 if DIFFMODE == 0 else refscen
aggregate_scenarios_to_csv(scenarios,ScenarioDV,DVpath,keytype='tuple',indexname=VarIndex,elist=0,refscen=REF,renamecol=1)


#%%BALANCES (Economic, Energy, Crop, Water)
#create directory
Bpath=os.path.join(result_path,'Balances')
#balances index names
BalIndex={'EconomicBalance':['nyear','ncountry'],
          'EnergyBalance':['ntime','npmarket'],
          'CropBalance':['nyear','ncmarket'],
          'WaterBalance':['ntime','ncatch']}

        
#load all balances from model runs
ScenarioB={scen:pickle.load(open(os.path.join(result_path,scen+'_Balances.txt'),"rb")) for scen in scen_to_load}    
#Assemble and Export to csv
REF = 0 if DIFFMODE == 0 else refscen
aggregate_scenarios_to_csv(scenarios,ScenarioB,Bpath,keytype='nested',indexname=BalIndex,elist=0,refscen=REF,renamecol=0)

#%% IFPRI indicators        
if IFPRI_IDX==1:
    VarIndex={
    'Runoff_Mm3':['nyear','ntime','ncountry'],
    'P_mm':['nyear','ntime','ncountry'],
    'ET_mm':['nyear','ntime','ncountry'],
    'Crop_area_kha':['nyear','ncountry','ncrop','nftype'],
    'Crop_price_dpt':['nyear','ncountry','ncrop'],
    'Crop_prod_kt':['nyear','ncountry','ncrop','nftype'],
    'Crop_val_Md':['nyear','ncountry','ncrop','nftype'],
    'Cul_Dem_mm':['nyear','ncountry','nculture'],
    'Cul_Rain_mm':['nyear','ncountry','nculture'],
    'Cul_Cons_Mm3':['nyear','ntime','ncountry','ncrop'],
    'Cul_Temp_Factor':['nyear','ncountry','nculture','nftype'],
    'Hydropower_prod_GWh':['nyear','ntime','ncountry','nhpp'],
    'Hydropower_val_Md':['nyear','ntime','ncountry','nhpp'],
    'Power_newprod_GWh':['nyear','ntime','ncountry','nhpp'],
    'Power_price_dpkWh':['nyear','ntime','ncountry'],
    'Power_welfare_Md':['nyear','ncountry'],
    'Crop_welfare_Md':['nyear','ncountry'],
    'Crop_impval_Md':['nyear','ncountry','ncrop'],
    'Crop_expval_Md':['nyear','ncountry','ncrop'],
    'Power_expval_Md':['nyear','ncountry'],
    'Power_impval_Md':['nyear','ncountry'],
    'Power_prodcost_Md':['nyear','ncountry'],
    'FZCrop_area_kha':['nyear','ncountry','ncrop','nftype','ncatch'],
    'FZCrop_prod_kt':['nyear','ncountry','ncrop','nftype','ncatch'],
    'FZCrop_val_Md':['nyear','ncountry','ncrop','nftype','ncatch'],
    #'UserDem_p':['nyear','ntime','ncountry','ncatch'],
    'UserDem':['nyear','ntime','ncountry','ncatch'],
    'UserSupply':['nyear','ntime','ncountry','ncatch']
    }
    ScenarioIFPRI={scen:pickle.load(open(os.path.join(result_path,scen+'_ifpri_IDX.txt'),"rb")) for scen in scen_to_load}
    DVpath=os.path.join(result_path,'IFPRI_IDX')
    aggregate_scenarios_to_csv(scenarios,ScenarioIFPRI,DVpath,keytype='tuple',indexname=VarIndex,elist=0,refscen=REF,renamecol=1)
    #%%TABLES
    Data={}
    for var in VarIndex.keys():
        path=os.path.join(DVpath,var+'.csv')
        pdata=pd.read_csv(path)
        Data[var]=pdata
    #read xTempFactor
    #pdata=pd.read_csv(os.path.join(result_path,'DecisionVariables','xTempFactor.csv'))
    #Data['xTempFactor']=pdata
    country='Zimbabwe'
    def selm(var,country=country):
        return Data[var][Data[var]['ncountry']==country]
    
    ny=len(set(Data['Hydropower_prod_GWh']['nyear']))
    writer = pd.ExcelWriter(os.path.join(DVpath,'IDX_'+country+'.xlsx'), engine='openpyxl')
    
    def table_ag(var,index=0,by='sum',select='ncountry',country='Zimbabwe',factor=1/ny,ref=IFPRI_REFSCEN):
        #Add decade
        Data[var]['decade']=Data[var]['nyear'].apply(lambda k:int(str(k-1)[0:3]+'0'))
        #Select country
        if select != 0:
            a=Data[var][Data[var][select]==country]
        else:
            a=Data[var]
        if index != 0:
            if by=='sum':
                b=a.groupby([index,'scenario']).sum(numeric_only=True)[var].unstack(level='scenario')*factor
                d=a.groupby(['decade',index,'scenario']).sum(numeric_only=True)[var].unstack(level='scenario')*factor
            else:
                b=a.groupby([index,'scenario']).mean(numeric_only=True)[var].unstack(level='scenario')*factor
                d=a.groupby(['decade',index,'scenario']).mean(numeric_only=True)[var].unstack(level='scenario')*factor
            #b.join(b.divide(b['observed'],axis=0))
            b.to_excel(writer, sheet_name=index+var)
            br=b.divide(b[ref],axis=0).to_excel(writer, sheet_name='rel'+index+var)
            dr=d.divide(d[ref],axis=0).to_excel(writer, sheet_name='decade_rel'+index+var)
        if by=='sum':
            c=a.groupby(['scenario','decade']).sum(numeric_only=True)[var]*factor
            a=a.groupby(['scenario']).sum(numeric_only=True)[var]*factor       
        else:
            c=a.groupby(['scenario','decade']).mean(numeric_only=True)[var]*factor
            a=a.groupby(['scenario']).mean(numeric_only=True)[var]*factor
        #a.join(a.divide(a['observed'],axis=0))
        a.to_excel(writer, sheet_name=var)
        ar=a.divide(a[ref],axis=0).to_excel(writer, sheet_name='rel'+var)
        cr=c.divide(c[ref],axis=0).to_excel(writer, sheet_name='decade_rel'+var)
    #Climate
    table_ag('Runoff_Mm3',by='sum')
    table_ag('P_mm',by='mean',factor=12)
    #Hydropower prod
    table_ag('Hydropower_prod_GWh',index='nhpp',by='sum')
    table_ag('Hydropower_val_Md',index='nhpp',by='sum')
    table_ag('Power_price_dpkWh',by='mean',factor=1)
    #Economics
    table_ag('Power_welfare_Md',index=0,by='sum')
    table_ag('Power_expval_Md',index=0,by='sum')
    table_ag('Power_impval_Md',index=0,by='sum')
    table_ag('Power_prodcost_Md',index=0,by='sum')
    #Agriculture production
    #economics
    table_ag('Crop_welfare_Md',index=0,by='sum')
    table_ag('Crop_impval_Md',index=0,by='sum')
    table_ag('Crop_expval_Md',index=0,by='sum')
    #yield
    Data['Crop_yield_tpha']=Data['Crop_prod_kt'].copy(deep=True)
    Data['Crop_yield_tpha']['Crop_yield_tpha']=Data['Crop_prod_kt']['Crop_prod_kt']/Data['Crop_area_kha']['Crop_area_kha']
    #prod
    table_ag('Crop_area_kha',index='ncrop',by='sum')
    table_ag('Crop_prod_kt',index='ncrop',by='sum')
    table_ag('Crop_val_Md',index='ncrop',by='sum')
    table_ag('Crop_val_Md',index='nftype',by='sum')
    table_ag('Crop_price_dpt',index='ncrop',by='mean',factor=1)
    table_ag('Crop_yield_tpha',index='ncrop',by='mean',factor=1)
    table_ag('Cul_Temp_Factor',index='nculture',by='mean',factor=1)
    #value
    writer.save()
    writer.close()
    #%% Ma