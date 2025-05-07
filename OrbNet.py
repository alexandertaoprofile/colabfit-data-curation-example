from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration

import pandas as pd
from ase import Atoms
from ase.io import read
from ase.io.vasp import read_vasp
from ase.db import connect
from tqdm import tqdm
import numpy as np

#call database using its name
#drop_database=True means to start with fresh database
client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)

# In[ ]:

def reader_OrbNet(p):
    df=pd.read_csv('/large_data/new_raw_datasets_2.0/OrbNet_Denali/denali_labels.csv',index_col=0)
    structures=[]
    #atoms=read(p,index=',')
    for row in tqdm(df.index):
        f='/large_data/new_raw_datasets_2.0/OrbNet_Denali/xyz_files/'+str(df.loc[row,'mol_id']+'/'+str(df.loc[row,'sample_id']+'.xyz'))
        structure=read(f)
        #structures_FM.append(structure_FM)
        #print(structure_FM)
        structure.info['energy']=df.loc[row,'dft_energy'].item()
        structure.info['xtb1_energy']=df.loc[row,'xtb1_energy'].item()
        structure.info['charge']=df.loc[row,'charge'].item()
        structures.append(structure)
        #print(type(structure_FM.info['energy_FM']))
        #file_AFM='/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/structures_xyz/'+str(df.loc[row,'xyz_filename_AFM'])
        #structure_AFM=read(file_AFM)
        #structure_AFM.info['energy_AFM']=df.loc[row,'E-AFM(a.u.)'].item()
        #structures_AFM.append(structure_AFM)
        #print(structures_AFM)

    return structures


#Loads data, specify reader function if not "usual" file format
'''
configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/Co_dimer/Co_dimer_data/',
    file_format='folder',
    name_field=None,
    elements=['Co', 'C', 'O', 'H', 'Cl', 'P', 'N','S'],
    default_name='Codimer',
    reader=reader_Codimer,
    glob_string='*.xyz',
    verbose=True,
    generator=False
)
'''
configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/OrbNet_Denali/',
    file_format='folder',
    name_field=None,
    elements=None,
    default_name='OrbNet',
    reader=reader_OrbNet,
    glob_string='denali_labels.csv',
    verbose=True,
    generator=False
)


client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
#client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
#client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')

# property included electronic and dispersion energies, highest occupied molecular orbital (HOMO) and lowest unoccupied molecular orbital (LUMO) energies, HOMO/LUMO gap, dipole moment, and natural charge of the metal center; GFN2-xTB polarizabilities are also provided. Need to decide what to add in the property setting

property_map = {
    'potential-energy': [{
        'energy':   {'field': 'energy',  'units': 'kcal/mol'},
        'per-atom': {'field': 'per-atom', 'units': None},
# For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
        '_metadata': {
           # 'software': {'value':'ENTOS QCORE 0.8.17'},
            'method':{'value':'DFT/ωB97X-D3/def2-TZVP'},
        }
    }],
    
 }
 
def tform(c):
    c.info['per-atom'] = False

# In[ ]:

ids = list(client.insert_data(
    configurations,
    property_map=property_map,
    generator=False,
    transform=tform,
    verbose=True
))

all_co_ids, all_pr_ids = list(zip(*ids))


ds_id = client.insert_dataset(
    do_hashes=all_pr_ids,
    name='Orbnet',
    authors=['Anders S. Christensen', 'Sai Krishna Sirumalla', 'Zhuoran Qiao', 'Michael B. O’Connor', 'Daniel G. A. Smith', 'Feizhi Ding', 'Peter J. Bygrave', 'Animashree Anandkumar', 'Matthew Welborn', 'Frederick R. Manby', 'Thomas F. Miller III'],
    links=[
        'https://aip.scitation.org/doi/10.1063/5.0061990',
        'https://figshare.com/articles/dataset/OrbNet_Denali_Training_Data/14883867',
    ],
    description ='All DFT single-point calculations for the OrbNet Denali '\
    'training set were carried out in ENTOS QCORE version 0.8.17 '\
    'at the ωB97X-D3/def2-TZVP level of theory using in-core '\
    'density fitting with the neese=4 DFT integration grid.',
    resync=True,
    verbose=True,
)

