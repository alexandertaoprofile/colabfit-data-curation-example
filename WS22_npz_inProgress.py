from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration


from tqdm import tqdm
import numpy as np
from ase import Atoms

#call database using its name
#drop_database=True means to start with fresh database
client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)

#multiple properties
# In[ ]:
def reader_ws22(p):
    atoms=[]
    a=np.load(p)
    #na=a['N']
    z=a['Z']
    e=a['E']
    r=a['R']
    f=a['F']
    hl=a['HL']
    d=a['DP']
    #q=a['nuclear_charges']
    #for i in tqdm(range(len(na))):  #need to change it
    for i in tqdm(1200):
        #n=na[i]
        #atom = Atoms(numbers=z[i, :], positions=r[i, :n, :])
        atom=Atoms(numbers=z,positions=r[i])
        #atom.info['energy']=e[i]
        atom.info['energy']=float(e[i])
        atom.arrays['forces']=f[i]
        atom.info['dipole_moment']=d[i]
        atom.info['homolumo']=hl[i]
        #atom.info['charge']=float(q[i])
        #print(atom.info['charge'])
        atoms.append(atom)
        #print(type (atom.info['charge']))
    return atoms
    
 #Loads data, specify reader function if not "usual" file format
configurations = load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_acrolein',
    reader=reader_ws22,
    glob_string='acrolein',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_nitrophenol',
    reader=reader_ws22,
    glob_string='nitrophenol',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_dmabn',
    reader=reader_ws22,
    glob_string='dmabn',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_sma',
    reader=reader_ws22,
    glob_string='sma',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_urea',
    reader=reader_ws22,
    glob_string='urea',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_thymine',
    reader=reader_ws22,
    glob_string='thymine',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_urocanic',
    reader=reader_ws22,
    glob_string='urocanic',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_alanine',
    reader=reader_ws22,
    glob_string='alanine',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_o-hbdi',
    reader=reader_ws22,
    glob_string='o-hbdi',
    verbose=True,
    generator=False
)

configurations += load_data(
    file_path='/large_data/new_raw_datasets_2.0/WS22_database',
    file_format='folder',
    name_field=None,
    elements=['C','N','O','H'],
    default_name='ws22_toluene',
    reader=reader_ws22,
    glob_string='toluene',
    verbose=True,
    generator=False
)



client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
#client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')


# In[ ]:


property_map = {

    'potential-energy': [{
        'energy':   {'field': 'energy',  'units': 'kcal/mol'},
        'per-atom': {'field': 'per-atom', 'units': None},
# For metadata want: software, method (DFT-XC Functional), basis information, more generic parameters
        '_metadata': {
            'software': {'value':'ORCA 4.0.1'},
            'method':{'value':'PBE0/6-311G*'},
        }
    }],

    'atomic-forces': [{
        'forces':   {'field': 'forces',  'units': 'kcal/mol/A'},
        '_metadata': {
            'software': {'value':'ORCA 4.0.1'},
            'method':{'value':'PBE0/6-311G*'},

        }

    }],

#    'cauchy-stress': [{
#    'stress':   {'field': 'virial',  'units': 'GPa'},

#                '_metadata': {
#            'software': {'value':'VASP'},
#        }

#    }],

    }

# In[ ]:

def tform(c):
    c.info['per-atom'] = False


# In[ ]:


ids = list(client.insert_data(
    configurations,
    property_map=property_map,
    #generator=False,
    transform=tform,
    verbose=True
))

#all_co_ids, all_pr_ids = list(zip(*ids))
all_cos, all_dos = list(zip(*ids))


cs_info = [

    {"name":"acrolein",
    "description": "Configurations with acrolein structure)"},

    {"name": "nitrophenol",
    "description": "Configurations with nitrophenol structure"},

    {"name": "dmabn",
    "description": "Configurations with dmabn structure"},

    {"name": "sma",
    "description": "Configurations with sma structure"},

    {"name": "urea",
    "description": "Configurations with urea structure"},

    {"name":"thymine",
    "description": "Configurations with thymine structure"},

    {"name": "urocanic",
    "description": "Configurations with urocanic structure"},

    {"name": "alanine",
    "description": "Configurations with alanine structure"},

    {"name": "o-hbdi",
    "description": "Configurations with o-hbdi structure"},

    {"name": "toluene",
    "description": "Configurations with toluene structure"},
]

cs_ids = []


for i in cs_info:
    cs_id = client.query_and_insert_configuration_set(
        co_hashes=all_cos,
        query={'names':{'$regex':i['name']+'_*'}},
        name=i['name'],
        description=i['description']
    )

    cs_ids.append(cs_id)



# In[ ]:


ds_id = client.insert_dataset(
    #cs_ids=cs_ids,
    do_hashes=all_dos,
    name='WS22',
    authors=['Pinheiro Jr', 'M., Zhang', 'S., Dral', 'P. O.','Barbatti, M.'],
    links=[
        'https://www.nature.com/articles/s41597-023-01998-3#code-availability',
        'https://zenodo.org/record/7032334#.ZEDJes7MJEY',
    ],
    description='The WS22 database combines Wigner sampling with geometry interpolation to generate 1.18 '\
    'million molecular geometries equally distributed into 10 independent datasets of flexible '\
    'organic molecules with varying sizes and chemical complexity. '\
    'In addition to the potential energy and forces required to construct potential energy surfaces, the WS22 '\
    'database provides several other quantum chemical properties, all obtained via single-point calculations '\
    'for each molecular geometry. All quantum chemical calculations were performed with the Gaussian09 program.',

    resync=True,
    verbose=True,

)
