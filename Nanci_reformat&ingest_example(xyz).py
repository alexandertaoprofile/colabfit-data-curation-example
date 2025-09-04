import os
from colabfit.tools.database import MongoDatabase, load_data
from colabfit.tools.property_settings import PropertySettings
from colabfit.tools.configuration import AtomicConfiguration
import ase
from tqdm import tqdm


# Call database using its name, drop_database=True means to start with fresh database
client = MongoDatabase('new_data_test_alexander', configuration_type=AtomicConfiguration, nprocs=4, drop_database=True)


# Reformat function to handle data formatting
def reformat(file_address):
    """
    Reformats the raw data from the given file.
    """
    do = True
    a = file_address.split('.')
    b = a[0] + '.' + a[1] + '_reformat.' + a[2]
    
    with open(b, 'w') as nf:
        with open(file_address, 'r') as f:
            while do:
                try:
                    # Reading the number of atoms
                    n = int(f.readline())
                    data = f.readline().split(' ')

                    atoms = ""
                    for i in range(n):
                        atoms += f.readline()

                    # Writing formatted data into the new file
                    nf.write('%s\n' % n)
                    nf.write(
                        'CCSD(T)/CBS=%s CCSD(T)/haTZ=%s MP2/haTZ=%s MP2/CBS=%s MP2/aTZ=%s MP2/aQZ=%s HF/haTZ=%s HF/aTZ=%s HF/aQZ=%s SAPT2+/aDZTot=%s Properties=species:S:1:pos:R:3\n' % (
                            data[16], data[18], data[20], data[22], data[24], data[26], data[28], data[30], data[32], data[34]
                        )
                    )
                    nf.write(atoms)
                except Exception as e:
                    print(f"Error processing file {file_address}: {e}")
                    do = False


# Reader function for the MongoDatabase ingestion
def reader(file_path):
    """
    Reads data from files, formats and yields atomic structures.
    """
    file_name = file_path.stem
    atom = ase.io.read(file_path)
    atom.info['name'] = file_name
    yield atom


# Loads data, specify reader function if not "usual" file format
def load_datasets():
    """
    Loads datasets using the defined reader function and the glob_string to find the relevant files.
    """
    configurations = load_data(
        file_path='/large_data/new_raw_datasets_2.0/nenci2021/nenci2021/xyzfiles/',
        file_format='folder',
        name_field='name',
        elements=['C', 'H', 'N', 'O', 'F', 'Cl', 'Br', 'S', 'P'],
        reader=reader,
        glob_string='*reformat.xyz',
        verbose=True,
        generator=False
    )

    return configurations


# Insert property definitions into MongoDB
def insert_property_definitions():
    """
    Inserts property definitions into the database.
    """
    client.insert_property_definition('/home/ubuntu/notebooks/potential-energy.json')
    # client.insert_property_definition('/home/ubuntu/notebooks/atomic-forces.json')
    # client.insert_property_definition('/home/ubuntu/notebooks/cauchy-stress.json')


# Property map for defining energy and metadata
property_map = {
    'potential-energy': [{
        'energy': {'field': 'CCSD(T)/CBS', 'units': 'kcal/mol'},
        'per-atom': {'field': 'per-atom', 'units': None},
        '_metadata': {
            'method': {'value': 'CCSD(T)/CBS'},
        }
    }],
}

# Transform function to modify data format
def tform(c):
    """
    Modify the 'per-atom' field for each configuration.
    """
    c.info['per-atom'] = False


# Ingest the data into MongoDB
def ingest_data(configurations):
    """
    Ingest the formatted data into the database with the property map.
    """
    ids = list(client.insert_data(
        configurations,
        property_map=property_map,
        generator=False,
        transform=tform,
        verbose=True
    ))

    return ids


# Function to insert datasets into MongoDB
def insert_datasets(all_co_ids, all_pr_ids):
    """
    Insert the dataset into MongoDB with additional metadata and configuration.
    """
    cs_info = [
        {"name": "Water", "description": "Configurations with water structure"},
        {"name": "MeOH", "description": "Configurations with MeOH structure"},
    ]
    cs_ids = []

    for i in cs_info:
        cs_id = client.query_and_insert_configuration_set(
            co_hashes=all_co_ids,
            query={'names': {'$regex': i['name'] + '_*'}},
            name=i['name'],
            description=i['description']
        )

        cs_ids.append(cs_id)

    ds_id = client.insert_dataset(
        cs_ids=cs_ids,
        do_hashes=all_pr_ids,
        name='NENCI-2021',
        authors=[
            'Zachary M. Sparrow', 'Brian G. Ernst', 'Paul T. Joo', 'Ka Un Lao', 'Robert A. DiStasio, Jr.'
        ],
        links=[
            'https://pubs.aip.org/aip/jcp/article/155/18/184303/199609/NENCI-2021-I-A-large-benchmark-database-of-non',
        ],
        description='A single file containing the Cartesian coordinates of the 7763 intermolecular complexes in NENCI-2021',
        resync=True,
        verbose=True,
    )


# Main process to run everything
def main():
    """
    Runs the full process of data reformatting, loading, and ingestion.
    """
    # Reformat and process files
    current_address = os.path.dirname(os.path.abspath(__file__))
    file_list = os.listdir(current_address)

    for file_address in file_list:
        if file_address != 'reformat_nanci.py':  # Skip the script itself
            file_path = os.path.join(current_address, file_address)
            reformat(file_path)

    # Load formatted data
    configurations = load_datasets()

    # Insert property definitions
    insert_property_definitions()

    # Ingest data
    ids = ingest_data(configurations)
    all_co_ids, all_pr_ids = list(zip(*ids))

    # Insert datasets
    insert_datasets(all_co_ids, all_pr_ids)


if __name__ == "__main__":
    main()

