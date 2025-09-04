This repository stores various scripts used for Colabfit, as well as some additional relevant examples and tools for data ingestion. The main focus is on scripts that utilize Colabfit to process datasets, prepare them for training, and perform analyses with machine learning models. Additionally, the repository includes scripts for data reformatting and ingestion, which can be applied to a variety of molecular datasets.
The goal is to preprocess and transform molecular data into a suitable format for training machine learning models (Machine-learning interatomic potential). These scripts also support the ingestion of various molecular datasets into the MongoDB database for further processing and use in Colabfit workflows.

The main features of this repository include:
Data Reformatting: Scripts that convert raw molecular data into a structured format suitable for training models.
Data Ingestion: Functions to load and insert molecular data into a MongoDB database using Colabfit's tools.
Property Definitions and Mapping: Scripts that define the properties of the data (e.g., energy, forces, dipole moments) for ingestion and analysis.

Example Datasets
The repository includes various example datasets used for training and testing, such as:

Nenci-2021 Dataset: A collection of intermolecular complex data.

Methane Dataset: A set of methane molecules formatted for simulation.

WS22 Dataset: A dataset related to flexible organic molecules.

Various Organic Molecules: Additional datasets from various studies (e.g., TiO2, Graphene, etc.).

These datasets can be used to train models that predict molecular properties, such as energy, dipole moments, and forces.
