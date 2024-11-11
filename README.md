
# Mini Project: PySpark Data Processing
Adil Keku Gazder <br>
ag825, adil.gazder@duke.edu <br>
IDS 706: Data Engineering Systems <br>
Duke University, Fall 2024 <br>
##

### About the project

The aim with this project was to read a .csv file, read it into a database using PySpark and perform CRUD (Create, Read, Update, Delete) operations on the database. 

The dataset used for this project was acquired from Kaggle (Cancer Data -> Cancer_Data.csv) and this dataset was modified to include only the following few columns:
- id
- diagnosis
- radius_mean
- texture_mean
- perimeter_mean
- area_mean
- smoothness_mean

Link to the dataset: (https://www.kaggle.com/datasets/erdemtaha/cancer-data/data?select=Cancer_Data.csv)


##
### Repository Structure


```
ag825_pyspark_data_processing/
├── .devcontainer/
│   ├── devcontainer.json
│   └── Dockerfile
├── .github/
│   └── workflows/CICD.yml
├── .gitignore
├── Dockerfile
├── main.py
├── test_main.py
├── requirements.txt
├── repeat.sh
├── setup.sh
├── LICENSE
├── Cancer_Data.csv
├── README.md
└── output.png
```

##
### Output and Results

The details about the CRUD operations performed have been detailed below:

> [CREATE] create() in main.py
    
- Inserts a new record into the CancerDB.db file (creates a record with unique ID 123123123)

>  [READ] read() in main.py
- Reads the top 5 rows from CancerDB.db
    
>  [UPDATE] update() in main.py

- Updates the value of the diagnosis feild for the record with unique ID: 123123123 in CancerDB.db

> [DELETE] delete() in main.py

- Deletes the record with unique ID: 123123123 in CancerDB.db
