# Dagger: A Data (not code) Debugger

Python data debugger

""" Dagger (Data Debugger) is an end-to-end data debugger that abstracts key data-centric primitives to enable users to quickly identify and mitigate data-related problems in a given pipeline. """ - Quote from the [Dagger paper](https://github.com/mschoema/dagger/blob/master/papers/Dagger%20A%20Data%20(not%20code)%20Debugger.pdf)

## Execution

### Prerequisits
 - python 3
 - postgresql
 
### Prepare database

The first step is to create a new database that will be used by dagger.
The drop and reset commands described later will remove all tables in the database, so make sure to not have any other tables in your database or you will lose them.

To create the needed tables in your database, modify the [database config file](https://github.com/mschoema/dagger/blob/master/database.ini) and then navigate to your [src directory](https://github.com/mschoema/dagger/tree/master/src) and type the folowing command:

```shell
python create_tables.py [-c | create]
```

There is currently no script or run versioning, so it is best to clear/reset the database between every run:
```shell
python create_tables.py [-r | reset]
```

To drop the tables, write:
```shell
python create_tables.py [-d | drop]
```

### Run dagger

To run a python script file using dagger, navigate to your [src directory](https://github.com/mschoema/dagger/tree/master/src) and type the following command:

```shell
python run.py <path_to_script> <parameters>
```

### Parameters

#### Blocks
Define the blocks of code where dagger will log the variables.

Code structures like functions, if blocks, for/while loops cannot be split into multiple blocks
and will always be contained entirely in one block.

To log only part of a function, for the moment it is needed to split the function in two and log only the one that is wanted

Example usage:
```shell
python run.py <path_to_script> [-b | --blocks] (12,25) (50,125) <other parameters>
```

#### Modifier attribute functions (Mafs)

The variable logging is automatically done when a variable is assigned.
To also log a variable after an attribute function modified it, this function has to be specified to dagger

For example the following code will modify an array without any assignement statement:
```python
24: my_list1.sort()
25: my_list1.pop()
```
So to have dagger save the variable after calls like this, run the follwing command:
```shell
python run.py <path_to_script> -b (24,25) [-maf | --modifier_attr_fcts] sort pop <other parameters>
```
The functions 'append', 'pop' and 'sort' are already defined as modifier attribute functions in the code and do not need to be defined again.

#### Delta logging

Dataframes can be saved in the database using delta logging to save memory space.
To do this use the parameter '-dl' when running dagger:
```shell
python run.py <path_to_script> -dl <other parameters>
```

#### Split primitive

Dagger implements a split primitive allowing the user to partition a dataframe at one point during the execution of the code
and run the rest of the script once for every partition.

To do this add a split command to the parameters when running dagger:
```shell
python run.py <path_to_script> <delta_logging> <blocks> <mafs> [-s | --split] "<split_command>"
```

The split command syntax is as follows:
```
SPLIT <dataframe_name> WHERE <column_name> <comparison_operator> <comparison_value> ON BLOCK <block_number>
```
List of possible comparison operators: \[<, <=, =, !=, >=, >\]
The block number must not exceed the amount of blocks defined

### Example scripts

Two [example scripts](https://github.com/mschoema/dagger/tree/master/src/test_scripts) manipulating dataframes in a data preprocessing and machine learning pipeline are given to test out dagger.

#### Taxifare prediction
The [script](https://github.com/mschoema/dagger/blob/master/src/test_scripts/taxifare-prediction-with-keras.py) is a modified version of a [notebook](https://www.kaggle.com/dolel22/taxifare-prediction-with-keras) created for the [New York taxi fare prediction competition](https://www.kaggle.com/c/new-york-city-taxi-fare-prediction/overview) from Kaggle.
To run the script, the train and test files have to be downloaded from Kaggle and placed in the [test_scripts folder](https://github.com/mschoema/dagger/tree/master/src/test_scripts) under the names taxi_test.csv and taxi_train.csv respectively.

Here is an example of how this code might be run using dagger:
```shell
python run.py test_scripts/taxifare-prediction-with-keras.py -dl -b (34,45) (69,110) (112,113) -s "SPLIT df WHERE fare_amount > 15 ON BLOCK 3"
```
This code would save the training dataframe (df) during the execution and run the script from line 112 on twice, every time using a different partition. (Once with all records with fare_amount > 15 and once with all records with fare_amount <= 15)
The delta logging doesn't handle column type changes in dataframes yet, so that is why the first block start at 34, after all the column types have been defined correctly.

#### Titanic data
The [script](https://github.com/mschoema/dagger/blob/master/src/test_scripts/titanic_data.py) comes from a notebook created for the [Titanic competition](https://www.kaggle.com/c/titanic/overview) from Kaggle.
The data is already in the test_scripts folder, so the code can be run as it is.

This script is not very suited  for dagger currently, because for-loops are not correctly handled yet, but this is an example of how we might run this script using dagger:
```shell
python run.py test_scripts/titanic_data.py -dl -b (9,18) (31,52)
```
This would save all the variables up to line 52

## Future improvements
 - Improved delta logging
 - Better code handling (ex: for/while-loops, user-defined classes, ...)
 - More debugging primitives:
    * Data breakpoints
    * Data generalization
    * Pipeline and data comparison
 - Dagger Query Language (DQL) to allow users to interact with and query the data saved during the execution
 
## Issues

For any issues or questions related to the code, open a new git issue or send a mail to maxime.schoemans@ulb.ac.be

## License

Dagger is licensed under the terms of the MIT License (see the file
[LICENSE](https://github.com/mschoema/dagger/blob/master/LICENSE)).
