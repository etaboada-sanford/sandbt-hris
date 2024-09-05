[![Developer: Edgar](https://img.shields.io/badge/developer-edgar-blue)](mailto:etaboada@sanford.co.nz)

# About the Project

This is Sanford's D365 Finance and Operations DBT project.

## Pre-requisites

To begin working with this project, please ensure that the following tools are available and pre-requisite setup is done:

1. [Visual Studio Code](https://code.visualstudio.com/download)
2. [Git Bash](https://git-scm.com/download/win) - if working on Windows machine, this is to run shell scripts 
3. [Python3](https://www.python.org/downloads/release/python-3120/) - idealy installed via Chocolatey: choco install python312
4. [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
5. [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows?tabs=azure-cli) - install with winget as admin: winget install -e --id Microsoft.AzureCLI
6. [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16#download-for-windows)

1. In your VS Code workspace, clone the following repository:
```bash
git clone https://sanfordltd@dev.azure.com/sanfordltd/Data%20and%20Analytics/_git/sandbt↵
```

The folders are arranged as:
```
  [sandbt] (root folder)
      \_ [.vscode] (folder containing the Visual Studio Code recommended settings and extensions)
      \_ [dags] (Apache Airflow DAG folder)
         \_ [fabric_d365] (dbt project for D365 FNO)
            \_ [analyses] (dbt analyses folder)
            \_ [macros] (dbt macros folder)
            \_ [models] (dbt models folder)
            \_ [scripts] (project supporting scripts)
            \_ [seeds] (dbt seeds folder)
            \_ [snapshots] (dbt snapshots folder)
            \_ [tests] (dbt tests folder)
            \_ d365_tables.yml (configuration files containing the list of D365 FNO tables for reporting)
            \_ dbt_project.yml (dbt project file)
            \_ packages.yml (sandbt project package dependencies)
         \_ requirements.txt (pip install requirements file)
      \_ [plugins] (Apache Airflow plugins folder)
      \_ developer_setup.sh (Shell script to setup developer project environment)

```

2. Launch Git Bash in administrator mode and make sure you are in the project directory. If not, then change to the project directory directory. You should see a (main) prompt indicating that you are in the main branch of the sandbt project:
```bash
$ cd /c/code/sandbt↵
MINGW64 /c/code/sandbt (main)
```

3. Setup your development environment variables by executing the following script:
```bash
MINGW64 /c/code/sandbt (main)
$ ./developer_setup.sh↵ [optional: supply the python executable command e.g. py or python (default is python)]
```
This script will create your python virtual environment in the .venv folder. A virtual python environment ensures that you have an isolated and controlled environment separate from your local python installation. This controls that you only develop on managed python packages and versions that are expected to be deployed in test/production environment. You need to be working in this virtual environment everytime you do coding. These packages are configured in the dags/requirements.txt file.

Important Note: To ensure that the setup is completed without fail, it is advised to run developer_setup.sh with elevated permissions.

4. Activate the virtual environment. When activated, the shell will have a (.venv) prompt displayed
```bash
MINGW64 /c/code/sandbt (main)
$ source .venv/Scripts/activate↵
```

Important Note: When launching a new terminal in Visual Studio code, you will be taken to the ${workspaceFolder} which is the root folder of the project i.e. './sandbt'. Before performing any commands, make sure that the (.venv) is displayed in the prompt indicating that your python virtual environment is active. If it's not, perform step 4. You can also run the shortcut alias 'sandbt' from the terminal which will do the same thing.

## Getting started

### The following are the steps to get started working with this project.

1. Activate your python virtual environment from the project root folder. Make sure that the (.venv) is visible in the prompt

```bash
MINGW64 /c/code/sandbt (main)
$ source .venv/Scripts/activate↵

(.venv)
MINGW64 /c/code/sandbt/dags/fabric_d365 (main)
```

or

```bash
MINGW64 /c/code/sandbt (main)
$ sandbt↵

(.venv)
MINGW64 /c/code/sandbt/dags/fabric_d365 (main)
```

2. Authenticate using 'az login':

```bash
(.venv)
MINGW64 /c/code/sandbt/dags/fabric_d365 (main)
$ az login↵
```

A popup dialog may come up. Login/select your username.

The terminal will return the following and select 1 from the options.

```bash
Please select the account you want to log in with.

Retrieving tenants and subscriptions for the selection...
The following tenants don't contain accessible subscriptions. Use `az login --allow-no-subscriptions` to have tenant level access.
5bf4dad5-915d-4127-aae9-15494b44ba52 'LMS365 Sanford Ltd'
72f988bf-86f1-41af-91ab-2d7cd011db47 'Microsoft'

[Tenant and subscription selection]

No     Subscription name                     Subscription ID                       Tenant
-----  ------------------------------------  ------------------------------------  ---------------
[1] *  Microsoft Azure Enterprise DEV        c2ceb3d7-c1c1-44fb-a6f6-c6d8ab177d3b  SANFORD LIMITED
[2]    Microsoft Azure Enterprise PROD       269081e2-99db-42c7-9356-3f97de13146c  SANFORD LIMITED
[3]    Visual Studio Professional Subscr...  9bb38411-be38-479a-ab2f-a123768c8f08  SANFORD LIMITED

The default is marked with an *; the default tenant is 'SANFORD LIMITED' and subscription is 'Microsoft Azure Enterprise DEV' (c2ceb3d7-c1c1-44fb-a6f6-c6d8ab177d3b).

Select a subscription and tenant (Type a number or Enter for no changes):
```

3. Do preliminary checks by executing 'dbt debug':

```bash
(.venv)
MINGW64 /c/code/sandbt/dags/fabric_d365 (main)
$ dbt debug↵
```

You should get an output similar to this:

```bash
01:47:25  Running with dbt=1.8.2
01:47:25  dbt version: 1.8.2
01:47:25  python version: 3.12.4
01:47:25  python path: D:\code\sandbt\.venv\Scripts\python.exe
01:47:25  os info: Windows-11-10.0.22631-SP0
01:47:26  Using profiles dir at D:\code\sandbt\dags\fabric_d365
01:47:26  Using profiles.yml file at D:\code\sandbt\dags\fabric_d365\profiles.yml
01:47:26  Using dbt_project.yml file at D:\code\sandbt\dags\fabric_d365\dbt_project.yml
01:47:26  adapter type: fabric
01:47:26  adapter version: 1.8.6
01:47:27  Configuration:
01:47:27    profiles.yml file [OK found and valid]
01:47:27    dbt_project.yml file [OK found and valid]
01:47:27  Required dependencies:
01:47:27   - git [OK found]

01:47:27  Connection:
01:47:27    server: b27nglr6dgderlhlbqidfj2kge-pwo43isi2ezetbspsg37pqmaqa.datawarehouse.fabric.microsoft.com
01:47:27    database: dwh_sanford
01:47:27    schema: dbo
01:47:27    UID: None
01:47:27    client_id: None
01:47:27    authentication: CLI
01:47:27    encrypt: True
01:47:27    trust_cert: False
01:47:27    retries: 1
01:47:27    login_timeout: 0
01:47:27    query_timeout: 0
01:47:27    trace_flag: False
01:47:27  Registered adapter: fabric=1.8.6
01:47:37    Connection test: [OK connection ok]

01:47:37  All checks passed!
```

Important Note: For development, please make sure that the output is showing that dbt is using the profiles.yml inside the project and not in the .dbt folder. This is for easy maintenance and to ensure that the developers only use their own credentials when accessing fabric resources and not the credential of a service account or someone else's.

4. Once authenticated, you can now start to initialize the project by running the command:

```bash
(.venv)
MINGW64 /c/code/sandbt/dags/fabric_d365 (main)
$ dbt deps↵
```

5. If you want to build the whole D365 dbt fabric project from scratch or if you want to add new source tables, edit the d365_tables.yml file under the dags/fabric_d365 folder, and run the following script:

```bash
(.venv)
MINGW64 /c/code/sandbt/fabric_d365 (main)
$ python ./scripts/init.py
```

The script will do the following:

1. Read the d365_tables.yml file which contains all the D365 tables required for reporting
2. Connect to the lakehouse sources to query the columns of the table as provided in the the d365_tables.yml
3. Build the dbt sources.yml file and store it in ./models/<medallion architecture layer>/source_<sourcename>.yml
