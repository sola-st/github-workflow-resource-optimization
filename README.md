# Resource Usage and Optimization Opportunities in Workflows of GitHub Actions



[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8344575.svg)](https://doi.org/10.5281/zenodo.8344575) [<img src="https://img.shields.io/badge/dockerhub-GH_workflows_study-blue.svg?logo=Docker">](https://hub.docker.com/r/islemdockerdev/github-workflow-resource-study)

This artifact submission includes the code and data utilized in our empirical study described in the [ICSE'24 paper "Resource Usage and Optimization Opportunities in Workflows of GitHub Actions"](https://software-lab.org/publications/icse2024_workflows.pdf). The study produces two primary outputs:

1. **Dataset:** We collected a dataset comprising 1.3 million runs with 3.7 million jobs.

2. **Analysis Code and Results:** The code and results of our analysis conducted on the collected dataset.

Both components can be accessed and explored using the instructions provided below.

## Badges to claim
To acknowledge our artifact, we seek to claim two badges:
1. **Available badge**
2. **Reusable badge**

## Requirements
The following sections outline the requirements for importing, installing, and using our artifact.
### User Requirements
1. **Familiarity with Python Virtual Environment**: Ability to create a Python virtual environment and install our tool within it. Step-by-step instructions are provided in this README file, but prior knowledge of this task is advantageous.

2. **Working with Jupyter Notebooks**: Capability to install and launch JupyterLab notebooks. Detailed instructions will be provided.

3. **Working with VScode**: Capability to install and launch VScode. Detailed instructions will be provided.

### Hardware Requirements
1. **Disk Space**: A minimum of 8GB disk space is required for our collected data, external data files, and our portable Python environment.
	
2. **RAM free space**: Some data preprocessing requires substantial RAM space. In our experiments, usage can extend up to 9GB of RAM for a single analysis notebook (on top of the overhead of other software such as VScode, Docker or JupyerLab when needed).

3. **CPU cores**: None of our scripts use multiprocessing. However, for reference, we conducted our experiments on a machine with 48 CPU cores. The time taken by our machine is specified within each code notebook.

### Software Requirements
1. **Operating System and Docker**: Our scripts are designed to run on a Linux machine (Ubuntu 20.04). For optimal compatibility, it is recommended to rerun them on an Ubuntu machine with a version close to 20.04. We also recommend the usage of Docker v20.x or higher.

2. **Python Version**: The implementation and used packages are compatible with Python 3.8 (recommended) and above.

3. **Jupyter-Lab or VScode**: Since we use notebooks to present our analysis, you will need a notebook runner. It is preferable to have JupyterLab or VSCode installed for running notebooks.

4. **GitHub Username and Token**: To execute the collection process using the GitHub API, you need to add at least one token to the file tokens.txt (see data collection part below).

## How to setup the artifact?
You can setup the artifact following one of the three options below:

### Option 1: Docker image from DockerHub:
The perhaps most straightforward way to utilize our artifact is by pulling our Docker image from DockerHub. Follow these steps:
1. Execute the following commands in your terminal to retrieve and start our docker image
    ```bash
    # Pull image
    docker pull islemdockerdev/github-workflow-resource-study:v1.0
    # Run the image inside a container
    docker run -itd --name github-study islemdockerdev/github-workflow-resource-study:v1.0
    # Start the container
    docker start -i github-study
    ```

2. After starting the container, open VSCode and navigate to the containers icon on the left panel (Ensure that you have the Remote Explorer extension installed on your VScode).

3. Under the Dev Containers tab, locate the name of the container you just started (e.g., github-study).

4. Finally, attach the container **github-study** to a new window by clicking the '+' sign located on the right of the container's name. Navigate to **workdir** folder in vscode window (all the files of the artifact are located there).

**Tutorial reference:** For detailed steps on how to attach a docker container in VScode, please refer to this short video tutorial (1min 38 sec): https://www.youtube.com/watch?v=8gUtN5j4QnY&t

### Option 2: Get our Zenodo image
1. Download our shared Zip from: https://doi.org/10.5281/zenodo.8344575

2. After unzipping the file, load our Docker image using the command:
    ```bash
    # load the container
    docker image load -i ./github_study_container.tar
    # run and attach (careful with the name)
    docker run -it islemdockerdev/github-workflow-resource-study:v1.0 
    ```
3. Attach the docker container to a vscode window as demonstrated in here: https://www.youtube.com/watch?v=8gUtN5j4QnY&t
4. Navigate to the folder **workdir** in vscode window (all the files of the artifact are located there).

### Option 3: Manually prepare the environement

If you prefer a manual setup, follow these steps. 

1. Ensure you have Python 3.8 or higher installed. In your terminal, in the same folder as this README file, execute the following commands:

```bash
# create a python environement, preferably use Python 3.8 or higher
python3.8 -m venv .venv
# activate the environement
source .venv/bin/activate
# install requirements
pip install -r requirements.txt
# install the current folder as a Python package
pip install .
# install jupyterlab
pip install jupyterlab
# launch jupyterlab
jupyter-lab
```

2. Following that, download our dataset release from: https://github.com/sola-st/github-workflow-resource-optimization/releases/tag/v1.0.0
3. Unzip the dataset in the same folder as this README file.

## Running the Artifact

### Data Collection
In this part, we present the necessary resources and instructions to replicate our data collection process (or collect more data).

#### Collection resources
* [repositories.csv](./repositories.csv): This is the list of popular repositories ([reference to this work](https://decan.lexpage.net/files/ICSME-2022.pdf)) from which we randomly sampled 600 repositories to use them in our study.

* [repositories-2021-03-08.zip](./repositories-2021-03-08.csv): The list containing unpopular repositories ([reference to this work](https://arxiv.org/abs/2103.04682)) from which we randomly sampled 74K candidates to collect workflows data. We ended up with 352 repositories having such data.

* [Collection notebook](./collect.ipynb): This is the code (in notebook format) used to collect our data using the two repositories lists as input.


#### How to run?
To run the collection code:
1. Open the notebook [Collection notebook](./collect.ipynb).
2. Open the file [tokens.json](./tokens.json) and add your GitHub tokens into the file. To have a non-interrupted collection, we used 4 tokens at a time granting 20K requests per hour in total.
Here is an example of how the tokens.json file should look like:
    ```json
    [
        "here you put token 1",
        "here you put token 2",
    ]
    ```
3. Run the cells of the notebook one after another.

#### Resulting Data
Applying the above collection process we obtained the following data files (In case you are viewing this on GitHub, the links below wouldn't work but you can find all the files within [this relase](https://github.com/sola-st/github-workflow-resource-optimization/releases/tag/v1.0.0)):
* [all_runs.csv](./all_runs.csv)
* [all_job.csv](./all_job.csv)
* [all_steps.csv](./all_steps.csv)
* [all_commits.csv](./all_commits.csv)
* [all_actors.csv](./all_actors.csv)
* [all_repositories.csv](./all_repositories.csv)
* [all_pull_steps.csv](./all_pull_steps.csv)


### Research Questions
#### 1. Reproducing Results of RQ1
This notebook contains the reproduction of the results of RQ1, mainly summarized in Tables 1, 2 and 3 of the PDF.

After Opening the notebook in VScode, click on RunAll as shown [here](https://code.visualstudio.com/docs/datascience/jupyter-notebooks)

*Important: It takes around 4 minutes to run this notebook on our machine and consumes around 9GB of RAM*

#### 2. Reproducing Results of RQ2
This notebook contains the reproduction of the results of RQ2, mainly summarized in Table 4 of the PDF.

After Opening the notebook in VScode, click on RunAll as shown [here](https://code.visualstudio.com/docs/datascience/jupyter-notebooks)

*Important: It takes around 13 minutes to run this notebook on our machine and consumes around 9GB of RAM*

#### 3. Reproducing Results of RQ3
This notebook contains the reproduction of the results of RQ1, mainly summarized in Tables 5 and 6 of the PDF.

After Opening the notebook in VScode, click on RunAll as shown [here](https://code.visualstudio.com/docs/datascience/jupyter-notebooks)

*Important: It takes around 17 minutes to run this notebook on our machine and consumes around 9GB of RAM*
