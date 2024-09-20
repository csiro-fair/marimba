# Pipeline Implementation Guide

A Marimba Pipeline is the core component responsible for processing data from a specific instruments or 
multi-instrument systems. Pipelines contain the complete logic required to transform raw data into FAIR-compliant 
datasets, allowing you to automate complex workflows tailored to the unique requirements of your data.

By developing a custom Pipeline, you can take advantage of Marimba's powerful features while integrating your own 
specialised processing steps. This guide will walk you through everything you need to know to create and implement 
your own Marimba Pipeline, from setting up the structure of your Pipeline and implementing key methods (`_import`, 
`_process` and `_package`), to making use of the available processing capabilities the Marimba standard library.

---

## Table of Contents

1. [Introduction to Pipelines](#introduction-to-pipelines)
2. [Understanding Marimba Pipelines](#understanding-marimba-pipelines)
   - [What Is a Marimba Pipeline?](#what-is-a-marimba-pipeline)
   - [Pipeline Components](#pipeline-components)
3. [Setting Up a New Pipeline](#setting-up-a-new-pipeline)
   - [Initializing the Pipeline](#initializing-the-pipeline)
   - [Managing Dependencies](#managing-dependencies)
4. [Implementing Your Pipeline](#implementing-your-pipeline)
   - [Implementing the `get_pipeline_config_schema` Method](#implementing-the-get_pipeline_config_schema-method)
        - [Example `get_pipeline_config_schema` Implementation](#example-get_pipeline_config_schema-implementation)
   - [Implementing the `get_collection_config_schema` Method](#implementing-the-get_collection_config_schema-method)
        - [Example `get_collection_config_schema` Implementation](#example-get_collection_config_schema-implementation)
   - [Implementing the `_import` Method](#implementing-the-_import-method)
        - [Example `_import` Implementation](#example-_import-implementation)
        - [Dry Run Mode](#dry-run-mode)
        - [Marimba Logging](#marimba-logging)
        - [Utilizing the `operation` Option](#utilizing-the-operation-option)
        - [Executing the `_import` Method](#executing-the-_import-method)
        - [Metadata Handling in Marimba Pipelines](#metadata-handling-in-marimba-pipelines)
           - [Pipeline-Level Metadata](#pipeline-level-metadata)
           - [Collection-Level Metadata](#collection-level-metadata)
           - [Metadata Handling Summary](#metadata-handling-summary)
       - [Defining and Using Custom `kwargs`](#defining-and-using-custom-kwargs)
            - [How to Pass Custom `kwargs` Through the CLI](#how-to-pass-custom-kwargs-through-the-cli)
            - [Accessing `kwargs` in a Marimba Pipeline](#accessing-kwargs-in-a-marimba-pipeline)
   - [Implementing the `_process` Method](#implementing-the-_process-method)
        - [Example `_process` Implementation](#example-_process-implementation)
        - [Executing the `_process` Method](#executing-the-_process-method)
          - [Targeted Processing with Specific Pipelines and Collections](#targeted-processing-with-specific-pipelines-and-collections)
        - [Multithreaded Thumbnail Generation](#multithreaded-thumbnail-generation)
   - [Implementing the `_package` Method](#implementing-the-_package-method)
        - [Example `_package` Implementation](#example-_package-implementation)
        - [Executing the `_package` Method](#executing-the-_package-method)
5. [Advanced Topics](#advanced-topics)
   - [Multi-Level iFDO Files](#multi-level-ifdo-files)
   - [Multi-Level Summary Files](#multi-level-summary-files)
6. [Conclusion and Next Steps](#conclusion-and-next-steps)

---

## Introduction to Pipelines

Marimba is designed with a clear distinction between the core Marimba system and user-authored Pipelines that are
responsible for processing data from single or multi-instrument systems. This modular architecture allows you to 
create custom Pipelines designed to your specific data processing requirements, while minimizing the amount of 
boilerplate code required to achieve an operational processing system for your instruments.

Within a Marimba Project, the core Marimba system manages the overall workflow, including the execution of key 
Pipeline methods (`_import`, `_process` and `_package`), the capture of processing logs, and the final packaging of 
datasets. The Pipeline itself is only responsible for implementing the necessary logic and code specific to the 
instruments or systems it is designed to process.

---

## Understanding Marimba Pipelines

### What Is a Marimba Pipeline?

A Marimba Pipeline is a Python file that defines the processing logic for data from a specific instrument or 
multi-instrument system. It is executed from within the context of a Marimba Project and interacts with the core 
Marimba system to perform tasks such as importing data, processing them, and packaging the final datasets.

Marimba Pipelines inherit from the Marimba `BasePipeline` class and must implement the specific methods that define 
their behavior. This design provides several key advantages:

- **Modularity**: Pipelines can be developed, tested, and maintained independently
- **Reusability**: Pipelines can be shared and reused across different projects
- **Customization**: Each Pipeline can be tailored to handle the unique requirements of different instruments and data 
  sources


### Pipeline Components

Understanding the structure and components of a Marimba Pipeline is essential for successful implementation. All custom 
Pipelines must inherit from the
[BasePipeline](https://github.com/csiro-fair/marimba/blob/docs/user-and-developer-docs/marimba/core/pipeline.py)
class provided by Marimba. The `BasePipeline` class defines the interface that your Pipeline must implement, 
including:

- `get_pipeline_config_schema()`: 
  - This method returns a dictionary schema that users will be prompted to complete when adding a new Marimba 
    Pipeline to a Marimba Project.
  - In this schema, you define the Pipeline-level metadata to be captured, such as `voyage_id`, `voyage_pi`, 
    `platform_id`, `start_date`, `end_date`, `data_collector`, etc.
  - The result of this metadata capture is stored in a `pipeline.yml` file located in the Pipeline’s root directory. 
    This metadata is then made available by Marimba throughout all stages of Pipeline processing.


- `get_collection_config_schema()`:
  - This method returns a dictionary schema that users will be prompted to complete when adding a new Marimba 
    Collection to a Marimba Project.
  - In this schema, you define the Collection-level metadata to be captured, such as `deployment_id`, `site_id`, 
    `batch_id`, `data_collector`, etc. 
  - The result of this metadata capture is stored in a `collection.yml` file located in the Collection’s root 
    directory. This metadata is then made available by Marimba throughout all stages of Pipeline processing.


- `_import()`:
  - This method manages the importation of raw data into a designated Marimba Collection. It is designed to be 
    implemented for a single import source, such as a folder on a hard disk or a mounted external storage device. 
    Core Marimba orchestrates the execution of this method across each installed Pipeline for every specified import 
    source within the Collection.
  - The primary function of this method is to ensure that all raw data files are correctly transferred into the 
    designated Collection directory, ready for subsequent processing stages. During this step, there is also the 
    option to implement hierarchical directory structuring and renaming of files to enhance data organization.
  - This import process is useful for maintaining the integrity and organization of data within a Marimba Project, 
    enabling effective and efficient data management across the entire Pipeline workflow.

    
- `_process()`:
  - This method contains the core processing logic of the Pipeline. It is responsible for applying specific 
    transformations, analyses, and enhancements to the imported data within the Marimba Collection.
  - The `_process` method should be designed to handle the unique requirements and characteristics of the data, and 
    can include processing steps such as converting image file formats, transcoding video files, generating image 
    thumbnails, and merging sensor data etc. 
  - Once the `_process` method has been executed, the data should be ready for the final packaging stage.


- `_package()`:
  - This method prepares the processed data for packaging, organizing the final assembly of the dataset. It is 
    designed to be implemented for a single Collection, with core Marimba orchestrating the packaging of each 
    Pipeline against every Collection, unless selected Pipelines and Collections are specified.
  - The `_package` method is responsible for returning a data mapping that specifies all the files to be included in 
    the final dataset. This process typically involves recursively scanning all files within a Collection to 
    assemble the necessary data mapping.
  - For each image and/or video file, the data mapping can include an
    [ImageData](https://github.com/kevinsbarnard/ifdo-py/blob/main/ifdo/models.py#L177) object, which is a Python 
    implementation of the [iFDO](https://marine-imaging.com/fair/ifdos/iFDO-overview/) specification.
  - Files listed in the data mapping that contain an iFDO will have their metadata embedded in a dataset-level iFDO 
    output file named `ifdo.yml`. Additionally, this metadata will be burned into the EXIF metadata of any included JPEG 
    file, adhering to FAIR data standards.

---

## Setting Up a New Pipeline

Marimba Pipelines have been designed to be set up in their own git repositories. This approach offers the benefits 
of version control and modularity, allowing for better management and tracking of changes. We recommend setting up 
a free GitHub account and creating a repository there to host your new Marimba Pipeline.

The simplest structure for a Marimba Pipeline is as follows:

```plaintext
my-pipeline
├── requirements.txt     # Additional Pipeline-level dependencies required by the Pipeline
└── my.pipeline.py       # The Marimba Pipeline implementation
```

Pipeline authors have full control over their repository's structure, which means you can include additional 
directories and files as necessary. For example:

```plaintext
my-pipeline
├── data                            # Optional directory containing any Pipeline-level ancilary data
├──└── platform_data.csv         # Any additional files, scripts or resources
├──└── calibration_files.txt        # Any additional files, scripts or resources
├──└── camera_serial_numbers.yml    # Any additional files, scripts or resources
├── .gitignore                      # Add description here
├── requirements.txt                # Additional Pipeline-level dependencies required by the Pipeline
└── my.pipeline.py                  # The Marimba Pipeline implementation
```

It's important to note that Pipeline have the extension `.pipeline.py`. Marimba is configured to automatically identify 
and bootstrap files with this extension located within the Marimba Project's pipelines directory. Therefore, all Marimba 
Pipelines must utilize this extension for proper recognition and operation.

To initialize a new Marimba Pipeline, you must first create a new Marimba Project:

```bash
marimba new project my-project
cd my-project
```

There are a couple of methods to create a new Marimba Pipeline:

1. **Create an empty Git repository and integrate it into your Marimba Project:**

   - **Set up a GitHub account:**
     - Visit [GitHub](https://github.com/signup) and sign up for a new account or log in if you already have one
     - Once logged in, click on "New repository" or navigate to [Create a new repository](https://github.com/new)

   - **Create a new repository:**
     - Name your repository (e.g., `my-pipeline`)
     - Choose whether to make the repository public or private
     - Click "Create repository"

   - **Copy the repository URL:**
     - Once your repository is created, GitHub will display the repository page. Copy the URL provided under 
     "Quick setup" — it will look something like `https://github.com/your-user-name/my-pipeline.git`

   - **Clone the new empty repository into your Marimba Project:**
     ```bash
     marimba new pipeline my-pipeline https://github.com/your-user-name/my-pipeline.git
     ```
     This command will clone the Git repository into a new directory in the Marimba project at `pipelines/my-pipeline`.

2. **Manually create the Marimba Pipeline directory and commit it to your Git repository:**
    - Create the pipeline directory:
       ```bash
       mkdir -p pipelines/my-pipeline
       ```
    - Navigate into your new Pipeline directory:
       ```bash
       cd pipelines/my-pipeline
       ```
    - Initialize a new Git repository:
       ```bash
       git init
       ```
    - Link your local repository to GitHub:
       ```bash
       git remote add origin https://github.com/yourusername/my-pipeline.git
       ```
    This step connects your local Pipeline implementation to your GitHub repository, allowing you to use standard Git 
    commands to add, commit, and push files
    - Now, navigate back to the root Marimba Project directory:
       ```bash
       cd ../..
       ```

For both methods, the next steps involve initializing the necessary Pipeline files in your new Pipeline directory.


### Initializing the Pipeline

- Create the Marimba Pipeline file (`my.pipeline.py`):
  ```bash
  touch pipelines/my-pipeline/my.pipeline.py
  ```
- Stage and commit the changes:
  ```bash
  git add .
  git commit -m "Initial commit of Marimba Pipeline"
  ```

Start coding your Pipeline by opening your new `my.pipeline.py` file in a text editor or IDE. Below is a basic 
template to help you get started with your Marimba Pipeline implementation:

```python
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ifdo.models import ImageData

from marimba.core.pipeline import BasePipeline


class MyPipeline(BasePipeline):

    @staticmethod
    def get_pipeline_config_schema() -> dict:
        return {}

    @staticmethod
    def get_collection_config_schema() -> dict:
        return {}

    def _import(
        self,
        data_dir: Path,
        source_path: Path,
        config: Dict[str, Any],
        **kwargs: dict,
    ):

        return

    def _process(
        self,
        data_dir: Path,
        config: Dict[str, Any],
        **kwargs: dict,
    ):

        return

    def _package(
        self,
        data_dir: Path,
        config: Dict[str, Any],
        **kwargs: dict,
    ) -> Dict[Path, Tuple[Path, Optional[List[ImageData]], Optional[Dict[str, Any]]]]:

        data_mapping: Dict[Path, Tuple[Path, Optional[List[ImageData]], Optional[Dict[str, Any]]]] = {}
        return data_mapping
```

This basic code template sets up the structure for your Marimba Pipeline, including methods to handle importing, 
processing, and packaging of data.


### Managing Dependencies

To make sure your environment includes all the necessary Python packages specific to your Pipeline, you can specify 
them in a `requirements.txt` file located within the root directory of your Pipeline repo:

```bash
# Create a requirements.txt file in your Pipeline directory
touch pipelines/my-pipeline/requirements.txt
```

Here's an example of what the contents of your `requirements.txt` might look like:

```plaintext
# pipelines/my-pipeline/requirements.txt
scipy
matplotlib
scikit-image
```

After creating or updating your `requirements.txt`, make sure to track these changes with Git:

```bash
# Stage and commit the requirements.txt file
git add pipelines/my-pipeline/requirements.txt
git commit -m "Adding Marimba Pipeline-specific requirements.txt"
```

Marimba streamlines the installation of these Pipeline-specific packages with the `marimba install` command. This 
command automatically traverses all installed Pipelines and installs all the packages listed in their `requirements.txt`
files, ensuring that each Pipeline has the necessary environment to run successfully.

---

## Implementing Your Pipeline

Implementing your Marimba Pipeline involves writing the necessary code to effectively import, process, and package 
your data. In total, five key methods need to be implemented within your Pipeline. Two of these methods are focused 
on capturing metadata at the Pipeline and Collection levels, while the other three implement the specific data 
processing and reporting procedures for your instrument or system.


### Implementing the `get_pipeline_config_schema` Method

The `get_pipeline_config_schema` method is designed to define the schema for capturing Pipeline-level metadata. 
This metadata typically includes details that are constant across various collections within the same Marimba Project, 
such as project principal investigator or platform ID. Here’s an example of a simple implementation of this method:


#### Example `get_pipeline_config_schema` Implementation

```python
@staticmethod
def get_pipeline_config_schema() -> dict:
    return {
        "project_pi": "Keiko Abe",
        "platform_id": "YM-6100",
    }
```

Here’s another example implementation of the method, demonstrating how to define detailed metadata for a voyage-based
oceanographic research project:

```python
@staticmethod
def get_pipeline_config_schema() -> dict:
    return {
        "voyage_pi": "Keiko Abe",
        "voyage_id": "RSAM202105",
        "start_date": "2021-05-01",
        "end_date": "2021-05-01",
        "data_collector": "Minoru Miki",
        "platform_id": "YM-6100",
    }
```

When a user creates a new Marimba Pipeline, they will be prompted to enter values for each element defined in this 
schema. Each value in the key-value pairs of the dictionary act as a default option provided to the user, who can 
simply press enter to accept the default value or input a new one as necessary. The completion of this process 
results in the creation of a new pipeline metadata file located at `pipelines/my-pipeline/pipeline.yml`, which will 
store all the entered Pipeline metadata. This procedure ensures that all necessary metadata is collected efficiently 
and consistently, customized specifically to the needs of the Pipeline and its data management objectives.

---

### Implementing the `get_collection_config_schema` Method

The `get_collection_config_schema` method is designed to define the schema for capturing Collection-level metadata,
which is specific to individual Collections within a Marimba Project. Here’s a basic example of how this method can 
be structured:


#### Example `get_collection_config_schema()` Implementation

```python
@staticmethod
def get_collection_config_schema() -> dict:
    return {
        "site_id": "TGSM-TOKYO",
        "sample_date": "2021-05-01",
        "data_collector": "Minoru Miki",
    }
```

Here’s another implementation, capturing example metadata for a voyage-based oceanographic research project:

```python
@staticmethod
def get_collection_config_schema() -> dict:
    return {
        "deployment_id": "RSAM202105_001",
        "rov_operator": "Minoru Miki",
        "platform_id": "YM-6100",
    }
```

Similar to the Pipeline-level config schema, users will be prompted to input values for each element defined in this 
schema when setting up a new Marimba Collection. This allows for the capture of Collection-level metadata and results 
in the creation of a new collection metadata file located at `collections/my-collection-name/collection.yml`, which 
will store all the entered Collection metadata.

---

### Implementing the `_import` Method

The `_import` method is responsible for bringing raw data into a Marimba Collection. A common pattern for handling 
imports involves recursively searching through all files in the `source_path` and selectively importing them into 
the Collection based on specified criteria.


#### Example `_import` Implementation

Here is a simple example of the `_import` method implementing this pattern:

```python
from pathlib import Path
from shutil import copy2
from typing import Any, Dict

def _import(
        self,
        data_dir: Path,
        source_path: Path,
        config: Dict[str, Any],
        **kwargs: dict,
    ) -> None:
    
    self.logger.info(f"Importing data from {source_path} to {data_dir}")
    
    for source_file in source_path.rglob("*"):
        if source_file.is_file() and source_file.suffix.lower() in [".csv", ".jpg", ".mp4"]:
            if not self.dry_run:
                copy2(source_file, data_dir)
            self.logger.debug(f"Copied {source_file.resolve().absolute()} -> {data_dir}")
```

In this example, the method begins by logging the start of the import process, noting both the source and destination 
paths. It uses `rglob("*")` from the `pathlib` library to recursively search for files, subsequently verifying if 
each file matches one of the specified extensions (`.csv`, `.jpg`, `.mp4`). This ensures that only targeted files for 
this Pipeline are imported.


#### Dry Run Mode

If the `--dry-run` boolean CLI option is not set to `True` when running the Marimba import command, the files are 
physically copied to the Collection directory (`data_dir`). During a `dry_run`, the method simulates the import process 
by logging the files that would be copied without actually performing the copy operation. This is useful for testing the 
import logic to ensure it behaves correctly without modifying any data.


#### Marimba Logging

We recommend including comprehensive logging statements within your Marimba Pipeline for all significant operations,
utilizing the Marimba logging system (`self.logger.<debug|info|warning|error|critical>`). Marimba is designed to capture
and manage all logs from this system, ensuring that packaged datasets include a complete provenance record of all 
operations performed on the raw data. This documentation not only aids in debugging but also enhances the transparency
of data processing within the Pipeline.


#### Utilizing the `operation` Option

In the Marimba CLI, the import command includes an `operation` flag that allows you to specify how files should be 
transferred into a Marimba collection. This option can be set to [copy|move|link]. The default setting is `copy`, but 
the `move` option is useful for transferring files from a removable storage device like an SD card directly into a 
Marimba collection, effectively clearing the device in the process. Alternatively, the `link` option enables the 
creation of hard-links (on Linux systems), which can avoid the time and disk space overhead associated with copying 
large data volumes, thus speeding up the import process significantly.

If you wish to incorporate these options into your `_import` method, you'll need to implement logic that handles each 
type of operation. Here's how you might modify your `_import` method to include these functionalities:

```python
from pathlib import Path
from shutil import copy2, move
import os
from typing import Any, Dict

def _import(
        self,
        data_dir: Path,
        source_path: Path,
        config: Dict[str, Any],
        **kwargs: dict,
    ) -> None:
    
    operation = kwargs.get('operation', 'copy')
    self.logger.info(f"Starting import from {source_path} to {data_dir} using {operation} operation.")
    
    for source_file in source_path.rglob("*"):
        if source_file.is_file() and source_file.suffix.lower() in [".csv", ".jpg", ".mp4"]:
            target_file = data_dir / source_file.name
            if not self.dry_run:
                if operation == 'copy':
                    copy2(source_file, target_file)
                    self.logger.debug(f"Copied {source_file} to {target_file}")
                elif operation == 'move':
                    move(source_file, target_file)
                    self.logger.debug(f"Moved {source_file} to {target_file}")
                elif operation == 'link':
                    os.link(source_file, target_file)
                    self.logger.debug(f"Linked {source_file} to {target_file}")
```

This revised version of the `_import` method now includes logic to handle `copy`, `move`, and `link` operations based 
on the `operation` key contained in `kwargs`. Each file handling operation is logged for debugging purposes, ensuring 
that you can track which files were processed and by what method, providing a complete trace of the import activity.


#### Executing the `_import` Method

Now that you have successfully implemented the `_import` method, you can import data into a new Marimba Collection 
using the following command:

```bash
marimba import collection-one /path/to/source/directory --operation link
```

With this command, Marimba will execute your `_import` method on the specified source path and create a new Marimba 
Collection at `collections/collection-one`. The method will create hard-links for any files with ".csv", ".jpg", or 
".mp4" extensions found in the source directory.

Congratulations - you have now successfully imported data into a Marimba Collection using an efficient linking 
operation!


#### Metadata Handling in Marimba Pipelines

In Marimba, the capture of both Pipeline-level and Collection-level metadata offers an important role in the scientific 
data processing workflow. 


##### Pipeline-Level Metadata

The `self.config` attribute within any Pipeline method contains metadata elements that are captured during the Pipeline 
installation process, as defined by the `get_pipeline_config_schema()` method. This schema specifies the metadata 
required at the Pipeline level, typically including settings that apply across multiple collections or are essential 
for the overall operation of the Pipeline.

Suppose your `get_pipeline_config_schema()` method defines metadata fields like `voyage_id` and `voyage_pi`. You 
can access these fields within your Pipeline methods as follows:

```python
voyage_id = self.config.get('voyage_id')
voyage_pi = self.config.get('voyage_pi')
self.logger.info(f"Processing data for voyage {voyage_id} under the supervision of {voyage_pi}.")
```

This allows your Pipeline to use these configurations dynamically during data processing, enabling for flexible and 
informed handling of data based on the specific metadata.


##### Collection-Level Metadata

The `config` dictionary passed into methods like `_import` contains the Collection-level metadata, which is captured as 
defined in the `get_collection_config_schema()` method. This metadata typically relates to specifics of the data 
collection, such as the collection date, site information, or specific parameters used during data collection.

Let’s assume your `get_collection_config_schema()` method captures `collection_date` and `site_id` as metadata for 
each collection. Here’s how you might access these within your Pipeline methods:

```python
collection_date = config.get('collection_date')
site_id = config.get('site_id')
self.logger.info(f"Importing data collected on {collection_date} from site {site_id}.")
```

This approach ensures that each collection can be processed with its unique context, enhancing the granularity and 
relevance of data processing activities.


##### Metadata Handling Summary

By distinguishing between Pipeline-level and Collection-level metadata, Marimba facilitates diverse data management 
strategies within a unified framework. Accessing these metadata elements within your Pipeline methods ensures that you 
can customize the data handling procedures to meet both general and specific requirements effectively. This ability to 
access both Pipeline-level and Collection-level metadata becomes increasingly important later in the `_package` method, 
where the requirement is to report all the data and metadata to core Marimba for final processing.


#### Defining and Using Custom `kwargs`

Marimba enables users to define their own custom `kwargs` (keyword arguments) which can be passed to any command in the 
Marimba CLI using the `--extra` option. This flexibility allows users to introduce additional parameters or settings 
that are not predefined in the Marimba system, but are essential for specific pipeline operations or conditions.

These custom `kwargs` are especially useful in environments where the Pipeline might require dynamic configuration 
changes that are dependent on external factors or where pipeline execution needs to be fine-tuned without altering 
the underlying codebase.


##### How to Pass Custom `kwargs` Through the CLI

When executing a command in the Marimba CLI, users can append the `--extra` flag followed by a key-value pair. These 
are then accessible within any Marimba Pipeline method, allowing the method to adjust its behavior based on the 
custom inputs.

**Example CLI Usage:**

```bash
marimba import collection-one /path/to/source --extra file_types=.txt,.pdf
```

In this example, the `file_types` are custom arguments passed during the import process and specifies additional file 
types to be included during the import.


##### Accessing `kwargs` in a Marimba Pipeline

Here's an example of how you might modify the `_import` method to utilize these custom `kwargs`, adjusting the behavior 
of the import process based on the passed parameters:

**Example Pipeline Method Implementation:**

```python
from pathlib import Path
from shutil import copy2
from typing import Any, Dict

def _import(
        self,
        data_dir: Path,
        source_path: Path,
        config: Dict[str, Any],
        **kwargs: dict,
    ) -> None:
    
    self.logger.info(f"Importing data from {source_path} to {data_dir}")
    
    # Merge default file types with those provided in kwargs
    default_types = [".csv", ".jpg", ".mp4"]
    extra_types = kwargs.get('file_types', '').split(',')
    file_types = default_types + [ftype for ftype in extra_types if ftype not in default_types]
    
    for source_file in source_path.rglob("*"):
        if source_file.is_file() and source_file.suffix.lower() in file_types:
            if not self.dry_run:
                copy2(source_file, data_dir)
            self.logger.debug(f"Copied {source_file.resolve().absolute()} -> {data_dir}")
```

In this updated `_import` method, the `file_types` are extracted from `kwargs` and merged in with the default file 
types. The method then checks each file to determine if its suffix matches with the types listed in `file_types`. This 
example shows how custom `kwargs` can be used to enhance the flexibility of Pipeline operations, providing customizable, 
dynamic, and context-aware data processing within Marimba.

---

### Implementing the `_process` Method

The `_process` method is designed to handle any data conversion, manipulation, and processing steps following the 
initial import. A typical process in this method might involve setting up a hierarchical directory structure, sorting 
files into specified subdirectories, applying validation or calibration techniques, and executing file-specific tasks 
such as image format conversion, video transcoding, thumbnail creation, and sensor data integration. It could also 
involve compiling data visualizations or generating derived data products. Following the completion of the `_process` 
method, the data should be prepared and ready for the final packaging stage.


#### Example `_process` Implementation

```python
from pathlib import Path
from typing import Any, Dict

from marimba.lib import image

def process(data_dir: Path, config: Dict[str, Any], **kwargs: dict):
    
    self.logger.info(f"Processing data in {data_dir}")

    # Create directories for different file types
    csv_dir = data_dir / "data"
    jpg_dir = data_dir / "images"
    thumbs_dir = data_dir / "thumbnails"
    mp4_dir = data_dir / "videos"
    
    csv_dir.mkdir(exist_ok=True)
    jpg_dir.mkdir(exist_ok=True)
    thumbs_dir.mkdir(exist_ok=True)
    mp4_dir.mkdir(exist_ok=True)

    # Move files into their respective directories
    for file_path in data_dir.rglob("*"):
        if file_path.is_file():
            if file_path.suffix.lower() == ".csv":
                file_path.rename(csv_dir / file_path.name)
            elif file_path.suffix.lower() == ".jpg":
                file_path.rename(jpg_dir / file_path.name)
            elif file_path.suffix.lower() == ".mp4":
                file_path.rename(mp4_dir / file_path.name)

    # Generate thumbnails for each jpg
    thumbnails = []
    for jpg_file in jpg_dir.glob("*.jpg"):
        thumbnail_path = thumbs_dir / f"{jpg_file.stem}_thumbnail{jpg_file.suffix}"
        image.resize_fit(jpg_file, 300, 300, thumbnail_path)
        thumbnails.append(thumbnail_path)

    # Create an tiled overview image from the thumbnails
    overview_path = data_dir / "overview.jpg"
    image.create_grid_image(thumbnails, overview_path)
```

This example organizes and processes various file types previously imported into a Marimba Collection. It begins by 
creating subdirectories for CSV files (`data`), JPG images (`images`), MP4 videos (`videos`), and thumbnails 
(`thumbnails`). It then recursively scans the Collection, moving CSV, JPG, and MP4 files into their respective 
subdirectories. For each JPG file, the method generates a low-resolution thumbnail, saving it in the `thumbnails` 
directory. Finally, it compiles all the generated thumbnails into a single tiled overview image stored in the root 
directory of the Collection using the `create_grid_image` from the Marimba standard library.


#### Executing the `_process` Method

To execute the `_process` method within your Marimba Pipeline, use the Marimba CLI to initiate the process command for 
all Collections across all Pipelines within a Project:

```bash
marimba process
```

Marimba will automatically identify each Pipeline and Collection within the Project and initiate the `_process` method
for each one. Due to Marimba's parallelized core architecture, Marimba will process each Pipeline and Collection 
combination independently and concurrently. This approach maximizes the utilization of computing resources and 
significantly accelerates data processing. The image below illustrates how Marimba implements parallel processing, 
showing the interactions between Pipelines and Collections:

![Marimba Workflow](img/marimba-workflow.png)


##### Targeted Processing with Specific Pipelines and Collections

For more targeted processing, Marimba allows you to specify particular Pipelines or Collections. This feature is 
particularly useful when you need to process only a subset of data or test changes in a specific pipeline without 
affecting the entire dataset. Using the `--collection-name` and `--pipeline-name` CLI options, you can direct 
Marimba to process only the specified subsets of Pipelines or Collections. For instance, if you wanted to process data 
only from a specific Collection using a particular Pipeline, you could use the command:

```bash
marimba process --collection-name collection-one --pipeline-name my-pipeline
```

This command directs Marimba to process data exclusively from `collection-one` using the logic defined in `my-pipeline`. 
Marimba also supports targeting multiple Pipelines or Collections by allowing the `--collection-name` and 
`--pipeline-name` CLI options to be specified multiple times:

```bash
marimba process --collection-name collection-one --collection-name collection-two --pipeline-name my-pipeline --pipeline-name my-other-pipeline
```

This ability to target specific pipelines and collections allows that Marimba can handle diverse processing requirements 
efficiently, whether for isolated testing or comprehensive data processing across various Pipelines or Collections.


#### Multithreaded Thumbnail Generation

Marimba offers a multithreaded approach for generating thumbnails, which efficiently utilizes the available compute 
resources. This method leverages parallel processing to accelerate the creation of thumbnails, enhancing performance 
especially on systems with multiple cores.

**Example of Multithreaded Thumbnail Generation:**

```python
from pathlib import Path
from typing import Any, Dict

from marimba.lib import image
from marimba.lib.parallel import multithreaded_generate_thumbnails

def process(data_dir: Path, config: Dict[str, Any], **kwargs: dict):
    
    self.logger.info(f"Processing data in {data_dir}")

    # Create directories for different file types
    jpg_dir = data_dir / "images"
    thumbs_dir = data_dir / "thumbnails"
    
    jpg_dir.mkdir(exist_ok=True)
    thumbs_dir.mkdir(exist_ok=True)

    # Move files into their respective directories
    image_list = []
    for file_path in data_dir.rglob("*"):
        if file_path.is_file():
            if file_path.suffix.lower() == ".jpg":
                file_path.rename(jpg_dir / file_path.name)
                image_list.append(jpg_dir / file_path.name)

    # Generate thumbnails using multithreading
    thumbnails = multithreaded_generate_thumbnails(
        self,
        image_list=image_list,
        output_directory=data_dir / "thumbnails",
    )

    # Create an overview image from the thumbnails
    thumbnail_overview_path = data_dir / "OVERVIEW.JPG"
    image.create_grid_image(thumbnails, thumbnail_overview_path)
```

This example demonstrates how to use the multithreading capabilities provided by the Marimba standard library to 
streamline thumbnail generation within a data processing workflow.


### Implementing the `_package` Method

The `_package` method prepares the processed data for packaging and returns a mapping of files and metadata.


#### Example `_package` Implementation

```python
def _package(self, data_dir, config, **kwargs):
    from ifdo.models import ImageData

    processed_data_dir = data_dir / 'processed_data'
    data_mapping = {}

    for image_file in processed_data_dir.glob('*.jpg'):
        metadata = ImageData(
            filename=image_file.name,
            # Add more metadata fields as needed
        )
        data_mapping[image_file.name] = (image_file, [metadata], None)

    return data_mapping
```


#### Executing the `_package` Method

To be written...

---

## Advanced Topics

### Multi-Level iFDO Files

To be written...


### Multi-Level Summary Files

To be written...

---

## Conclusion and Next Steps

By following this guide, you've learned how to:

- Understand the role and capabilities of Marimba Pipelines.
- Set up the necessary directory structure and files.
- Implement the essential methods required by the `BasePipeline` class.
- Handle metadata using the iFDO standard.
- Expand your Pipeline with advanced features and integrations.

With this foundation, you're now equipped to develop custom Pipelines that meet your specific data processing needs. 
Explore the Marimba documentation for more advanced features, and consider contributing to the project or sharing your 
Pipelines with the community.

Happy Marimba data processing!
