<a name="readme-top"></a>

<!-- PROJECT LOGO -->
<figure markdown style="text-align: center">

![](img/logo.png "Marimba logo")
![](docs/img/logo.png "Marimba logo")

</figure>

<div style="text-align: center">

<h1 style="color: #00A9CE">Marimba</h1>
<h3><span style="color: #00A9CE">Mar</span>ine <span style="color: #00A9CE">Im</span>agery <span style="color: #00A9CE">B</span>atch <span style="color: #00A9CE">A</span>ctions</h3>
<p><i>A Python framework for structuring, managing, processing and FAIR-ising scientific marine image datasets.</i></p>
<div>
  <a href="https://github.com/elangosundar/awesome-README-templates/stargazers"><img src="https://img.shields.io/github/stars/elangosundar/awesome-README-templates" alt="Stars Badge"/></a>
<a href="https://github.com/elangosundar/awesome-README-templates/network/members"><img src="https://img.shields.io/github/forks/elangosundar/awesome-README-templates" alt="Forks Badge"/></a>
<a href="https://github.com/elangosundar/awesome-README-templates/pulls"><img src="https://img.shields.io/github/issues-pr/elangosundar/awesome-README-templates" alt="Pull Requests Badge"/></a>
<a href="https://github.com/elangosundar/awesome-README-templates/issues"><img src="https://img.shields.io/github/issues/elangosundar/awesome-README-templates" alt="Issues Badge"/></a>
<a href="https://github.com/elangosundar/awesome-README-templates/graphs/contributors"><img alt="GitHub contributors" src="https://img.shields.io/github/contributors/elangosundar/awesome-README-templates?color=2b9348"></a>
<a href="https://github.com/elangosundar/awesome-README-templates/blob/master/LICENSE"><img src="https://img.shields.io/github/license/elangosundar/awesome-README-templates?color=2b9348" alt="License Badge"/></a>
</div>
(all badges show example values, but will show real values when this project is open-sourced)
<br>
</div>

The Marimba Python framework is a specialised library designed for scientific marine image processing. Developed collaboratively by [CSIRO](https://www.csiro.au/) and [MBARI](https://www.mbari.org/), Marimba includes a diverse set of functionality aimed to facilitate the structuring, processing and FAIR-ising of marine imaging data. The framework is integrated with a [Typer](https://typer.tiangolo.com/) CLI (Command Line Interface), and makes use of the [Rich](https://pypi.org/project/rich/) Python package for enhanced the CLI user experience. Marimba also contains a well-structured API (Application Programming Interface) which enables programmatic interaction with Marimba, allowing for automated scripting from external scripts or Graphical User Interfaces (GUIs).

The Marimba framework offers a comprehensive suite of advanced features that are designed for various aspects of scientific marine imaging:

#### Data Project Structuring and Management

- **Image Dataset Structuring**: Marimba facilitates a systematic approach to structuring and managing scientific image data projects throughout the entire image processing workflow.

- **Batch Processing**: The features within the Marimba framework are engineered to facilitate batch processing of extensive image datasets. Users have the flexibility to tailor the data processing scope, targeting it at the deployment, instrument, or project level.
  
#### File and Metadata Management

- **Automated File Renaming**: Marimba allows for user-defined and instrument-specific naming conventions to automatically rename files, maintaining dataset consistency and enhancing data discoverability.

- **Advanced Image Metadata Management**: Marimba offers extensive capabilities for managing image metadata including:
  - Integration of image datasets with corresponding navigation and sensor data.
  - Writing metadata directly into image EXIF tags for greater accessibility.
  - Compliance with [iFDO](https://marine-imaging.com/fair/ifdos/iFDO-overview/) (image FAIR Digital Object) standards to ensure interoperability and reusability of data.
  
#### Modular Image and Video Processing

- **Automatic Thumbnail Generation**: Marimba can automatically generate thumbnails for both images and videos, including the creation of composite overview thumbnail images for rapid assessment of the contents of image datasets.
  
- **Image Manipulation**: Marimba utilises the Python [Pillow](https://pypi.org/project/Pillow/) library for image conversion, compression, and resizing.

- **Video Processing**: Marimba integrates features from [Ffmpeg](https://ffmpeg.org/) for tasks such as video transcoding, chunking, and frame extraction.
  
- **Quality Control**: Marimba implements quality control measures using [CleanVision](https://github.com/cleanlab/cleanvision) to detect issues like duplicate, blurry, or improperly exposed images.

#### FAIR Data Standards Compliance

- **FAIR Image Dataset Packaging**: Marimba aligns with FAIR (Findable, Accessible, Interoperable, and Reusable) data principles and offers functionalities including:
  - Generation of file manifests for dataset validation.
  - Automated enumeration and summarisation of image dataset statistics.
  
#### Data Distribution

- **FAIR Image Dataset Distribution**: Provides methods for distributing image datasets compliant with FAIR principles, such as:
  - Storage to S3 buckets.
  - FathomNet for annotated ML datasets (to be implemented)

#### Provenance and Transparency

- **Automated Processing Logs**: Marimba captures logs to archive all processing operations, ensuring data transparency and provenance.

---

## Contents

- [Getting started](#getting-started)
  - [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)
- [Acknowledgments](#acknowledgments)

---

<a name="getting-started"></a>
## Getting started

<a name="installation"></a>
### Installation

The Marimba framework can be installed using Python's package manager, pip. Ensure you have a compatible version of Python installed (3.10 or greater) on your system before proceeding.

To install Marimba, open your terminal or command prompt and execute the following command:

```bash
pip install marimba
```

This will download and install the latest version of Marimba along with any required dependencies. After successful installation, you can confirm that Marimba has been correctly installed by running:

```bash
marimba --version
```

Marimba also has a few system level dependencies such as `ffmpeg` and `ffprobe` (which is installed with `ffmpeg`) (it would be very nice to move away from this, especially if we can find a cross-platform pip ffmpeg library!). On Ubuntu you can install `ffmpeg` with:

```bash
sudo apt install ffmpeg
```

To set up a Marimba development environment, additional instructions and guidelines can be found in the documentation located in the [ENVIRONMENT.md](docs/ENVIRONMENT.md). Please refer to the relevant section for detailed information on how to properly configure your development setup.


---

<a name="usage"></a>
## Usage

Marimba is based on the [Typer](https://typer.tiangolo.com/) Python package which is self-documenting by default. Try running Marimba to see the default help menu:

```bash
marimba
```

![](docs/img/marimba_default-help.png "marimba_default-help")

The default entry point to start using Marimba is the `new` command. This allows you to create a new Marimba collection, pipeline or collection that adheres to the following standard Marimba structure:

```
{collection}
│
└───distribution                    - 
│
└───pipelines                     - 
│   │
│   └───{pipeline}                - 
│       │
│       └───lib                     - 
│       │   │   pipeline.py       - 
│       │   │   requirement.txt     - 
│       │
│       └───work                    - 
│       │   │
│       │   └───{collection}        - 
│       │
│       │   {pipeline}.log        - 
│       │   pipeline.yml          - 
│       │   metadata.yml            - 
│
└───collection.yml                  - 
└───{collection}.log                - 
```

The usual order you might use the Marimba commands might be:
* `marimba new {collection}`
* `marimba new {pipeline}`
* `marimba new {collection}`
* `marimba qc` - it applicable
* `marimba rename`
* `marimba metadata`
* `marimba convert`
* `marimba distribute`
* ...


<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<a name="contributing"></a>
## Contributing

Marimba is an open-source project and we welcome contributions. If you have a suggestion that would make Marimba better, please clone the repo and submit a pull request by following the [CONTRIBUTING.md](docs/CONTRIBUTING.md) documentation.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<a name="license"></a>
## License

This project is licensed under [MIT](https://opensource.org/licenses/MIT) license. Please refer to the [LICENSE.md](LICENSE.md) file for information regarding the licensing agreement for Marimba.

TODO: This needs to be reviewed according to CSIRO current licensing recommendations.
There was recently an interesting thread on the MS Teams linux channel [here](https://teams.microsoft.com/l/message/19:f76b576ac1df4742a7a8cb5c2a86439d@thread.skype/1673393871094?tenantId=0fe05593-19ac-4f98-adbf-0375fce7f160&groupId=20e7492d-eca3-4f55-bbc6-e87f2ad12df2&parentMessageId=1673393871094&teamName=CSIRO&channelName=linux&createdTime=1673393871094&allowXTenantAccess=false)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<a name="contact"></a>
## Contact

The primary points-of-contact for this repository are: 
* Chris Jackett - CSIRO
* Kevin Barnard - MBARI

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<a name="acknowledgments"></a>
## Acknowledgments

This project has been developed in collaboration between CSIRO and MBARI, two leading institutions in marine science and technology. The conceptual foundation of Marimba was formulated at CSIRO in late 2022. Substantial elements of its initial design and implementation were created during the CSIRO Image Data Collection and Delivery Hackathon in Feb/March 2023, along with further collaborative development between CSIRO and MBARI in Oct/Nov 2023.

The development of this project has benefited from the contributions of many people including:

* Chris Jackett - CSIRO Environment
* Kevin Barnard - MBARI
* Nick Mortimer - CSIRO Environment
* David Webb - CSIRO NCMI
* Aaron Tyndall - CSIRO NCMI
* Franzis Althaus - CSIRO Environment
* Bec Gorton - CSIRO Environment
* Ben Scoulding - CSIRO Environment

<p align="right">(<a href="#readme-top">back to top</a>)</p>

