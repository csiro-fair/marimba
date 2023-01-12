<a name="readme-top"></a>

<br>

<!-- PROJECT LOGO -->
<div style="text-align: center">

![](img/logo.png "marimba-logo")
<h1 style="color: #00A9CE">MarImBA</h1>
<h3><span style="color: #00A9CE">Mar</span>ine <span style="color: #00A9CE">Im</span>agery <span style="color: #00A9CE">B</span>atch <span style="color: #00A9CE">A</span>ctions</h3>
<p><i>A Python CLI for batch processing, transforming and FAIR-ising large volumes of marine imagery.</i></p>
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


This repository contains the MarImBA Python CLI (Command Line Interface) which is a scientific marine image processing library initially develop at [CSIRO](https://www.csiro.au/). MarImBA is based on the [Typer](https://typer.tiangolo.com/) and [Rich](https://pypi.org/project/rich/) Python packages and contains a range of capabilities including:

* File renaming and directory structuring using instrument-specific naming conventions
* Integration with the [iFDO](https://marine-imaging.com/fair/ifdos/iFDO-overview/) (image FAIR Digital Object) standards
* Image conversion, compression and resizing using Python [Pillow](https://pypi.org/project/Pillow/)
* Video transcoding, chunking and frame extraction using [Ffmpeg](https://ffmpeg.org/)
* Automated logfile capturing to archive the image processing provenance

MarImBA can be used directly after data acquisition has occurred and can efficiently produce well-described image datasets that are aligned with the FAIR data standards.

---

## Contents

- [Getting started](#getting-started)
  - [Project structure](#project-structure)
  - [Set up Python virtual environment](#python-virtual-environment)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)
- [Acknowledgments](#acknowledgments)

---

<a name="getting-started"></a>
## Getting started

Start by cloning the MarImBA repository:

```bash
git clone https://bitbucket.csiro.au/scm/biaa/marimba.git
```

In the future we are anticipating making MarImBA an official [PyPI](https://pypi.org/) package, which will be installable with `pip install marimba` and usable anywhere on your system.

<a name="project-structure"></a>
### Project structure

We have structured the code in this project guided loosely by the following articles:

* [The optimal python project structure](https://awaywithideas.com/the-optimal-python-project-structure/)
* [Structuring Your Project — The Hitchhiker's Guide to Python](https://docs.python-guide.org/writing/structure/)

The specific structure for this project is:

```
marimba
└───doc                         - Documentation files for MarImBA
└───img                         - Images for this README.md file
└───src                         - Source directory containing the MarImBA Python CLI application code
│   │
│   └───naming                  - File naming schemes for different instruments
│   └───utils                   - Utility modules
│   │   │   file_system.py      - File system tools
│   │   │   logger_config.py    - Logging configuration settings
│   │
│   │   marimba.py              - Main Python application
│   │   config.py               - General application-wide configuration settings
│
└───tests                       - Unit tests for the application
│
│   .flake8                     - Custom flake8 settings
│   .pre-commit-config.yaml     - Pre-commit hooks that can be executed locally and in CI
│   Dockerfile                  - Main Dockerfile to build MarImBA in a Docker container
│   pyproject.toml              - Custom Python Black settings
│   README.md                   - This file!
│   requirements.txt            - Pip dependencies for Python virtual environment
```


<a name="python-virtual-environment"></a>
### Set up Python virtual environment

Let's install a Python virtual environment into the marimba/env directory.

```bash
cd marimba
python3 -m venv env
source env/bin/activate
```

Upgrade pip and install Python packages from requirements.txt:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<a name="usage"></a>
## Usage

MarImBA is based on the [Typer](https://typer.tiangolo.com/) Python package which provides good documentation by default. Try running MarImBA to see the default help menu:

```bash
python src/marimba.py
```

![](img/marimba_default-help.png "marimba_default-help")

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<a name="contributing"></a>
## Contributing

This is an open-source project and we welcome contibutions. If you have a suggestion that would make this better, please clone the repo and submit a pull request.

1. Clone the project: <a href="#getting-started">Getting Started</a>
2. Create your new feature branch: 
    ```bash
    git checkout -b feature/amazing-feature
    ```
4. Commit your changes: 
    ```bash
    git commit -m 'Added some amazing feature'
    ```
5. Push to the branch: 
    ```bash
    git push origin feature/amazing-feature
    ```
6. Open a pull request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<a name="license"></a>
## License

This project is licensed under [MIT](https://opensource.org/licenses/MIT) license.

(TODO: This needs to be reviewed according to CSIRO current licensing recommendations. There was recently an interesting thread on the MS Teams linux channel [here](https://teams.microsoft.com/l/message/19:f76b576ac1df4742a7a8cb5c2a86439d@thread.skype/1673393871094?tenantId=0fe05593-19ac-4f98-adbf-0375fce7f160&groupId=20e7492d-eca3-4f55-bbc6-e87f2ad12df2&parentMessageId=1673393871094&teamName=CSIRO&channelName=linux&createdTime=1673393871094&allowXTenantAccess=false))

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<a name="contact"></a>
## Contact

The primary point-of-contact for this repository is: 
* Chris Jackett - [chris.jackett@csiro.au](chris.jackett@csiro.au)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<a name="acknowledgments"></a>
## Acknowledgments

The inception of MarImBA was at CSIRO in 2021 and much of the initial design and implementation took place at the CSIRO 2022 [Image Data Collection and Delivery Hackathon](/docs/hackathon.md).

There have been many contributors to this project including:

* Chris Jackett - (CSIRO Environment)
* Ben Scoulding - (CSIRO Environment)
* Franzis Althaus - (CSIRO Environment)
* Nick Mortimer - (CSIRO Environment)
* Aaron Tyndall - (CSIRO NCMI)
* David Webb - (CSIRO NCMI)
* Karl Forcey - (CSIRO NCMI)
* Brett Muir - (CSIRO NCMI)
* Bec Gorton - (CSIRO Environment)
* ...

<p align="right">(<a href="#readme-top">back to top</a>)</p>

