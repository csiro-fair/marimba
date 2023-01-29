# Image Data Collection and Delivery Hackathon
### Monday Feb 27 to Friday Mar 3, 2023

---

Through the [FAIR for Imagery project](https://research.csiro.au/fair/) we identified the need to streamline and where possible standardise workflows for the collection of marine imagery. We are organising a Hackathon week to develop and deliver python coding solutions that can be shared across CSIRO and national and international partners for:

* **Streamlined and consistent imagery** and ancillary data collection,
* **Georeferencing**
* **File naming and structure, and description**
* File transfer
* Storage solutions
* **Image processing and QC tools**

The Hackathon will be held at an off-site venue (Airbnb or similar) close to Hobart from February 27 to March 3 2023. Travel and accommodation will be paid for by the project; some FTE allocation can be made available if needed.

We propose to deliver the python coding solutions in form of a Python CLI package ‘MarImBA’. The following notes outline potential discussions and tasks for one component of the Hackathon; the development of the MarImBA Python CLI package that will help automate and standardise marine image dataset management.

Some open questions to be discussed before the Hackathon:

* How should we develop code together?
  * What existing code solutions do we already have that align with the themes the hackathon and could be incorporated into MarImBA?
  * Code contributions could be managed using git (CSIRO Bitbucket), working on feature branches and submitting pull requests to be reviewed and merged (see README.md for details).
  * Should we use the following Python packages to help us work consistently together?:
    * Black for standardised code formatting
    * Flake8 for style guide enforcement
    * Mypy for static type checking
    * Bandit for security testing
    * Pylint for code analysis
    * Pytest for unit testing
  * Can we all bring along example raw imagery for testing purposes?
  * Any other questions/suggestions?

Some initial design thoughts have already occurred for MarImBA and below is a non-exhaustive list of potential tasks that could be worked on during the Hackathon:

Task | Description | Priority | Assigned | Complete
---|---|:---:|---|:---:
Recursive processing | Figure out a clever way to pass the `-recursive` argument to each command and execute the selected code on a single directory or recursive directory structure (perhaps using lambda functions). | High | Chris Jackett | 
Redesign API | Rework MarImBA API so that it doesn't begin with the selection of `image` or `video`. Top level command should be `rename`, `metadata`, `convert`... | High | Chris Jackett | ✅
File renaming | Based on the previous [Standard Data Structure and File Formats for Towed Camera Platforms](https://confluence.csiro.au/display/TCDM/File+Format+Document) work, design and implement an abstract base class that outlines what each instrument specification need to follow to be able to rename files automatically with MarImBA. | High | Chris Jackett | 
iFDO compatibility | Investigate and implement [iFDOs](https://marine-imaging.com/fair/ifdos/iFDO-overview/). An initial survey-level iFDO might need to be hand-crafted that contains information such as the instrument used (that matches a pre-made instrument renaming specification), which then gets passed to the `rename` command, and also the `metadata` command. This is because `rename` needs to know the naming convention and folder structure (detailed in the instrument renaming specification), and the `metadata` command also needs to know the naming structure to write data files to the appropriate locations. Need to determine if we write iFDO files into each deployment directory and/or the survey folder. | High |  | 
Metadata | Design and implement the `metadata` command that takes a survey-level iFDO and deployment-level data files (e.g. navigation data), merges and outputs nav data files, writes metadata into image EXIF fields, and writes iFDO files into the directory structure. Given that we may encounter different deployment / nav data files/structures, it would be great if the design of this followed the abstract base class design from the `rename` command, so that we can implement different nav data specification classes, allowing us to merge nav data from different sources. | High | Nick Mortimer? | 
Add new instruments | Implement renaming schemes for different instruments by inheriting from the renaming abstract base class <ul><li>Chris Jackett -> NCMI Zeiss Axio Observer <input type="checkbox" checked /><il><li>Nick Mortimer / David Webb -> Environment BU deep towed camera system?<il><li>Aaron Tyndall -> MNF deep towed camera system?<il><li>Brett Muir -> SMARTCAM?<il><li>BRUVS?<il></ul> | High | Nick Mortimer<br>David Webb<br>Aaron Tyndall | 
Logging | Configure logging StreamHandler to write logfiles to disk and archive the image processing provenance. Need to determine if we write processing logfiles into each deployment directory and/or the survey folder. The ability to append to logfiles would be very nice. | High |  | 
Documentation | Work on documentation in README.md and inline code comments aligned with [PEP 257 – Docstring Conventions](https://peps.python.org/pep-0257). | High | Chris Jackett<br>Nick Mortimer<br>David Webb<br>Aaron Tyndall | 
Ffmpeg settings | Investigate the optimal `ffmpeg` arguments for standardise transcoded videos. Look at `marimba.py` `transcode_files()` method comments for current suggestions. <ul><li>Should we standardise video format on web-streamable MP4 files encoded with the h264 codec?<il></ul> | Medium |  | 
License file | Investigate and include an official CSIRO license file. This needs to be reviewed according to CSIRO current licensing recommendations. There was recently an interesting thread on the MS Teams linux channel [here](https://teams.microsoft.com/l/message/19:f76b576ac1df4742a7a8cb5c2a86439d@thread.skype/1673393871094?tenantId=0fe05593-19ac-4f98-adbf-0375fce7f160&groupId=20e7492d-eca3-4f55-bbc6-e87f2ad12df2&parentMessageId=1673393871094&teamName=CSIRO&channelName=linux&createdTime=1673393871094&allowXTenantAccess=false). | Medium | | 
Secret storage | Determine how we want to implement secret storage. Should we just store credentials in a .env file? Or should we implement secret storage using Hashicorp Vault (current IM&T approved method)? Or should we do both? | Medium |  | 
Rich status | For potentially long operations, figure out how to use the rich console status (a very snazzy console spinner) while also maintaining writing logfile to disk. | Low |  | 
Thumbnails | Design and implement a method to generate thumbnails and manage them neatly (i.e. not just intermingled in a deployment folder). Generate large composite image for each deployment. | Low |  | 
MarImBA CI | Set up MarImBA on GitLab Continuous Integration (CI) so that commits are automatically built, tested and packaged and pushed. | Low | Chris Jackett | 
Testing | Implement unit tests for MarImBA. | Low |  | 
PyPI package | Implement MarImBA as a PyPI package. | Low  | Chris Jackett | 
|  |  |  |  | 

* Perhaps a search/list function for marine imagery
* Config file for overriding ffmpeg setting, located in ifdo or instrument spec
* QC - identifying/removing black images
