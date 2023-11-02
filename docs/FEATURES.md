
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
