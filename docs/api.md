# Marimba API Reference

This reference documents the Marimba Python API for developers who want to build pipelines, drive Marimba
programmatically, or integrate it into larger applications. It covers the pipeline base class you subclass, the metadata
classes you emit, the standard library of processing helpers, the wrapper classes that manage projects and datasets, and
the exception and logging conventions. For a narrative tutorial on building a pipeline, read the
[Pipeline Implementation Guide](pipeline.md) alongside this reference; for the command-line surface, see the
[Overview and CLI Usage Guide](overview.md).

All signatures below reflect Marimba v1.2.0. The authoritative source is always the code itself; where a method has many
parameters, only those relevant to typical use are highlighted in prose.

---

## Table of Contents

1. [API Overview](#api-overview)
2. [Building a Pipeline: `BasePipeline`](#building-a-pipeline-basepipeline)
   - [Constructor and Metadata Class](#constructor-and-metadata-class)
   - [Configuration Schemas](#configuration-schemas)
   - [Lifecycle Methods](#lifecycle-methods)
   - [The `_package` Return Mapping](#the-_package-return-mapping)
   - [Runtime Helpers](#runtime-helpers)
3. [Metadata Classes](#metadata-classes)
   - [`BaseMetadata`](#basemetadata)
   - [`iFDOMetadata`](#ifdometadata)
   - [`DarwinCoreMetadata` and `GenericMetadata`](#darwincoremetadata-and-genericmetadata)
4. [Standard Library](#standard-library)
   - [`marimba.lib.image`](#marimbalibimage)
   - [`marimba.lib.gps`](#marimbalibgps)
   - [`marimba.lib.exif`](#marimbalibexif)
5. [A Complete Worked Example](#a-complete-worked-example)
6. [Driving Marimba Programmatically](#driving-marimba-programmatically)
   - [`ProjectWrapper`](#projectwrapper)
   - [`PipelineWrapper`](#pipelinewrapper)
   - [`CollectionWrapper`](#collectionwrapper)
   - [`DatasetWrapper`](#datasetwrapper)
   - [`DistributionTargetWrapper`](#distributiontargetwrapper)
7. [Exceptions](#exceptions)
8. [Logging](#logging)

---

## API Overview

Marimba's public API has two layers:

- **The pipeline API**: the `BasePipeline` class and the metadata classes. This is what you implement to add support
  for a new instrument, and it is what most developers work with. A pipeline is a self-contained Git repository
  containing a single `BasePipeline` subclass.
- **The wrapper API**: `ProjectWrapper`, `PipelineWrapper`, `CollectionWrapper`, `DatasetWrapper` and
  `DistributionTargetWrapper`. These manage the on-disk project structure and implement the operations the CLI exposes.
  Use them when you want to drive Marimba from Python rather than the command line.

The common imports:

```python
from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.ifdo import iFDOMetadata
from marimba.core.wrappers.project import ProjectWrapper
from marimba.core.utils.constants import Operation
from marimba.core.utils.log import get_logger
```

---

## Building a Pipeline: `BasePipeline`

`BasePipeline` (in `marimba.core.pipeline`) is the abstract base class that every pipeline subclasses. It defines the
import, process and package lifecycle, wires up per-pipeline logging and dry-run handling, and gives your code access to
its static configuration.

### Constructor and Metadata Class

```python
class BasePipeline(ABC, LogMixin):
    def __init__(
        self,
        root_path: str | Path,
        config: dict[str, Any] | None = None,
        metadata_class: type[BaseMetadata] = BaseMetadata,
        *,
        dry_run: bool = False,
    ) -> None: ...
```

Subclasses override `__init__` to fix the `metadata_class` their pipeline emits, almost always `iFDOMetadata`, and
forward the remaining arguments to `super().__init__`:

```python
from pathlib import Path
from typing import Any

from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.ifdo import iFDOMetadata


class MyPipeline(BasePipeline):
    def __init__(
        self,
        root_path: str | Path,
        config: dict[str, Any] | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        super().__init__(root_path, config, dry_run=dry_run, metadata_class=iFDOMetadata)
```

The chosen class is available at runtime as `self._metadata_class`, so `_package` can construct metadata without
hard-coding the type.

### Configuration Schemas

Two static methods declare the configuration Marimba prompts for. Each returns a dict mapping field names to default
values; return an empty dict if a level needs no configuration.

```python
@staticmethod
def get_pipeline_config_schema() -> dict[str, Any]: ...

@staticmethod
def get_collection_config_schema() -> dict[str, Any]: ...
```

- `get_pipeline_config_schema()` - values constant across all collections (for example a platform identifier). Prompted
  at `marimba new pipeline` and stored in `pipeline.yml`.
- `get_collection_config_schema()` - values specific to a single import (for example a deployment identifier). Prompted
  at `marimba import` and stored in `collection.yml`.

### Lifecycle Methods

You override three lifecycle methods. Only `_package` is abstract and must be implemented; `_import` and `_process` have
no-op default implementations you override as needed. Marimba calls the public `run_import` / `run_process` /
`run_package` wrappers, which apply dry-run and logging handling and then invoke your `_`-prefixed methods.

```python
def _import(self, data_dir: Path, source_path: Path, config: dict[str, Any], **kwargs: Any) -> None: ...

def _process(self, data_dir: Path, config: dict[str, Any], **kwargs: Any) -> None: ...

def _package(
    self,
    data_dir: Path,
    config: dict[str, Any],
    **kwargs: Any,
) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]: ...
```

- **`_import`**: copy, move or link source data from `source_path` into the collection's `data_dir`. Honour
  `self.dry_run` before writing.
- **`_process`**: transform the imported data in place within `data_dir` (rename, generate thumbnails, extract frames,
  and so on).
- **`_package`**: build and return the *dataset mapping* that tells Marimba which files to include, where they go, and
  what metadata to attach.

`config` is the merged collection configuration for the collection being processed. Extra `--extra key=value` arguments
from the CLI arrive in `**kwargs`.

An optional fourth method runs after the dataset directory has been assembled:

```python
def _post_package(self, dataset_dir: Path) -> set[Path]: ...
```

Override it to add or modify files in the packaged dataset (returning the set of paths you changed so the manifest can
be updated). The default returns an empty set.

### The `_package` Return Mapping

`_package` returns a dict whose keys are source file paths and whose values are 3-tuples:

```python
dict[
    Path,                                   # source file path (under data_dir)
    tuple[
        Path,                               # destination path, relative to the dataset data root
        list[BaseMetadata] | None,          # metadata objects for this file, or None
        dict[str, Any] | None,              # ancillary metadata dict, or None
    ],
]
```

For each file to include, map its source path to `(destination_path, [metadata], ancillary)`. Attach metadata to
imagery; pass `None` for the metadata and ancillary slots for files that carry none (for example a sensor CSV copied
verbatim). Marimba mints deterministic per-image UUIDs and computes SHA-256 hashes at packaging time, so you do not set
those yourself.

### Runtime Helpers

Inside your lifecycle methods you have access to:

- `self.config` - the pipeline-level configuration dict (from `pipeline.yml`).
- `self.dry_run` - `True` when the command was invoked with `--dry-run`; gate all filesystem writes on `not
  self.dry_run`.
- `self.logger` - a standard library `logging.Logger` scoped to the pipeline; use it for the audit trail.
- `self._metadata_class` - the metadata class fixed in the constructor.

---

## Metadata Classes

Metadata classes adapt a metadata standard to Marimba's packaging machinery. They live under `marimba.core.schemas` and
all inherit from `BaseMetadata`.

### `BaseMetadata`

`BaseMetadata` (in `marimba.core.schemas.base`) is an abstract class defining the interface Marimba relies on when it
writes dataset metadata, embeds EXIF and builds the summary and map. Concrete subclasses expose the following read-only
properties, each returning the value or `None` where absent:

```python
@property
def datetime(self) -> datetime | None: ...
@property
def latitude(self) -> float | None: ...
@property
def longitude(self) -> float | None: ...
@property
def altitude(self) -> float | None: ...
@property
def context(self) -> str | None: ...
@property
def license(self) -> str | None: ...
@property
def creators(self) -> list[str]: ...
@property
def hash_sha256(self) -> str | None: ...   # settable; Marimba populates it at packaging time
```

It also declares two static methods Marimba calls during packaging - `create_dataset_metadata(...)` (writes the
dataset-level metadata file) and `process_files(...)` (embeds per-file metadata such as EXIF). You rarely call these
directly; they are invoked by `DatasetWrapper` when packaging.

### `iFDOMetadata`

`iFDOMetadata` (in `marimba.core.schemas.ifdo`) is the default and most complete implementation, targeting the
[iFDO](https://marine-imaging.com/fair/ifdos/iFDO-overview/) standard. Construct it from an `ifdo` `ImageData` object
(or a list of them, for multi-frame video):

```python
from datetime import datetime, timezone

from ifdo.models import ImageData
from marimba.core.schemas.ifdo import iFDOMetadata

image_data = ImageData(
    image_datetime=datetime(2026, 2, 14, 3, 21, 0, tzinfo=timezone.utc),
    image_latitude=-23.5,
    image_longitude=152.0,
)
metadata = iFDOMetadata(image_data)
```

`ImageData` and its supporting types (`ImageContext`, `ImagePI`, `ImageCreator`, `ImageLicense`, and the various capture
enumerations) come from the [`ifdo`](https://pypi.org/project/ifdo/) package. See the
[Pipeline Implementation Guide](pipeline.md) for the full set of iFDO fields and which are required.

### `DarwinCoreMetadata` and `GenericMetadata`

Two further implementations are available for pipelines that emit other standards:

- `DarwinCoreMetadata` (in `marimba.core.schemas.darwin`) - for [Darwin Core](https://dwc.tdwg.org/) biological
  occurrence data.
- `GenericMetadata` (in `marimba.core.schemas.generic`) - a flexible dictionary-backed implementation for metadata that
  does not fit iFDO or Darwin Core.

Both expose the same `BaseMetadata` property interface, so the rest of the packaging pipeline treats them uniformly.

---

## Standard Library

`marimba.lib` provides helpers for the common image, GPS and EXIF operations pipelines perform during `_process` and
`_package`.

### `marimba.lib.image`

Image manipulation built on Pillow and OpenCV. Selected functions:

```python
from marimba.lib import image

# Write a thumbnail of `image` into `output_directory`; returns the thumbnail path.
image.generate_image_thumbnail(image: Path, output_directory: Path, suffix: str = "_THUMB") -> Path

# Resize to fit within a bounding box, preserving aspect ratio (writes to `destination` or in place).
image.resize_fit(path, max_width: int = 1920, max_height: int = 1080, destination=None) -> None

# Resize to exact dimensions.
image.resize_exact(path, width, height, destination=None) -> None

# Convert an image to JPEG at the given quality; returns the output path.
image.convert_to_jpeg(path, quality: int = 95, destination=None) -> Path

# Compose a grid montage from many images; returns the list of written grid image paths.
image.create_grid_image(paths, destination, columns: int = 5, column_width: int = 300, max_height: int = 32000) -> list[Path]

# Read dimensions without fully decoding.
image.get_width_height(path) -> tuple[int, int]

# Simple blur detection via variance of the Laplacian.
image.is_blurry(path, threshold: float = 100.0) -> bool
```

Further helpers include `scale`, `crop`, `rotate_clockwise`, `turn_clockwise`, `flip_vertical`, `flip_horizontal`,
`sharpen`, `gaussian_blur`, `apply_clahe`, `get_shannon_entropy` and `get_average_image_color`.

### `marimba.lib.gps`

Coordinate conversion and extraction:

```python
from marimba.lib import gps

gps.convert_gps_coordinate_to_degrees(...)                 # DMS components -> decimal degrees
gps.convert_degrees_to_gps_coordinate(degrees: float) -> tuple[int, int, int]   # decimal -> (deg, min, sec)
gps.parse_location_from_metadata(metadata) -> tuple[float | None, float | None]  # (lat, lon) from an EXIF dict
gps.read_exif_location(path) -> tuple[float | None, float | None]                # (lat, lon) from a file
```

### `marimba.lib.exif`

Reading EXIF, with a batched session for efficiency over many files:

```python
from marimba.lib import exif

tags = exif.get_dict(path)      # read all EXIF tags for one file as a dict

with exif.session() as et:      # batched ExifTool session for many files
    ...
```

---

## A Complete Worked Example

The following is a minimal but complete pipeline. It imports JPEG images, generates thumbnails, and packages each image
with iFDO metadata. It is the pattern a real pipeline follows; the [Pipeline Implementation Guide](pipeline.md) expands
each step.

```python
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from typing import Any

from ifdo.models import ImageContext, ImageData

from marimba.core.pipeline import BasePipeline
from marimba.core.schemas.base import BaseMetadata
from marimba.core.schemas.ifdo import iFDOMetadata
from marimba.lib import image


class ExamplePipeline(BasePipeline):
    """A minimal pipeline that imports JPEGs and packages them with iFDO metadata."""

    def __init__(
        self,
        root_path: str | Path,
        config: dict[str, Any] | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        super().__init__(root_path, config, dry_run=dry_run, metadata_class=iFDOMetadata)

    @staticmethod
    def get_pipeline_config_schema() -> dict[str, Any]:
        return {"platform_id": "CAM01"}

    @staticmethod
    def get_collection_config_schema() -> dict[str, Any]:
        return {}

    def _import(self, data_dir: Path, source_path: Path, config: dict[str, Any], **kwargs: Any) -> None:
        for source_file in source_path.rglob("*"):
            if source_file.is_file() and source_file.suffix.lower() == ".jpg":
                if not self.dry_run:
                    copy2(source_file, data_dir)
                self.logger.debug(f"Imported {source_file} -> {data_dir}")

    def _process(self, data_dir: Path, config: dict[str, Any], **kwargs: Any) -> None:
        thumbs = data_dir / "thumbnails"
        if not self.dry_run:
            thumbs.mkdir(exist_ok=True)
        for jpg in data_dir.iterdir():
            if jpg.is_file() and jpg.suffix.lower() == ".jpg":
                if not self.dry_run:
                    image.generate_image_thumbnail(jpg, thumbs)
                self.logger.debug(f"Thumbnailed {jpg.name}")

    def _package(
        self,
        data_dir: Path,
        config: dict[str, Any],
        **kwargs: Any,
    ) -> dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]]:
        platform_id = self.config.get("platform_id", "CAM01")
        data_mapping: dict[Path, tuple[Path, list[BaseMetadata] | None, dict[str, Any] | None]] = {}

        for jpg in data_dir.iterdir():
            if not (jpg.is_file() and jpg.suffix.lower() == ".jpg"):
                continue
            image_data = ImageData(
                image_datetime=datetime.now(tz=timezone.utc),
                image_platform=ImageContext(name=platform_id),
            )
            metadata = self._metadata_class(image_data)
            output_path = Path("images") / jpg.name
            data_mapping[jpg] = (output_path, [metadata], None)

        return data_mapping
```

Publish this class as a Git repository (a single `*.pipeline.py` file plus a `requirements.txt`) and install it with
`marimba new pipeline`, then run `marimba import`, `marimba process` and `marimba package` as shown in the
[Overview and CLI Usage Guide](overview.md).

---

## Driving Marimba Programmatically

The wrapper classes let you run the same operations from Python. They live under `marimba.core.wrappers`.

### `ProjectWrapper`

`ProjectWrapper` manages a project directory and orchestrates the import/process/package lifecycle.

```python
from marimba.core.wrappers.project import ProjectWrapper

# Create a new project, or load an existing one
project = ProjectWrapper.create("/data/projects/example")   # classmethod; returns a ProjectWrapper
project = ProjectWrapper("/data/projects/example")           # load existing
```

Key methods:

```python
# Install a pipeline from a Git URL (accept_defaults skips config prompts)
project.create_pipeline(name: str, url: str, config: dict[str, Any], *, accept_defaults: bool = False) -> PipelineWrapper

# Import source data into a collection
project.run_import(
    collection_name: str,
    source_paths: list[Path],
    pipeline_names: list[str],
    extra_args: list[str] | None = None,
    operation: Operation = Operation.copy,
    max_workers: int | None = None,
) -> None

# Process collections with pipelines
project.run_process(
    collection_names: list[str],
    pipeline_names: list[str],
    extra_args: list[str] | None = None,
    max_workers: int | None = None,
    **kwargs: Any,
) -> None

# Build the dataset mapping across collections and pipelines
project.compose(
    dataset_name: str,
    collection_names: list[str],
    pipeline_names: list[str],
    extra_args: list[str] | None = None,
    max_workers: int | None = None,
    **kwargs: Any,
) -> dict   # the composed dataset mapping

# Package a composed mapping into a dataset
project.create_dataset(
    dataset_name: str,
    dataset_mapping: dict,
    metadata_mapping_processor_decorator: list,
    post_package_processors: list,
    operation: Operation = Operation.copy,
    version: str | None = "1.0",
    contact_name: str | None = None,
    contact_email: str | None = None,
    zoom: int | None = None,
    max_workers: int | None = None,
    *,
    force: bool = False,
    exif_chunk_size: int | None = None,
    allow_destination_collisions: bool = False,
) -> DatasetWrapper

# Distribute a packaged dataset to a configured target
project.distribute(dataset_name: str, target_name: str, validate: bool) -> None
```

`ProjectWrapper` also exposes `create_collection`, `create_target`, `install_pipelines`, `update_pipelines`, the
`delete_*` methods, and read-only properties `root_dir`, `name`, `pipeline_wrappers`, `collection_wrappers`,
`dataset_wrappers`, `target_wrappers` and `dry_run`. `get_pipeline_post_processors(pipeline_names)` returns the
post-package processors to pass to `create_dataset`.

A programmatic package mirrors what the CLI does - compose the mapping, then create the dataset:

```python
pipeline_names = list(project.pipeline_wrappers.keys())
collection_names = list(project.collection_wrappers.keys())

mapping = project.compose("my-dataset", collection_names, pipeline_names)
dataset = project.create_dataset(
    "my-dataset",
    mapping,
    metadata_mapping_processor_decorator=[],
    post_package_processors=project.get_pipeline_post_processors(pipeline_names),
    contact_name="Your Name",
    contact_email="you@example.org",
)
```

### `PipelineWrapper`

Manages a single installed pipeline directory: cloning/installing the repository, loading and saving its configuration,
and instantiating the pipeline class.

```python
from marimba.core.wrappers.pipeline import PipelineWrapper

pw = PipelineWrapper("/data/projects/example/pipelines/my-pipeline")
pw.load_config() -> dict[str, Any]
pw.get_instance() -> BasePipeline | None    # instantiate the pipeline class
pw.install() -> None                         # install its requirements
pw.update() -> None                          # git pull the repository
```

### `CollectionWrapper`

Manages a collection directory and its per-pipeline data directories.

```python
from marimba.core.wrappers.collection import CollectionWrapper

cw = CollectionWrapper("/data/projects/example/collections/dive-001")
cw.load_config() -> dict[str, Any]
cw.get_pipeline_data_dir(pipeline_name: str) -> Path
```

### `DatasetWrapper`

Represents a packaged dataset directory. It is normally produced by `ProjectWrapper.create_dataset`, but can be opened
directly to validate an existing dataset against its manifest:

```python
from marimba.core.wrappers.dataset import DatasetWrapper

dw = DatasetWrapper("/data/projects/example/datasets/my-dataset")
dw.validate()   # raises DatasetWrapper.ManifestError on a mismatch
```

Useful read-only properties include `root_dir`, `name`, `data_dir`, `manifest_path`, `version`, `contact_name`,
`contact_email` and `image_set_uuid`.

### `DistributionTargetWrapper`

Represents a distribution target configuration and resolves the concrete target implementation (for example S3):

```python
from marimba.core.wrappers.target import DistributionTargetWrapper

tw = DistributionTargetWrapper("/data/projects/example/targets/s3-public.yml")
target = tw.get_instance()   # the concrete DistributionTargetBase implementation, or None
```

---

## Exceptions

All Marimba errors derive from a common base class, `MarimbaError`, importable from `marimba.core`:

```python
from marimba.core import MarimbaError
```

Wrapper-specific errors are defined as nested classes on the wrapper that raises them, so catch them via the wrapper.
For example, `ProjectWrapper` defines `NoSuchPipelineError`, `NoSuchCollectionError`, `NoSuchDatasetError`,
`NoSuchTargetError`, `InvalidNameError`, `CreatePipelineError`, `CreateCollectionError`, `CompositionError`,
`InvalidStructureError` and others; `DatasetWrapper` defines `ManifestError`, `InvalidStructureError` and
`InvalidDatasetMappingError`. Because they all inherit from `MarimbaError`, you can catch broadly or narrowly:

```python
try:
    project.run_process(collection_names, pipeline_names)
except ProjectWrapper.NoSuchCollectionError:
    ...                       # handle a specific failure
except MarimbaError:
    ...                       # handle any Marimba error
```

---

## Logging

Marimba uses the standard library `logging` module throughout, accessed via a helper:

```python
from marimba.core.utils.log import get_logger

logger = get_logger(__name__)
logger.info("Processing complete")
```

Inside a pipeline, prefer the ready-made `self.logger` rather than creating your own. Keep library code free of
`print(...)`; the logger is the audit trail that is captured into `project.log` and the per-pipeline logs, and copied
into every packaged dataset.

For more on the pipeline lifecycle these APIs support, see the [Pipeline Implementation Guide](pipeline.md). For the
command-line equivalents, see the [Overview and CLI Usage Guide](overview.md) and the
[CLI Scripting Guide](cli.md).
