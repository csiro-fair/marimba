"""Collection-config schema resolution helpers used by ProjectWrapper.prompt_collection_config.

Extracted from ProjectWrapper to keep that class focused on lifecycle / orchestration. These helpers
walk the project's loaded pipeline + collection wrappers to assemble the final config dict that
prompt_collection_config returns; they have no implicit dependency on ProjectWrapper state beyond
the wrapper dicts and the logger.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from marimba.core import MarimbaError
from marimba.core.utils.prompt import prompt_schema

if TYPE_CHECKING:
    import logging

    from marimba.core.wrappers.collection import CollectionWrapper
    from marimba.core.wrappers.pipeline import PipelineWrapper


class NoSuchParentCollectionError(MarimbaError):
    """Raised when a referenced parent collection does not exist.

    Internal to this helper module; ProjectWrapper translates it to ProjectWrapper.NoSuchCollectionError
    at the delegation boundary so the public exception type is unchanged.
    """


def get_unified_collection_schema(pipeline_wrappers: dict[str, PipelineWrapper]) -> dict[str, Any]:
    """Aggregate collection config schemas from all pipelines in the project.

    Raises:
        RuntimeError: If any pipeline wrapper fails to load its pipeline instance.
    """
    schema: dict[str, Any] = {}
    for pipeline_wrapper in pipeline_wrappers.values():
        pipeline = pipeline_wrapper.get_instance()
        if pipeline is None:
            msg = f"Failed to load pipeline instance for '{pipeline_wrapper.name}'. Pipeline may be invalid or empty."
            raise RuntimeError(msg)
        schema.update(pipeline.get_collection_config_schema())
    return schema


def get_last_modified_collection_name(
    collection_wrappers: dict[str, CollectionWrapper],
) -> str | None:
    """Return the name of the most-recently-modified collection, or None if there are no collections."""
    if not collection_wrappers:
        return None
    return max(
        collection_wrappers,
        key=lambda k: collection_wrappers[k].root_dir.stat().st_mtime,
    )


def resolve_parent_collection_name(
    parent_collection_name: str | None,
    collection_wrappers: dict[str, CollectionWrapper],
    logger: logging.Logger,
) -> str | None:
    """Determine the appropriate parent collection name if not specified."""
    if parent_collection_name is None:
        parent_collection_name = get_last_modified_collection_name(collection_wrappers)
        if parent_collection_name:
            logger.info(f'Using last collection "{parent_collection_name}" as parent')
    return parent_collection_name


def update_schema_with_parent_config(
    schema: dict[str, Any],
    parent_collection_name: str | None,
    collection_wrappers: dict[str, CollectionWrapper],
    logger: logging.Logger,
) -> None:
    """Update the schema in-place with values from the parent collection's config, if applicable.

    Raises:
        NoSuchParentCollectionError: If parent_collection_name is set but does not exist in
            collection_wrappers. Callers translate this to the appropriate public exception type.
    """
    if parent_collection_name:
        parent_wrapper = collection_wrappers.get(parent_collection_name)
        if parent_wrapper is None:
            raise NoSuchParentCollectionError(parent_collection_name)
        parent_config = parent_wrapper.load_config()
        schema.update(parent_config)
        logger.info(
            f'Using parent collection "{parent_collection_name}" with config: {parent_config}',
        )


def collect_final_config(
    schema: dict[str, Any],
    provided_config: dict[str, Any] | None,
    logger: logging.Logger,
    *,
    accept_defaults: bool = False,
) -> dict[str, Any]:
    """Combine the user-provided config with additional prompted entries from the schema."""
    final_config = provided_config or {}
    # Prepopulate with existing config and remove keys that will not be prompted
    for key in list(schema.keys()):
        if key in final_config:
            del schema[key]

    if schema:
        additional_config = prompt_schema(schema, accept_defaults=accept_defaults)
        if additional_config:
            final_config.update(additional_config)

    logger.info(f"Provided collection config={final_config}")
    return final_config
