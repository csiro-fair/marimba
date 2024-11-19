"""
Marimba Prompt Utilities.

This module provides functionality to prompt a user for input values based on a provided schema. The schema is a
dictionary mapping field names to default values. The user is prompted for each field, with the default value used if no
input is provided. The resulting user input is returned as a dictionary with keys matching the schema fields and values
of the appropriate type based on the schema defaults.

Imports:
    - typing.Any: Type hint for any type.
    - typing.Dict: Type hint for dictionaries.
    - typing.Optional: Type hint for optional values.
    - rich.prompt.Confirm: Function for prompting the user with a yes/no question.
    - rich.prompt.FloatPrompt: Function for prompting the user for a float value.
    - rich.prompt.IntPrompt: Function for prompting the user for an integer value.
    - rich.prompt.Prompt: Function for prompting the user for a string value.

Functions:
    - prompt_schema(schema: Dict[str, Any]) -> Optional[Dict[str, Any]]: Prompts the user for values for each field in
    the provided schema, returning a dictionary of user input values with keys matching the schema fields,
    or None if the input was interrupted.
"""

from typing import Any

from rich.prompt import Confirm, FloatPrompt, IntPrompt, Prompt


def prompt_schema(schema: dict[str, Any]) -> dict[str, Any] | None:
    """
    Prompt the user for values for each field in the schema.

    The schema is given as a dictionary of field names to defaults.
    The user will be prompted for a value for each field, and the default will be used if no value is entered.
    User values will be converted into the appropriate type as given by the schema default.

    Supported types:
    - str
    - int
    - float
    - bool

    Args:
        schema: The schema to prompt the user for.

    Returns:
        The user values as a dictionary, or None if the input was interrupted.

    Raises:
        NotImplementedError: If the schema contains a type that is not supported.
    """
    user_values = schema.copy()

    try:
        for key, default_value in schema.items():
            value_type = type(default_value)
            if value_type is bool:
                value = Confirm.ask(key, default=default_value)
            elif value_type is int:
                value = IntPrompt.ask(key, default=default_value)
            elif value_type is float:
                value = FloatPrompt.ask(key, default=default_value)
            elif value_type is str:
                value = Prompt.ask(key, default=default_value)
            else:
                raise NotImplementedError(f"Unsupported type: {value_type.__name__}")
            if value is not None:
                user_values[key] = value
    except KeyboardInterrupt:
        return None

    return user_values
