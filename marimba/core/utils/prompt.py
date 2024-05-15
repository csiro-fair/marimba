from typing import Any, Dict, Optional

from rich.prompt import Confirm, FloatPrompt, IntPrompt, Prompt


def prompt_schema(schema: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
            if value_type == bool:
                value = Confirm.ask(key, default=default_value)
            elif value_type == int:
                value = IntPrompt.ask(key, default=default_value)
            elif value_type == float:
                value = FloatPrompt.ask(key, default=default_value)
            elif value_type == str:
                value = Prompt.ask(key, default=default_value)
            else:
                raise NotImplementedError(f"Unsupported type: {value_type.__name__}")
            if value is not None:
                user_values[key] = value
    except KeyboardInterrupt:
        return None

    return user_values
