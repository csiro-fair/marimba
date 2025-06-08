import pytest

from marimba.core.installer.uv_executor import UvExecutor


def test_uv_executor_output():
    uv_executor = UvExecutor.create()
    result = uv_executor("list")

    # Should succeed with empty list if no packages installed in current env
    assert result.error == ""


def test_uv_executor_error():
    uv_executor = UvExecutor.create()

    with pytest.raises(UvExecutor.UvError):
        uv_executor("install abaöskjdsök")
