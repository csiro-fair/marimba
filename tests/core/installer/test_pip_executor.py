import pytest

from marimba.core.installer.pip_executor import PipExecutor


def test_pip_executor_output():
    pip_executor = PipExecutor.create()
    result = pip_executor("--version")

    assert result.output[:3] == "pip"
    assert result.error == ""

def test_pip_executor_error():
    pip_executor = PipExecutor.create()

    with pytest.raises(PipExecutor.PipException):
        pip_executor("install abaöskjdsök")
