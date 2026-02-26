import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires a running Hadoop cluster)",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration"):
        return
    skip_integration = pytest.mark.skip(reason="Need --integration flag to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
