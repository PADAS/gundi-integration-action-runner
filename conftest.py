# Root conftest: enables the `pytester` fixture for plugin self-tests and
# anchors pytest's rootdir-based sys.path insertion, so isolated runs like
# `pytest tests/test_registry.py` can import the in-repo `app` package.
pytest_plugins = ["pytester"]
