"""gundi-runner CLI: run + register (new/add-action tested in their own tasks)."""
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

from gundi_action_runner.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


def test_cli_group_lists_commands(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    for command in ("run", "register"):
        assert command in result.output


def test_run_invokes_uvicorn_with_factory(runner, mocker, monkeypatch):
    monkeypatch.delenv("GUNDI_HANDLERS_MODULES", raising=False)
    monkeypatch.setattr("gundi_action_runner.settings.GUNDI_HANDLERS_MODULES", None)
    uvicorn_run = mocker.patch("uvicorn.run")
    result = runner.invoke(
        cli, ["run", "--handlers", "myconn.handlers", "--port", "9000"]
    )
    assert result.exit_code == 0, result.output
    args, kwargs = uvicorn_run.call_args
    assert args[0] == "gundi_action_runner.app_factory:create_app"
    assert kwargs["factory"] is True
    assert kwargs["port"] == 9000
    # The reload subprocess reads env; the in-process factory reads settings
    import os

    from gundi_action_runner import settings

    assert os.environ["GUNDI_HANDLERS_MODULES"] == "myconn.handlers"
    assert settings.GUNDI_HANDLERS_MODULES == "myconn.handlers"


def test_register_forwards_options(runner, mocker):
    register = mocker.patch(
        "gundi_action_runner.cli.register_integration_in_gundi", new_callable=AsyncMock
    )
    result = runner.invoke(
        cli,
        [
            "register",
            "--slug", "x_tracker",
            "--name", "X Tracker",
            "--service-url", "https://x.example.com",
            "--schedule", "pull_observations:0 */4 * * *",
        ],
    )
    assert result.exit_code == 0, result.output
    kwargs = register.call_args.kwargs
    assert kwargs["type_slug"] == "x_tracker"
    assert kwargs["type_name"] == "X Tracker"
    assert kwargs["service_url"] == "https://x.example.com"
    assert "pull_observations" in kwargs["action_schedules"]


def test_register_rejects_bad_schedule(runner, mocker):
    mocker.patch(
        "gundi_action_runner.cli.register_integration_in_gundi", new_callable=AsyncMock
    )
    result = runner.invoke(cli, ["register", "--schedule", "not-a-schedule"])
    assert result.exit_code != 0
    assert "Invalid schedule format" in result.output


def test_new_generates_project_from_local_template(runner, staged_template, tmp_path):
    dst = tmp_path / "my-connector"
    result = runner.invoke(
        cli,
        [
            "new", str(dst),
            "--template", str(staged_template),
            "--defaults",
            "--data", "project_name=Acme Tracker",
            "--data", "include_pull=true",
            "--data", "include_webhook=false",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (dst / "acme_tracker" / "handlers.py").exists()
    assert "Next steps" in result.output


def test_new_default_template_pins_ref_to_library_version(runner, mocker, tmp_path):
    run_copy = mocker.patch("copier.run_copy")
    from gundi_action_runner import __version__

    result = runner.invoke(cli, ["new", str(tmp_path / "x"), "--defaults"])
    assert result.exit_code == 0, result.output
    assert run_copy.call_args.kwargs["vcs_ref"] == f"v{__version__}"


def test_new_rejects_data_without_equals(runner, tmp_path):
    result = runner.invoke(cli, ["new", str(tmp_path / "x"), "--data", "project_name"])
    assert result.exit_code != 0
    assert "KEY=VALUE" in result.output


def test_new_requires_copier(runner, mocker, tmp_path):
    import builtins

    real_import = builtins.__import__

    def _no_copier(name, *args, **kwargs):
        if name == "copier":
            raise ImportError("No module named 'copier'")
        return real_import(name, *args, **kwargs)

    mocker.patch("builtins.__import__", side_effect=_no_copier)
    result = runner.invoke(cli, ["new", str(tmp_path / "x")])
    assert result.exit_code != 0
    assert "pip install 'gundi-action-runner[cli]'" in result.output


@pytest.fixture
def scaffolded_project(generate_project, monkeypatch):
    dst = generate_project()
    monkeypatch.chdir(dst)
    return dst


def test_add_action_appends_pull_stub(runner, scaffolded_project):
    result = runner.invoke(
        cli,
        ["add-action", "--type", "pull", "--id", "pull_events",
         "--title", "Pull Events", "--crontab", "0 */2 * * *"],
    )
    assert result.exit_code == 0, result.output
    handlers = (scaffolded_project / "acme_tracker" / "handlers.py").read_text()
    configurations = (scaffolded_project / "acme_tracker" / "configurations.py").read_text()
    assert '@action.pull(config=PullEventsConfig, title="Pull Events")' in handlers
    assert '@crontab_schedule("0 */2 * * *")' in handlers
    assert "async def pull_events(integration, action_config):" in handlers
    assert "class PullEventsConfig(PullActionConfiguration):" in configurations
    # Still valid python
    compile(handlers, "handlers.py", "exec")
    compile(configurations, "configurations.py", "exec")


def test_add_action_push_generates_data_model(runner, scaffolded_project):
    result = runner.invoke(
        cli, ["add-action", "--type", "push", "--id", "push_positions"]
    )
    assert result.exit_code == 0, result.output
    handlers = (scaffolded_project / "acme_tracker" / "handlers.py").read_text()
    configurations = (scaffolded_project / "acme_tracker" / "configurations.py").read_text()
    assert "data: PushPositionsData" in handlers
    assert "class PushPositionsData(pydantic.BaseModel):" in configurations


def test_add_action_refuses_duplicate_id(runner, scaffolded_project):
    result = runner.invoke(
        cli, ["add-action", "--type", "pull", "--id", "pull_observations"]
    )
    assert result.exit_code != 0
    assert "already defines" in result.output


def test_add_action_refuses_outside_project(runner, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(cli, ["add-action", "--type", "auth", "--id", "auth2"])
    assert result.exit_code != 0
    assert "Could not locate" in result.output
