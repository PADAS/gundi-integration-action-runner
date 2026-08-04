from gundi_action_runner.testing.fixtures import *  # noqa: F401,F403

import pathlib
import shutil

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def staged_template(tmp_path_factory):
    """Copy copier.yml + template/ into a plain (non-git) dir so copier
    generation is deterministic regardless of this repo's git state."""
    stage = tmp_path_factory.mktemp("template-stage")
    shutil.copy(REPO_ROOT / "copier.yml", stage / "copier.yml")
    shutil.copytree(REPO_ROOT / "template", stage / "template")
    return stage


@pytest.fixture
def generate_project(staged_template, tmp_path):
    """Generate a scaffolded connector project; returns its path."""
    import copier

    def _generate(**answers):
        defaults = {
            "project_name": "Acme Tracker",
            "package_name": "acme_tracker",
            "integration_type_slug": "acme_tracker",
            "display_name": "Acme Tracker",
            "include_pull": True,
            "include_webhook": False,
        }
        defaults.update(answers)
        dst = tmp_path / "generated"
        copier.run_copy(
            str(staged_template), str(dst), data=defaults,
            defaults=True, overwrite=True, quiet=True,
        )
        return dst

    return _generate
