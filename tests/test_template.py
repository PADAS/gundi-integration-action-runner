"""The copier scaffold: generation correctness + generated-project e2e."""
import subprocess
import sys


def test_generates_expected_tree(generate_project):
    dst = generate_project()
    for path in (
        "pyproject.toml", "main.py", "Dockerfile", ".env.example", "README.md",
        "conftest.py", "acme_tracker/__init__.py", "acme_tracker/handlers.py",
        "acme_tracker/configurations.py", "acme_tracker/client.py",
        "acme_tracker/transformers.py", "tests/test_handlers.py",
        ".copier-answers.yml",
    ):
        assert (dst / path).exists(), f"missing {path}"
    handlers = (dst / "acme_tracker" / "handlers.py").read_text()
    assert "@action.auth" in handlers
    assert "@action.pull" in handlers
    assert "@webhook" not in handlers  # include_webhook=False default


def test_webhook_variant(generate_project):
    dst = generate_project(include_webhook=True, include_pull=False)
    handlers = (dst / "acme_tracker" / "handlers.py").read_text()
    assert "@webhook" in handlers
    assert "@action.pull" not in handlers
    configurations = (dst / "acme_tracker" / "configurations.py").read_text()
    assert "AcmeTrackerWebhookPayload" in configurations
    for py in (dst / "acme_tracker").rglob("*.py"):
        compile(py.read_text(), str(py), "exec")


def test_generated_files_are_valid_python(generate_project):
    dst = generate_project(include_webhook=True)
    for py in dst.rglob("*.py"):
        compile(py.read_text(), str(py), "exec")


def test_generated_project_test_suite_passes(generate_project):
    """The end-to-end contract: a fresh scaffold's own tests pass using the
    installed library + its pytest plugin (fixtures with no conftest wiring)."""
    dst = generate_project()
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q"],
        cwd=dst, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "2 passed" in result.stdout


def test_local_dev_stack(generate_project):
    import yaml

    dst = generate_project()
    for path in (
        "local/docker-compose.yml", "local/helpers/create_subscriptions.sh",
        "local/.env.local.example", "local/.gitignore", "local/LOCAL_DEVELOPMENT.md",
    ):
        assert (dst / path).exists(), f"missing {path}"

    compose = yaml.safe_load((dst / "local" / "docker-compose.yml").read_text())
    assert set(compose["services"]) == {
        "redis", "pubsub_emulator", "pubsub_topic_initializer", "connector"
    }
    connector = compose["services"]["connector"]
    assert connector["build"]["target"] == "dev"
    assert any("acme_tracker" in volume for volume in connector["volumes"])
    assert connector["depends_on"]["pubsub_topic_initializer"]["condition"] == (
        "service_completed_successfully"
    )

    helper = (dst / "local" / "helpers" / "create_subscriptions.sh").read_text()
    assert "http://connector:8080/" in helper
    assert "local-actions-topic" in helper

    env_example = (dst / "local" / ".env.local.example").read_text()
    assert 'INTEGRATION_TYPE_SLUG="acme_tracker"' in env_example
    assert "PUBSUB_EMULATOR_HOST=pubsub_emulator:8085" in env_example
    assert "INTEGRATION_COMMANDS_TOPIC=local-actions-topic" in env_example

    # Dual-mode auth: personal login default, OAUTH_* names only
    assert "GUNDI_USERNAME=" in env_example
    assert "GUNDI_PASSWORD=" in env_example
    assert 'OAUTH_CLIENT_ID="cdip-oauth2"' in env_example
    assert "KEYCLOAK_" not in env_example
    root_env = (dst / ".env.example").read_text()
    assert "OAUTH_CLIENT_ID=" in root_env
    assert "KEYCLOAK_" not in root_env

    dockerfile = (dst / "Dockerfile").read_text()
    assert "AS dev" in dockerfile and "AS prod" in dockerfile
    # prod must be the LAST stage so a bare `docker build .` builds production
    assert dockerfile.rindex("AS prod") > dockerfile.rindex("AS dev")
    assert "debugpy" in dockerfile
