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

    # Dual-mode auth: personal login default, GUNDI_OAUTH_* names only
    assert "GUNDI_USERNAME=" in env_example
    assert "GUNDI_PASSWORD=" in env_example
    assert 'GUNDI_OAUTH_CLIENT_ID="cdip-oauth2"' in env_example
    assert "KEYCLOAK_" not in env_example
    assert "\nOAUTH_" not in env_example and "# OAUTH_" not in env_example
    root_env = (dst / ".env.example").read_text()
    assert "GUNDI_OAUTH_CLIENT_ID=" in root_env
    assert "KEYCLOAK_" not in root_env and "\nOAUTH_" not in root_env
    # The shared OAuth token cache lives in Redis db 2, beside state (0) and config (1)
    assert "REDIS_TOKEN_CACHE_DB=2" in root_env
    assert "GUNDI_TOKEN_CACHE_URL" in root_env
    assert "REDIS_TOKEN_CACHE_DB" in env_example

    dockerfile = (dst / "Dockerfile").read_text()
    assert "AS dev" in dockerfile and "AS prod" in dockerfile
    # prod must be the LAST stage so a bare `docker build .` builds production
    assert dockerfile.rindex("AS prod") > dockerfile.rindex("AS dev")
    assert "debugpy" in dockerfile


def test_pyproject_pins_a_prerelease_library_exactly(generate_project):
    """pip's `~=0.1` never resolves to a pre-release, so a scaffold generated
    by an rc library could not build its image or run CI until 0.1.0 final.
    `gundi-runner new` passes the installed version; the pin follows it."""
    def pins(**answers):
        text = (generate_project(**answers) / "pyproject.toml").read_text()
        return [line.strip().strip('",') for line in text.splitlines() if "gundi-action-runner" in line]

    assert pins(runner_version="0.1.0rc4") == ["gundi-action-runner==0.1.0rc4", "gundi-action-runner[testing]==0.1.0rc4"]
    assert pins(runner_version="0.1.0") == ["gundi-action-runner~=0.1", "gundi-action-runner[testing]~=0.1"]
    assert pins() == ["gundi-action-runner~=0.1", "gundi-action-runner[testing]~=0.1"]  # plain `copier copy`


def test_ci_workflows_mirror_the_fork_pipeline(generate_project):
    """A fork inherits tests-on-PR and tests -> image -> deploy on push. A
    generated connector has to get the same, or it is not a fork replacement."""
    import yaml

    dst = generate_project()
    workflows = dst / ".github" / "workflows"
    for name in ("pr.yaml", "main.yaml", "_tests.yml"):
        assert (workflows / name).exists(), f"missing {name}"

    pr = yaml.safe_load((workflows / "pr.yaml").read_text())
    assert pr["jobs"]["pr_unit_tests"]["uses"] == "./.github/workflows/_tests.yml"

    tests_wf = (workflows / "_tests.yml").read_text()
    assert 'pip install --no-cache-dir -e ".[dev]"' in tests_wf
    assert "run: pytest" in tests_wf

    main = yaml.safe_load((workflows / "main.yaml").read_text())
    jobs = main["jobs"]
    assert set(jobs) == {"vars", "run_unit_tests", "build", "deploy_dev", "deploy_stage", "deploy_prod"}
    assert jobs["run_unit_tests"]["uses"] == "./.github/workflows/_tests.yml"
    assert jobs["build"]["uses"].startswith("PADAS/gundi-workflows/.github/workflows/build_docker.yml@")
    for env in ("dev", "stage", "prod"):
        job = jobs[f"deploy_{env}"]
        assert job["uses"].startswith("PADAS/gundi-workflows/.github/workflows/update_hcl.yml@")
        assert job["with"]["environment"] == env
        assert job["with"]["git_repository"] == "PADAS/gundi-integrations-v2-infra"
        assert job["with"]["key_name"] == "image"
    assert jobs["deploy_prod"]["needs"] == ["vars", "build", "deploy_stage"]

    # The workflows are copied, not rendered: GitHub's own ${{ }} expressions
    # must survive generation untouched.
    raw = (workflows / "main.yaml").read_text()
    assert "${{ github.event.repository.name }}" in raw
    assert "${{ vars.GUNDI_INTEGRATIONS_WORKLOAD_IDENTITY_PROVIDER }}" in raw
    assert "${{ secrets.GUNDI_INTEGRATIONS_DEPLOY_KEY }}" in raw

    # ...and the README tells the author what the pipeline expects of the repo.
    readme = (dst / "README.md").read_text()
    for needed in (
        "GUNDI_INTEGRATIONS_WORKLOAD_IDENTITY_PROVIDER",
        "GUNDI_INTEGRATIONS_SERVICE_ACCOUNT",
        "GUNDI_INTEGRATIONS_DEPLOY_KEY",
        "gundi-integrations-v2-infra",
    ):
        assert needed in readme, needed
