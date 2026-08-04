"""gundi-runner: developer CLI for building and operating Gundi connectors."""
import asyncio
import os

import click
import pydantic

from gundi_action_runner import settings
from gundi_action_runner.services.action_runner import _portal
from gundi_action_runner.services.action_scheduler import CrontabSchedule
from gundi_action_runner.services.self_registration import register_integration_in_gundi


def _apply_handlers_setting(handlers):
    """Point discovery at the given handler modules for this process AND
    any child process (uvicorn --reload workers read the env var)."""
    if not handlers:
        return
    value = ",".join(handlers)
    os.environ["GUNDI_HANDLERS_MODULES"] = value
    settings.GUNDI_HANDLERS_MODULES = value


@click.group()
def cli():
    """Build, run, and register Gundi action-runner connectors."""


@cli.command()
@click.option("--handlers", "-m", multiple=True,
              help="Import path(s) of modules registering handlers with @action/@webhook. "
                   "Defaults to GUNDI_HANDLERS_MODULES or the legacy app.* convention.")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8080, show_default=True, type=int)
@click.option("--reload/--no-reload", default=True, show_default=True)
def run(handlers, host, port, reload):
    """Run the connector locally with uvicorn."""
    import uvicorn

    _apply_handlers_setting(handlers)
    uvicorn.run(
        "gundi_action_runner.app_factory:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload,
    )


@cli.command()
@click.option("--slug", default=None, help="Slug ID for the integration type")
@click.option("--name", default=None,
              help="Display name for the integration type (defaults to a name derived from the slug)")
@click.option("--service-url", default=None,
              help="Service URL used to trigger actions or receive webhooks")
@click.option("--handlers", "-m", multiple=True,
              help="Import path(s) of handler modules (defaults to env/legacy discovery)")
@click.option("--schedule", multiple=True,
              help='Schedules as "action_id:crontab" (e.g. "pull_events:0 */4 * * *")')
def register(slug, name, service_url, handlers, schedule):
    """Register this integration type (actions, schemas, schedules) in Gundi."""
    _apply_handlers_setting(handlers)
    schedules = {}
    for item in schedule:
        try:
            action_id, cron = item.split(":", 1)
            schedules[action_id.strip()] = CrontabSchedule.parse_obj_from_crontab(cron.strip())
        except (pydantic.ValidationError, ValueError) as e:
            raise click.BadParameter(
                f"Invalid schedule format: {item}.\n"
                f"Expected 'action_id:MIN HOUR DOM MON DOW [TZ]', "
                f"e.g. 'pull_events:0 */4 * * * -5'.\n{e}"
            )
    asyncio.run(
        register_integration_in_gundi(
            gundi_client=_portal,
            type_slug=slug,
            type_name=name,
            service_url=service_url,
            action_schedules=schedules,
        )
    )


DEFAULT_TEMPLATE = "gh:PADAS/gundi-integration-action-runner"


@cli.command()
@click.argument("destination", type=click.Path())
@click.option("--template", default=DEFAULT_TEMPLATE, show_default=True,
              help="Copier template source (git URL or local path)")
@click.option("--vcs-ref", default=None,
              help="Template git ref; defaults to the tag matching the installed library "
                   "version (vX.Y.Z) when using the official template")
@click.option("--data", "data_pairs", multiple=True,
              help="Answer as KEY=VALUE (repeatable); unanswered questions prompt interactively")
@click.option("--defaults", is_flag=True, default=False,
              help="Fill unanswered questions with their defaults instead of prompting "
                   "(recommended for CI/scripted use; incomplete answers without this flag "
                   "in a non-TTY produce a broken scaffold — copier does not error)")
def new(destination, template, vcs_ref, data_pairs, defaults):
    """Scaffold a new connector project from the official template."""
    try:
        import copier
    except ImportError:
        raise click.UsageError(
            "copier is not installed. Install the CLI extras first:\n"
            "  pip install 'gundi-action-runner[cli]'"
        )
    if template == DEFAULT_TEMPLATE and vcs_ref is None:
        from gundi_action_runner import __version__
        vcs_ref = f"v{__version__}"
    data = {}
    for pair in data_pairs:
        if "=" not in pair:
            raise click.BadParameter(f"--data expects KEY=VALUE, got {pair!r}")
        key, _, value = pair.partition("=")
        if value.lower() in ("true", "false"):
            value = value.lower() == "true"
        data[key] = value
    copier.run_copy(
        template, destination, data=data, vcs_ref=vcs_ref, defaults=defaults,
    )
    click.echo(
        f"\nProject created at {destination}. Next steps:\n"
        f"  cd {destination}\n"
        f"  pip install -e '.[dev]'\n"
        f"  pytest\n"
        f"  gundi-runner run --handlers <package>.handlers"
    )


_CONFIG_BASES = {
    "auth": "AuthActionConfiguration",
    "pull": "PullActionConfiguration",
    "push": "PushActionConfiguration",
    "generic": "GenericActionConfiguration",
}


def _find_package_dir(explicit):
    import pathlib

    if explicit:
        pkg = pathlib.Path(explicit)
        if not (pkg / "handlers.py").exists() or not (pkg / "configurations.py").exists():
            raise click.UsageError(
                f"Could not locate handlers.py + configurations.py under {pkg}."
            )
        return pkg
    candidates = [
        d for d in pathlib.Path(".").iterdir()
        if d.is_dir() and not d.name.startswith((".", "_"))
        and (d / "handlers.py").exists() and (d / "configurations.py").exists()
    ]
    if len(candidates) != 1:
        raise click.UsageError(
            "Could not locate a connector package (a directory containing "
            "handlers.py and configurations.py). Run from your project root "
            "or pass --package."
        )
    return candidates[0]


def _class_name(action_id, suffix):
    return "".join(part.capitalize() for part in action_id.split("_")) + suffix


@cli.command("add-action")
@click.option("--type", "action_type", type=click.Choice(list(_CONFIG_BASES)),
              prompt="Action type")
@click.option("--id", "action_id", prompt="Action id (snake_case)",
              callback=lambda ctx, param, value: value.strip())
@click.option("--title", default="", help="Display name shown in the Gundi portal")
@click.option("--crontab", default="",
              help="Crontab schedule (pull actions only), e.g. '*/15 * * * *'")
@click.option("--package", "package_dir", default=None,
              help="Connector package directory (auto-detected by default)")
def add_action(action_type, action_id, title, crontab, package_dir):
    """Append a stub handler + config class to your connector."""
    if not action_id.isidentifier() or action_id.lower() != action_id:
        raise click.UsageError(f"'{action_id}' is not a valid snake_case identifier.")
    pkg = _find_package_dir(package_dir)
    handlers_path, config_path = pkg / "handlers.py", pkg / "configurations.py"
    handlers_src = handlers_path.read_text()
    if f"def {action_id}(" in handlers_src:
        raise click.UsageError(f"{handlers_path} already defines '{action_id}'.")

    config_cls = _class_name(action_id, "Config")
    base = _CONFIG_BASES[action_type]
    config_block = (
        f"\n\nclass {config_cls}({base}):\n"
        f"    # Add configuration fields (FieldWithUIOptions) here.\n"
        f"    pass\n"
    )
    imports = [f"from gundi_action_runner.actions.core import {base}"]
    if action_type == "push":
        data_cls = _class_name(action_id, "Data")
        config_block += (
            f"\n\nclass {data_cls}(pydantic.BaseModel):\n"
            f"    # Shape of the data this push action receives.\n"
            f"    pass\n"
        )
        imports.append("import pydantic")

    title_arg = f', title="{title}"' if title else ""
    handler_lines = [
        "",
        "",
        f"from .configurations import {config_cls}" + (
            f", {_class_name(action_id, 'Data')}" if action_type == "push" else ""
        ),
    ]
    if crontab:
        handler_lines.append("from gundi_action_runner.services.action_scheduler import crontab_schedule")
    handler_lines.append("")
    handler_lines.append(f"@action.{action_type}(config={config_cls}{title_arg})")
    if crontab:
        handler_lines.append(f'@crontab_schedule("{crontab}")')
    if action_type == "push":
        handler_lines.append(
            f"async def {action_id}(integration, action_config, "
            f"data: {_class_name(action_id, 'Data')}, metadata):"
        )
    else:
        handler_lines.append(f"async def {action_id}(integration, action_config):")
    handler_lines.append("    # Implement the action; return a summary dict.")
    handler_lines.append("    return {}")
    handler_lines.append("")

    config_src = config_path.read_text()
    import_lines = "\n".join(i for i in imports if i not in config_src)
    config_path.write_text(
        config_src.rstrip("\n") + "\n"
        + (("\n" + import_lines + "\n") if import_lines else "")
        + config_block
    )
    handlers_path.write_text(handlers_src.rstrip("\n") + "\n" + "\n".join(handler_lines))
    click.echo(f"Added '{action_id}' ({action_type}) to {handlers_path} and {config_path}.")


if __name__ == "__main__":
    cli()
