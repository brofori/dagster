import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Iterator, Optional, Sequence, Tuple, Type, Union

from click.testing import CliRunner, Result
from dagster_dg.cli import (
    DG_CLI_MAX_OUTPUT_WIDTH,
    cli as dg_cli,
)
from dagster_dg.utils import discover_git_root, pushd
from typing_extensions import Self


@contextmanager
def isolated_example_deployment_foo(runner: Union[CliRunner, "ProxyRunner"]) -> Iterator[None]:
    runner = ProxyRunner(runner) if isinstance(runner, CliRunner) else runner
    with runner.isolated_filesystem():
        runner.invoke("deployment", "generate", "foo")
        with pushd("foo"):
            yield


@contextmanager
def isolated_example_code_location_bar(
    runner: Union[CliRunner, "ProxyRunner"], in_deployment: bool = True, skip_venv: bool = False
) -> Iterator[None]:
    runner = ProxyRunner(runner) if isinstance(runner, CliRunner) else runner
    dagster_git_repo_dir = str(discover_git_root(Path(__file__)))
    if in_deployment:
        with isolated_example_deployment_foo(runner):
            runner.invoke(
                "code-location",
                "generate",
                "--use-editable-dagster",
                dagster_git_repo_dir,
                *(["--skip-venv"] if skip_venv else []),
                "bar",
            )
            with pushd("code_locations/bar"):
                yield
    else:
        with runner.isolated_filesystem():
            runner.invoke(
                "code-location",
                "generate",
                "--use-editable-dagster",
                dagster_git_repo_dir,
                *(["--skip-venv"] if skip_venv else []),
                "bar",
            )
            with pushd("bar"):
                yield


@dataclass
class ProxyRunner:
    original: CliRunner
    append_args: Optional[Sequence[str]] = None

    @classmethod
    @contextmanager
    def test(
        cls, use_test_component_lib: bool = True, verbose: bool = False, disable_cache: bool = False
    ) -> Iterator[Self]:
        with TemporaryDirectory() as cache_dir:
            append_opts = [
                *(
                    ["--builtin-component-lib", "dagster_components.test"]
                    if use_test_component_lib
                    else []
                ),
                "--cache-dir",
                str(cache_dir),
                *(["--verbose"] if verbose else []),
                *(["--disable-cache"] if disable_cache else []),
            ]
            yield cls(CliRunner(), append_args=append_opts)

    def invoke(self, *args: str):
        # We need to find the right spot to inject global options. For the `dg component generate`
        # command, we need to inject the global options before the final subcommand. For everything
        # else they can be appended at the end of the options.
        if args[:2] == ("component", "generate"):
            index = 2
        elif "--help" in args:
            index = args.index("--help")
        elif "--" in args:
            index = args.index("--")
        else:
            index = len(args)
        all_args = [*args[:index], *(self.append_args or []), *args[index:]]

        # For some reason the context setting `max_content_width` is not respected when using the
        # CliRunner, so we have to set it manually.
        return self.original.invoke(dg_cli, all_args, terminal_width=DG_CLI_MAX_OUTPUT_WIDTH)

    @contextmanager
    def isolated_filesystem(self) -> Iterator[None]:
        with self.original.isolated_filesystem():
            yield


def assert_runner_result(result: Result, exit_0: bool = True) -> None:
    try:
        assert result.exit_code == 0 if exit_0 else result.exit_code != 0
    except AssertionError:
        if result.output:
            print(result.output)  # noqa: T201
        if result.exc_info:
            print_exception_info(result.exc_info)
        raise


def print_exception_info(
    exc_info: Tuple[Type[BaseException], BaseException, TracebackType],
) -> None:
    """Prints a nicely formatted traceback for the current exception."""
    exc_type, exc_value, exc_traceback = exc_info
    print("Exception Traceback (most recent call last):")  # noqa: T201
    formatted_traceback = "".join(traceback.format_tb(exc_traceback))
    print(formatted_traceback)  # noqa: T201
    print(f"{exc_type.__name__}: {exc_value}")  # noqa: T201
