import textwrap
from pathlib import Path

import pytest
import tomli
from dagster_dg.utils import discover_git_root, ensure_dagster_dg_tests_import, pushd

ensure_dagster_dg_tests_import()

from dagster_dg_tests.utils import (
    ProxyRunner,
    assert_runner_result,
    isolated_example_deployment_foo,
)

# ########################
# ##### GENERATE
# ########################

# At this time all of our tests are against an editable install of dagster-components. The reason
# for this is that this package should always be tested against the corresponding version of
# dagster-copmonents (i.e. from the same commit), and the only way to achieve this right now is
# using the editable install variant of `dg code-location generate`.
#
# Ideally we would have a way to still use the matching dagster-components without using the
# editable install variant, but this will require somehow configuring uv to ensure that it builds
# and returns the local version of the package.


def test_code_location_generate_inside_deployment_success(monkeypatch) -> None:
    # Remove when we are able to test without editable install
    dagster_git_repo_dir = discover_git_root(Path(__file__))
    monkeypatch.setenv("DAGSTER_GIT_REPO_DIR", str(dagster_git_repo_dir))

    with ProxyRunner.test() as runner, isolated_example_deployment_foo(runner):
        result = runner.invoke("code-location", "generate", "bar", "--use-editable-dagster")
        assert_runner_result(result)
        assert Path("code_locations/bar").exists()
        assert Path("code_locations/bar/bar").exists()
        assert Path("code_locations/bar/bar/lib").exists()
        assert Path("code_locations/bar/bar/components").exists()
        assert Path("code_locations/bar/bar_tests").exists()
        assert Path("code_locations/bar/pyproject.toml").exists()

        # Check venv created
        assert Path("code_locations/bar/.venv").exists()
        assert Path("code_locations/bar/uv.lock").exists()

        # Restore when we are able to test without editable install
        # with open("code_locations/bar/pyproject.toml") as f:
        #     toml = tomli.loads(f.read())
        #
        #     # No tool.uv.sources added without --use-editable-dagster
        #     assert "uv" not in toml["tool"]

        # Check cache was populated
        with pushd("code_locations/bar"):
            result = runner.invoke("component-type", "list", "--verbose")
            assert "CACHE [hit]" in result.output


def test_code_location_generate_outside_deployment_success(monkeypatch) -> None:
    # Remove when we are able to test without editable install
    dagster_git_repo_dir = discover_git_root(Path(__file__))
    monkeypatch.setenv("DAGSTER_GIT_REPO_DIR", str(dagster_git_repo_dir))

    with ProxyRunner.test() as runner, runner.isolated_filesystem():
        result = runner.invoke("code-location", "generate", "bar", "--use-editable-dagster")
        assert_runner_result(result)
        assert Path("bar").exists()
        assert Path("bar/bar").exists()
        assert Path("bar/bar/lib").exists()
        assert Path("bar/bar/components").exists()
        assert Path("bar/bar_tests").exists()
        assert Path("bar/pyproject.toml").exists()

        # Check venv created
        assert Path("bar/.venv").exists()
        assert Path("bar/uv.lock").exists()


@pytest.mark.parametrize("mode", ["env_var", "arg"])
def test_code_location_generate_editable_dagster_success(mode: str, monkeypatch) -> None:
    dagster_git_repo_dir = discover_git_root(Path(__file__))
    if mode == "env_var":
        monkeypatch.setenv("DAGSTER_GIT_REPO_DIR", str(dagster_git_repo_dir))
        editable_args = ["--use-editable-dagster", "--"]
    else:
        editable_args = ["--use-editable-dagster", str(dagster_git_repo_dir)]
    with ProxyRunner.test() as runner, isolated_example_deployment_foo(runner):
        result = runner.invoke("code-location", "generate", *editable_args, "bar")
        assert_runner_result(result)
        assert Path("code_locations/bar").exists()
        assert Path("code_locations/bar/pyproject.toml").exists()
        with open("code_locations/bar/pyproject.toml") as f:
            toml = tomli.loads(f.read())
            assert toml["tool"]["uv"]["sources"]["dagster"] == {
                "path": f"{dagster_git_repo_dir}/python_modules/dagster",
                "editable": True,
            }
            assert toml["tool"]["uv"]["sources"]["dagster-pipes"] == {
                "path": f"{dagster_git_repo_dir}/python_modules/dagster-pipes",
                "editable": True,
            }
            assert toml["tool"]["uv"]["sources"]["dagster-webserver"] == {
                "path": f"{dagster_git_repo_dir}/python_modules/dagster-webserver",
                "editable": True,
            }
            assert toml["tool"]["uv"]["sources"]["dagster-components"] == {
                "path": f"{dagster_git_repo_dir}/python_modules/libraries/dagster-components",
                "editable": True,
            }
            # Check for presence of one random package with no component to ensure we are
            # preemptively adding all packages
            assert toml["tool"]["uv"]["sources"]["dagstermill"] == {
                "path": f"{dagster_git_repo_dir}/python_modules/libraries/dagstermill",
                "editable": True,
            }


def test_code_location_generate_skip_venv_success() -> None:
    # Don't use the test component lib because it is not present in published dagster-components,
    # which this test is currently accessing since we are not doing an editable install.
    with ProxyRunner.test() as runner, runner.isolated_filesystem():
        result = runner.invoke("code-location", "generate", "--skip-venv", "bar")
        assert_runner_result(result)
        assert Path("bar").exists()
        assert Path("bar/bar").exists()
        assert Path("bar/bar/lib").exists()
        assert Path("bar/bar/components").exists()
        assert Path("bar/bar_tests").exists()
        assert Path("bar/pyproject.toml").exists()

        # Check venv created
        assert not Path("bar/.venv").exists()
        assert not Path("bar/uv.lock").exists()


def test_code_location_generate_editable_dagster_no_env_var_no_value_fails(monkeypatch) -> None:
    monkeypatch.setenv("DAGSTER_GIT_REPO_DIR", "")
    with ProxyRunner.test() as runner, isolated_example_deployment_foo(runner):
        result = runner.invoke("code-location", "generate", "--use-editable-dagster", "--", "bar")
        assert_runner_result(result, exit_0=False)
        assert "requires the `DAGSTER_GIT_REPO_DIR`" in result.output


def test_code_location_generate_already_exists_fails() -> None:
    with ProxyRunner.test() as runner, isolated_example_deployment_foo(runner):
        result = runner.invoke("code-location", "generate", "bar", "--skip-venv")
        assert_runner_result(result)
        result = runner.invoke("code-location", "generate", "bar", "--skip-venv")
        assert_runner_result(result, exit_0=False)
        assert "already exists" in result.output


# ########################
# ##### LIST
# ########################


def test_code_location_list_success():
    with ProxyRunner.test() as runner, isolated_example_deployment_foo(runner):
        runner.invoke("code-location", "generate", "foo")
        runner.invoke("code-location", "generate", "bar")
        result = runner.invoke("code-location", "list")
        assert_runner_result(result)
        assert (
            result.output.strip()
            == textwrap.dedent("""
                bar
                foo
            """).strip()
        )


def test_code_location_list_outside_deployment_fails() -> None:
    with ProxyRunner.test() as runner, runner.isolated_filesystem():
        result = runner.invoke("code-location", "list")
        assert_runner_result(result, exit_0=False)
        assert "must be run inside a Dagster deployment directory" in result.output
