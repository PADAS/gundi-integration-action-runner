"""The pytest11 entry point contract: a project that installs
gundi-action-runner gets the testing fixtures with no conftest wiring."""


def test_fixtures_available_without_conftest(pytester):
    pytester.makepyfile(
        """
        def test_uses_plugin_fixture(integration_v2):
            # integration_v2 comes from gundi_action_runner.testing.fixtures,
            # loaded via the pytest11 entry point — this file has no imports
            # and the temp project has no conftest.
            assert str(integration_v2.id)
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(passed=1)


def test_plugin_is_registered_by_entry_point(pytester):
    result = pytester.runpytest("--trace-config")
    result.stdout.fnmatch_lines(["*gundi_action_runner.testing.fixtures*"])
