# Releasing WaveLinkAdapter

Releases are built and published by `.github/workflows/publish.yml`. The
workflow uses PyPI Trusted Publishing, so no long-lived PyPI token is stored in
GitHub.

## One-time PyPI setup

Create the `wavelink-adapter` project or a pending Trusted Publisher on PyPI
with these values:

- PyPI project name: `wavelink-adapter`
- GitHub owner: `Nekit678`
- GitHub repository: `WaveLinkAdapter`
- Workflow filename: `publish.yml`
- Environment name: `pypi`

Create a GitHub environment named `pypi` as well. Adding required reviewers to
that environment is recommended for release approval.

## Publishing a version

1. Update `project.version` in `pyproject.toml` using a PEP 440 version.
2. Run `python -m unittest discover -v`, `ruff check .`, `python -m build`, and
   `python -m twine check dist/*`.
3. Commit and push the release changes.
4. Create a GitHub release whose tag is exactly `v` followed by the package
   version, for example `v0.1.0`.

Publishing the GitHub release triggers the workflow. It verifies that the tag
matches `project.version`, reruns the tests, builds both the wheel and source
distribution, and publishes them to PyPI.

