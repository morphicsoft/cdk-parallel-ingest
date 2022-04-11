import pathlib
from invoke import task, run

COMPONENTS = {
    "ingestion",
}


def resolve_components(components):
    result = COMPONENTS & set(components or COMPONENTS)
    if not result:
        print(f"No components matched from {components}")
    for r in result:
        print(f"{r}...")
        yield r
        print(f"...{r} done.")


@task(iterable=["components"])
def update(c, components=None):
    for component in resolve_components(components):
        c.run(f"pip-compile-multi -d {component}/requirements")


@task()
def sync(c):
    requirements_files = [
        str(r)
        for component in COMPONENTS
        for r in pathlib.Path(f"{component}/requirements").rglob("*.txt")
    ]
    c.run(f"pip-sync {' '.join(requirements_files)}")


@task(iterable=["components"])
def package(c, components=None):
    for component in resolve_components(components):
        c.run("mkdir -p dist")
        c.run(f"rm -rf dist/{component}")
        c.run(f"rsync -r {component}/source/* dist/{component}/")
        c.run(f"pip install -r {component}/requirements/app.txt -t dist/{component}")


@task(aliases=["check"])
def static_analysis(c):
    pass  # TODO


@task
def test(c):
    pass  # TODO


@task
def cdk_check(c):
    run("cdk doctor")
    # TODO
    # c.run("cfn_nag_scan -i stack/.stack.yaml")


@task
def clean(c):
    run("rm -rf dist")
