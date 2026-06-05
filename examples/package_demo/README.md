# Package Demo

A minimalistic opinionated rivers project.

## Getting Started

Initialize `venv` and `sync` dependencies

```shell
uv venv
source .venv/bin/activate
uv sync
```

Start the local development server:

```shell
rivers dev
```

Open the server at [http://localhost:3000](http://localhost:3000).

## Reproduce from project from scratch

1. Create project directory

    ```shell
    mkdir package_demo
    cd package_demo
    ```

2. Pin the Python version

    ```shell
    uv python pin 3.13
    ```

3. Initialize project

    ```shell
    uv init --lib .
    ```

4. Add the rivers dependency

    ```shell
    uv add rivers
    ```

5. Add a `rivers.toml` configuration file to the root-level of the project with the following contents:

    ```toml
    [rivers]
    module = "package_demo.repository"
    repo_var = "repo"
    port=3000
    ```

6. Add the Python files according to the contents of the `src`-directory.

7. Run the local development server:

    ```shell
    rivers dev
    ```
