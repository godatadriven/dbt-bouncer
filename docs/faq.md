<details>

    <summary>How to set up `dbt-bouncer` in a monorepo?</summary>

    A monorepo may consist of one directory with a dbt project and other directories with unrelated code. It may be desired for `dbt-bouncer` to be configured from the root directory. Sample directory tree:

    ```shell
    .
    ├── dbt-bouncer.yml
    ├── README.md
    ├── dbt-project
    │   ├── models
    │   ├── dbt_project.yml
    │   └── profiles.yml
    └── package-a
        ├── src
        ├── tests
        └── package.json
    ```

    To ease configuration you can use `exclude` or `include` at the global level (see [Config File](./config-file.md) for more details). For the above example `dbt-bouncer.yml` could be configured as:

    ```yaml
    dbt_artifacts_dir: dbt-project/target
    include: ^dbt-project

    manifest_checks:
        - name: check_exposure_based_on_non_public_models
    ```

    `dbt-bouncer` can now be run from the root directory.

</details>
