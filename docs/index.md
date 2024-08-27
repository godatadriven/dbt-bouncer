<p align="center">
  <img src="https://github.com/godatadriven/dbt-bouncer/raw/main/docs/assets/logo.svg" alt="dbt-bouncer logo" width="500"/>
</p>

## Welcome to dbt-bouncer

`dbt-bouncer` is an open-source tool that allows you to configure and enforce conventions for your dbt project. The conventions are run against dbt's artifact files (think `./target/manifest.json`) resulting in speedy tests. Conventions can be specified in a `.yml` file, allowing maximum customisation to the conventions you wish to follow (or create 😀).

Check out our [`Getting Started`](./getting_started.md) guide.

## Terminology

- __Check__: A check is a rule run against a dbt artifact.
- __Config file__: A `.yml` file that specifies which checks to run along with any parameters.
- __dbt artifacts directory__: The directory that contains the dbt artifacts (`manifest.json`, etc.), generally this is `./target`.

## Aims

`dbt-bouncer` aims to:

- Provide a 100% configurable way to enforce conventions in a dbt project.
- Be as fast as possible, running checks against dbt artifacts.
- Be as easy as possible to use, with a simple config file written in `YML`.
- Be as flexible as possible, allowing checks to be written in python.
- Provide immediate feedback when run as part of a CI pipeline.

## About

`dbt-bouncer` is free software, released under the MIT license. It originated at Xebia Data in Amsterdam, Netherlands. Source code is available on [GitHub](https://github.com/godatadriven/dbt-bouncer).

All contributions, in the form of bug reports, pull requests, feedback or discussion are welcome. See the [contributing guide](./CONTRIBUTING.md) for more information.
