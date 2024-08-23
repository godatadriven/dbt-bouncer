<p align="center">
  <img src="https://github.com/godatadriven/dbt-bouncer/raw/main/images/logo.webp" alt="dbt-bouncer logo" width="500"/>
</p>

## Welcome to dbt-bouncer

`dbt-bouncer` is an open-source tool that allows you to configure and enforce conventions for your dbt project. The conventions are run against dbt's artifact files (think `./target/manifest.json`) resulting in speedy tests. Conventions can be specified in a `.yml` file, allowing maximum customisation to the conventions you wish to follow (or create 😀).

Check out our [`Getting Started`](./getting_started.md) guide.

## Terminology

- __Check__: A check is a rule run against a dbt artifact.
- __Config file__: A `.yml` file that specifies which checks to run along with any parameters.
- __dbt artifacts directory__: The directory that contains the dbt artifacts (`manifest.json`, etc.), generally this is `./target`.

## About

`dbt-bouncer` is free software, released under the MIT license. It originated at Xebia Data in Amsterdam, Netherlands. Source code is available on Github [here](https://github.com/godatadriven/dbt-bouncer).

All contributions, in the form of bug reports, pull requests, feedback or discussion are welcome. See the [contributing guide](./CONTRIBUTING.md) for more information.
