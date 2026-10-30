# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/godatadriven/dbt-bouncer/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                        |    Stmts |     Miss |   Branch |   BrPart |    Cover |   Missing |
|------------------------------------------------------------ | -------: | -------: | -------: | -------: | -------: | --------: |
| src/dbt\_bouncer/\_\_init\_\_.py                            |       13 |        0 |        4 |        0 |     100% |           |
| src/dbt\_bouncer/artifact\_parsers/info\_schema.py          |      107 |        0 |       24 |        0 |     100% |           |
| src/dbt\_bouncer/artifact\_parsers/parser.py                |      178 |        0 |       58 |        0 |     100% |           |
| src/dbt\_bouncer/artifact\_types.py                         |      195 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/check\_framework/\_\_init\_\_.py           |        5 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/check\_framework/base.py                   |       38 |        0 |        2 |        0 |     100% |           |
| src/dbt\_bouncer/check\_framework/context.py                |       59 |        0 |       16 |        0 |     100% |           |
| src/dbt\_bouncer/check\_framework/decorator.py              |       74 |        0 |       26 |        0 |     100% |           |
| src/dbt\_bouncer/check\_framework/exceptions.py             |        8 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/checks/catalog/check\_catalog\_seeds.py    |       49 |        0 |       30 |        0 |     100% |           |
| src/dbt\_bouncer/checks/catalog/check\_catalog\_sources.py  |       16 |        0 |        8 |        0 |     100% |           |
| src/dbt\_bouncer/checks/catalog/columns/\_\_init\_\_.py     |        0 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/checks/catalog/columns/description.py      |       57 |        0 |       34 |        0 |     100% |           |
| src/dbt\_bouncer/checks/catalog/columns/naming.py           |       36 |        0 |       20 |        0 |     100% |           |
| src/dbt\_bouncer/checks/catalog/columns/tests.py            |       24 |        0 |       12 |        0 |     100% |           |
| src/dbt\_bouncer/checks/info\_schema/columns.py             |      116 |        0 |       62 |        0 |     100% |           |
| src/dbt\_bouncer/checks/info\_schema/grain.py               |       16 |        0 |        6 |        0 |     100% |           |
| src/dbt\_bouncer/checks/info\_schema/query.py               |       12 |        0 |        2 |        0 |     100% |           |
| src/dbt\_bouncer/checks/info\_schema/sources.py             |       11 |        0 |        4 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/check\_exposures.py        |       50 |        0 |       24 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/check\_lineage.py          |       20 |        0 |        6 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/check\_macros.py           |      138 |        0 |       72 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/check\_metadata.py         |       10 |        0 |        2 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/check\_seeds.py            |       40 |        0 |       12 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/check\_semantic\_models.py |       14 |        0 |        8 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/check\_snapshots.py        |       50 |        0 |       24 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/check\_tests.py            |       30 |        0 |       16 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/check\_unit\_tests.py      |       31 |        0 |       18 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/\_\_init\_\_.py     |        0 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/access.py           |       48 |        0 |       20 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/code.py             |      137 |        0 |       70 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/columns.py          |      152 |        0 |       76 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/description.py      |       56 |        0 |       26 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/directories.py      |       67 |        0 |       34 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/lineage.py          |      106 |        0 |       46 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/meta.py             |       17 |        0 |        4 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/naming.py           |       21 |        0 |        8 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/tags.py             |       17 |        0 |       10 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/tests.py            |       61 |        0 |       32 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/models/versioning.py       |       29 |        0 |       14 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/sources/\_\_init\_\_.py    |        0 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/sources/description.py     |       15 |        0 |        4 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/sources/directories.py     |       27 |        0 |       10 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/sources/freshness.py       |       11 |        0 |        4 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/sources/lineage.py         |       36 |        0 |       10 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/sources/loader.py          |        5 |        0 |        2 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/sources/meta.py            |       27 |        0 |        6 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/sources/naming.py          |        9 |        0 |        2 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/sources/tags.py            |       16 |        0 |       10 |        0 |     100% |           |
| src/dbt\_bouncer/checks/manifest/sources/tests.py           |        8 |        0 |        2 |        0 |     100% |           |
| src/dbt\_bouncer/checks/run\_results/check\_run\_results.py |       18 |        0 |        6 |        0 |     100% |           |
| src/dbt\_bouncer/cli/\_\_init\_\_.py                        |        3 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/cli/baseline/\_\_init\_\_.py               |       26 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/cli/baseline/utils.py                      |       29 |        0 |        2 |        0 |     100% |           |
| src/dbt\_bouncer/cli/explain/\_\_init\_\_.py                |       25 |        0 |        6 |        0 |     100% |           |
| src/dbt\_bouncer/cli/explain/utils.py                       |       57 |        0 |       18 |        0 |     100% |           |
| src/dbt\_bouncer/cli/init/\_\_init\_\_.py                   |       42 |        0 |        8 |        0 |     100% |           |
| src/dbt\_bouncer/cli/init/utils.py                          |       34 |        0 |        6 |        0 |     100% |           |
| src/dbt\_bouncer/cli/list/\_\_init\_\_.py                   |       18 |        0 |        4 |        0 |     100% |           |
| src/dbt\_bouncer/cli/list/utils.py                          |       44 |        0 |       12 |        0 |     100% |           |
| src/dbt\_bouncer/cli/mcp/\_\_init\_\_.py                    |       12 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/cli/mcp/server.py                          |       86 |        0 |       20 |        0 |     100% |           |
| src/dbt\_bouncer/cli/run/\_\_init\_\_.py                    |       20 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/cli/run/utils.py                           |      140 |        0 |       48 |        0 |     100% |           |
| src/dbt\_bouncer/cli/studio/\_\_init\_\_.py                 |       31 |        0 |       10 |        0 |     100% |           |
| src/dbt\_bouncer/cli/studio/utils.py                        |      143 |        0 |       68 |        0 |     100% |           |
| src/dbt\_bouncer/cli/utils.py                               |        5 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/cli/validate/\_\_init\_\_.py               |       28 |        0 |        8 |        0 |     100% |           |
| src/dbt\_bouncer/configuration\_file/\_\_init\_\_.py        |        0 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/configuration\_file/parser.py              |       57 |        0 |       10 |        0 |     100% |           |
| src/dbt\_bouncer/configuration\_file/validator.py           |      464 |        0 |      196 |        0 |     100% |           |
| src/dbt\_bouncer/context.py                                 |       62 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/enums.py                                   |      230 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/exceptions.py                              |        6 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/executor.py                                |       53 |        0 |       16 |        0 |     100% |           |
| src/dbt\_bouncer/main.py                                    |       26 |        0 |        4 |        0 |     100% |           |
| src/dbt\_bouncer/presets/\_\_init\_\_.py                    |       14 |        0 |        2 |        0 |     100% |           |
| src/dbt\_bouncer/regression.py                              |       40 |        0 |       10 |        0 |     100% |           |
| src/dbt\_bouncer/reporting/\_\_init\_\_.py                  |        0 |        0 |        0 |        0 |     100% |           |
| src/dbt\_bouncer/reporting/formatters.py                    |       95 |        0 |       40 |        0 |     100% |           |
| src/dbt\_bouncer/reporting/logger.py                        |       39 |        0 |        6 |        0 |     100% |           |
| src/dbt\_bouncer/reporting/reporter.py                      |      103 |        0 |       32 |        0 |     100% |           |
| src/dbt\_bouncer/runner.py                                  |      172 |        0 |       56 |        0 |     100% |           |
| src/dbt\_bouncer/selectors.py                               |      141 |        0 |       66 |        0 |     100% |           |
| src/dbt\_bouncer/sql\_utils.py                              |       51 |        0 |       18 |        0 |     100% |           |
| src/dbt\_bouncer/testing.py                                 |       92 |        0 |       26 |        0 |     100% |           |
| src/dbt\_bouncer/types.py                                   |       19 |        0 |        4 |        0 |     100% |           |
| src/dbt\_bouncer/utils.py                                   |      368 |        0 |      142 |        0 |     100% |           |
| src/dbt\_bouncer/version.py                                 |        2 |        0 |        0 |        0 |     100% |           |
| **TOTAL**                                                   | **5057** |    **0** | **1724** |    **0** | **100%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/godatadriven/dbt-bouncer/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/godatadriven/dbt-bouncer/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/godatadriven/dbt-bouncer/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/godatadriven/dbt-bouncer/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Fgodatadriven%2Fdbt-bouncer%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/godatadriven/dbt-bouncer/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.