# MySQL - ElasticSearch Synchronization

A MySQL-ElasticSearch synchronization tool with **Real-Time**, **No-Lose**, **One-to-One Relation**.

base on [alibaba/canal](https://github.com/alibaba/canal), [RxJava](https://github.com/ReactiveX/RxJava).

> The Canal is a bin-log parser and subscriber of alibaba

## Version

- 1.0-beta : 2018-08-21

## Manuals

- [Install and launch](docs/install.md)
- [Settings](docs/settings.md)
- [Relation](docs/relation.md)
- [Errors](docs/error.md)
- [For developer](docs/developer.md)

## Requirements

- Java 1.8 +
- 4 GB Memory +
- 2 Core CPU +
- 100M Free space (for logs)

## Features

- supported ElasticSearch 5.x ~ 6.x.

- supported **Non-bin-log** MySQL **Before**.

    If MySQL did not enable the bin-log before, NO PROBLEM.

    See [How to work](#how-to-work).

- supported One-to-One relation.
  - Original tables

    - **users-table**:  | id | nickname | xxx |

    - **posts-table**:  | id | user_id | title | content |

  - Use a simple settings to synchronize them all, like:

        **posts-ES-index**:  | id | user_id | user.id | user.nickname | user.xxx | title | content |

    See [Relation](docs/relation.md).

- parsing the bin-log's records to synchronize in **REAL-TIME**, include Create / Update / Delete operations

- synchronize the relation records in **REAL-TIME**, Also after them modified.

- supported multiple primary keys.

- Backup bin-log position's file

## How to work

This tools contains two parts:

1. Dump the history data via "mysqldump"

    - Whether the MySQL enable the bin-log or not, before, enable it now.

    - Launch "mysqldump", dump the history data and synchronize them to Elastic.

    - Because "mysqldump" will returning a new bin-log position for saving.

    > 1. If exists a position file, it'll skiping the dumper.
    > 2. if MySQL do not enable the bin-log, "mysqldump" will not return a position.

2. Parse the Real-time bin-log data via Canal

    - Dump complete or exists a bin-log position file.

    - Launch the canal with the position.

    - Loop executing:

        1. parse and synchronize the records from bin-log in Real-Time.

        2. Save the newest bin-log position after synchronized

## Known issue

- Do not support the **No-Primary-key's table.

- If a table's primary key had be modified, like "id", cannot modify the old id to new id in Elastic.

- If a relation table's primary key had be modified, cannot modify the record to the related index in Elastic.

- If a column had be added / droped / modified, cannot synchronize.

- If the settings of tables or relations had be modified, cannot synchronize

## Todo

We will Support these features like:

- Synchronize when Alter table's column (`ADD` / `DROP` / `MODIFY`)
- Synchronize when the primary key modified
- Synchronize when the relation's primary key modified
- Synchronize the *Partial* columns that you want.
- Column *alias*

## Copyright and License

This tool is released under the **MIT License**

```text
Copyright (C) 2018 Fly-Studio

　　Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

　　The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```