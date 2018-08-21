# MySQL - ElasticSearch Synchronization

A No-Lose data's synchronization tool, Supported **One-to-One Relation**, Read and parse bin-log to sync, base on [alibaba/canal](https://github.com/alibaba/canal), [RxJava](https://github.com/ReactiveX/RxJava).

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

- supported No-bin-log MySQL Before.

    If your MySQL did not enable the bin-log before, NO PROBLEM.

    Enable it now, And this tool will sync the history via "mysqldump"

    See [How to work](#how-to-work).

- supported One-to-One relation.

    - **users-table**:  | id | nickname | xxx |

    - **posts-table**:  | id | user_id | title | content |

        this tool can add the "users-table"'s related record to the "posts-ES-index"'s, like this:

    - **posts-ES-index**:  | id | user_id | user.id | user.nickname | user.xxx | title | content |

        When "users-table"'s record were modified,  "posts-ES-index" will synchronizing the related record.

    See [Relation](docs/relation.md).

- use bin-log to synchronize the record in **REAL-TIME**

    Base on cannal, but you do not need via other canal setting, this tool set it automation.

- supported synchronize the Create / Update / Delete operations in MySQL in **REAL-TIME**

- supported multiple primary keys.

- synchronize the partial columns If you want.

## How to work

This tools contains two parts:

1. Dump the history data via "mysqldump"

    - Whether the MySQL enable the bin-log or not, before, enable it now.

    - This tool will "mysqldump" the history data and synchronize them to Elastic.

    - Because "mysqldump" will returning a new bin-log position for saving.

    > 1. If exists a position file, it'll skiping the dumper.
    > 2. if MySQL do not enable the bin-log, it'll not return a position.

2. Parse the Real-time bin-log data via Canal

    - Dump complete or exists a bin-log position file.

    - Tool launch the canal with the position.

    - Loop executing:

        1. Then parse and synchronize the data from bin-log in Real-Time.

        2. Save the newest bin-log position after synchronized




## Known issue

- Do not support the **No-Primary-key's table.

- If a table's primary key had be modified, like "id", cannot modify the old id to new id in Elastic.

- If a relation table's primary key had be modified, cannot modify the record to the related index in Elastic.

- If a column had be added / droped / modified, cannot synchronize.

- If you modify the settings of tables or relations, cannot synchronize

## Todo

We will Support these features like:

- Alter Table's column (ADD / DROP / MODIFY)
- Modify the primary key
- Modify the relation's primary key
- Column alias

## Copyright and License

This tool is released under the **MIT License**

```text
Copyright (C) 2018 Fly-Studio

　　Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

　　The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```