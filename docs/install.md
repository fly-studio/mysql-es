# Install

## Requirements

### Enable MySQL binlog

> Warning: Computer must have enough free space for enabling bin-log.
>
> Don't worry about your historical data, this tool could sync these via other way.

`binlog_format` AND `binlog_row_image` are very important，MUST be `ROW` and `FULL`:

```ini
log_bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
explicit_defaults_for_timestamp=true
log-slave-updates=1
; MUST set a server-id, this tool used the 1001+ for slave-id
server-id=1
```

### MySQL user privileges

The user MUST had these privileges:

`SELECT, SHOW VIEW, Reload, REPLICATION SLAVE, REPLICATION CLIENT`

Create a user like this:

```sql
CREATE USER 'canal'@'%' IDENTIFIED BY 'Your Password';
GRANT SELECT, SHOW VIEW, Reload, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;
```

## Jre

Java 1.8+

CentOS/Redhat

```sh
yum install java java-devel
```

Fedora

```sh
dnf -y install \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel
```


## Download & Install

1. Goto [release](https://github.com/fly-studio/mysql-es/releases) page, and download the newest stable version.

    filename like: `mysql_es-x.x.x-release.zip`.

2. Unzip it.

3. Goto `mysql_es` folder.

4. Set `config/config.json`, `config/river.json`.

    See [Settings](settings.md) page

## Run

1. Add `X` permission to file: `chmod +x bin/me.sh`,

2. `bin/me.sh start`

3. all usages:

```sh
bin/me.sh start|stop|restart|info|status
```

## Uninstall

Delete the `mysql_es` folder.