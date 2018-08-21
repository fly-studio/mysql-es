# Install

## Requirements

### Enable your MySQL

> Warning: You must have enough free space for enabling bin-log

`binlog_format` AND `binlog_row_image` are very important，MUST be `ROW` and `FULL`:

```
log_bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
explicit_defaults_for_timestamp=true
log-slave-updates=1
; 在BinLog打开的时候需要设置一个ID，集群内唯一，1001之后的数字已经被本工具使用
server-id=1
```

### A MySQL user

The user MUST had these privileges: 

`SELECT, SHOW VIEW, Reload, REPLICATION SLAVE, REPLICATION CLIENT`

Create a user like this:

```
CREATE USER 'canal'@'%' IDENTIFIED BY 'Your Password';
GRANT SELECT, SHOW VIEW, Reload, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;
```
