# 安装

## 要求

### 开启MySQL的binlog

> 警告: 请确定服务器有足够的剩余空间开启BinLOG.
>
> 开启BinLOG之前的数据也会被导入，因为工具采用了两套模式同步数据，所以无需担心

`binlog_format` 和 `binlog_row_image` 的配置非常重要的，必须设置为 `ROW` 和 `FULL`:

```ini
log_bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
explicit_defaults_for_timestamp=true
log-slave-updates=1
; 在BinLog打开的时候需要设置一个ID，集群内唯一，1001之后的数字已经被本工具使用
; MUST set a server-id, the tool used the 1001+ for slave-id
server-id=1
```

### MySQL 用户

这个MySQL的用户必须拥有如下权限:

`SELECT, SHOW VIEW, Reload, REPLICATION SLAVE, REPLICATION CLIENT`

可以按照下面的语句创造用户:

```sql
CREATE USER 'canal'@'%' IDENTIFIED BY 'Your Password';
GRANT SELECT, SHOW VIEW, Reload, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;
```

## 安装Jre

Java 1.8+

CentOS/Redhat

```sh
yum install java java-devel
```

Fedora

```sh
dnf -y install \
    unzip \
    git \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel \
    ncurses-compat-libs
```


## 狭隘 & 安装

1. 到 [release](https://github.com/fly-studio/mysql-es/releases) 页面下载最新的稳定版本

    文件名类似: `mysql_es-x.x.x-release.zip`.

2. 解压.

3. 进入 `mysql_es` 文件夹.

4. 设置 `config/config.json`, `config/river.json`.

    可以查看 [配置](settings.md) 页面

## 运行

1. 给 `bin/me.sh` 文件添加 `X` 权限，比如: `chmod +x bin/me.sh`,

2. 启动 `bin/me.sh start`

3. 支持的指令有：

```sh
bin/me.sh start|stop|restart|info|status
```

## 卸载

直接删除 `mysql_es` 目录即可.