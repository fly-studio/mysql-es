# MySQL - ElasticSearch Synchronization

一个**实时**、**无数据遗失**、**支持一对一关系**的Mysql -> ElasticSearch 同步工具。

基于[alibaba/canal](https://github.com/alibaba/canal), [RxJava](https://github.com/ReactiveX/RxJava)

## 版本

- 1.0-beta : 2018-09-04

## 手册

- [安装、启动](docs/zh-install.md)
- [配置](docs/zh-settings.md)
- [一对一关系导入](docs/zh-relation.md)
- [错误](docs/zh-error.md)
- [二次开发](docs/zh-developer.md)

## 最低配置

- Java 1.8 +
- 2 GB 内存 +
- 2 核 CPU +
- 100M 剩余空间

## 特性

- 支持Elastic Search 5.x ~ 6.x