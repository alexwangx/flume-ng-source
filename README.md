flume-ng-source
================

Source of Flume NG for tailing files in a directory

Configuration
=============
| Property Name | Default   | Description         |
| ------------- | :-----:   | :----------         |
| Channels      |    -      |                     |
| Type          |    -      |                     |
| monitorPath   |    -      | Monitoring directory path   |
| fileEncode    | UTF-8     | Monitoring file encoding    |
| fileRegex     |   .*      | Regular expression specifying which files to slected,Note:Escape character |
| batchSize     |   20      | The max number of lines to read and send to the channel at a time |
| startFromEnd  |   true    | If startFromEnd is set true, begin reading the file at the end|

* Example
```
agent.sources = tailDir
agent.sources.tailDir.type = com.source.tailDir.TailDirSourceNG
agent.sources.tailDir.monitorPath = /Users/alex/Downloads/testDir/test

```
