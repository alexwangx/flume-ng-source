flume-ng-source
================

Source of Flume NG

Configuration -- tailDir
=============
| Property Name | Default   | Description         |
| ------------- | :-----:   | :----------         |
| Channels      |    -      |                     |
| Type          |    -      |                     |
| monitorPath   |    -      | Monitoring directory path   |
| fileEncode    |   UTF-8   | Monitoring file encoding    |
| fileRegex     |   .*      | Regular expression specifying which files to slected,Note:Escape character |
| batchSize     |   100     | The max number of lines to read and send to the channel at a time |
| startFromEnd  |   true    | If startFromEnd is set true, begin reading the file at the end|
| delimRegex    |   null    | delimiter regex|
| delimMode     |   null    | delimMode="exclude;prev;next" |

* Example
```
agent.sources = tailDir
agent.sources.tailDir.type = com.source.tailDir.TailDirSourceNG
agent.sources.tailDir.monitorPath = /Users/alex/Downloads/testDir/test
```

Configuration -- tailFile
=============
    tail a file on unix(HP-UX,AIX...) platform, replacing tail -F command
| Property Name | Default   | Description         |
| ------------- | :-----:   | :----------         |
| Channels      |    -      |                     |
| Type          |    -      |                     |
| monitorFile   |    -      | Monitoring file path        |
| fileEncode    |  UTF-8    | Monitoring file encoding    |
| batchSize     |   100     | The max number of lines to read and send to the channel at a time |
| startFromEnd  |   true    | If startFromEnd is set true, begin reading the file at the end|
| delimRegex    |   null    | delimiter regex|
| delimMode     |   null    | delimMode="exclude;prev;next" |

* Example
```
#tail a file on unix(HP-UX,AIX...) platform, replacing tail -F command
agent.sources = tailFile
agent.sources.tailDir.type = com.source.tailDir.TailSourceNG
agent.sources.tailDir.monitorFile = /Users/alex/Downloads/testFile/test.txt
```