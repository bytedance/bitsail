# Developer Guide

In this document, you can find how to effectively develop a connector in BitSail.

Here are the contents:

 - [Check style](#jump_check_style)

----

## Check Style

We define check style in [tools/maven/checkstyle.xml](https://github.com/bytedance/bitsail/blob/master/tools/maven/checkstyle.xml).
When developing in IDE, you can import this checkstyle configuration file.


![](images/set_checkstyle.png)

After importing, run command `mvn checkstyle:check` to see if checkstyle satisfied.
