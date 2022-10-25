# 开发指南


在此文档中，你可以找到如何高效地在BitSail中进行开发。

下面是本文档包含地内容:
- [Check style](#jump_check_style)

-----

## Check Style

我们为开发者定义了一份checkstyle配置 [tools/maven/checkstyle.xml](https://github.com/bytedance/bitsail/blob/master/tools/maven/checkstyle.xml) 。
在IDE中进行开发时，可将上述checkstyle配置文件导入项目:

![](images/set_checkstyle.png)

导入完成后，运行指令 `mvn checkstyle:check` 即可检查是否满足checkstyle.

