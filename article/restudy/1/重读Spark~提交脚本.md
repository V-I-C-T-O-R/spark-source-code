重读Spark~提交脚本
---------------------------------------

工作起来，时间总是过的飞快！距离上次看Spark已小半年过去了，不经常使用真是会忘记。借着此次特殊情况，重新摸起来，好好了解下内部原理和实现。

大部分初学者学习Spark都是从搭建到编写WordCount程式来入门，鉴于目前的阶段，直接跳过从提交开始看。我们都知道，一般首次在Linux平台进行提交Spark任务时，都是在命令行输入类似`spark-submit --class com.example.spark.WordCount --master local[2]`的指令，但是具体从调用脚本到正式执行发生了什么却不甚明了。对于spark-submit脚本来说，内部调用的是${SPARK_HOME}/bin/spark-class脚本，并添加了具体的submit参数类

```
#!/usr/bin/env bash
if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi
# disable randomized hash for string in Python 3.3+
export PYTHONHASHSEED=0
exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"
```
执行spark-class脚本会首先加载spark需要的各种环境变量，例如$SPARK_CONF_DIR、$SPARK_SCALA_VERSION等，其实也是通过执行load-spark-env.sh脚本完成环境变量的变更。加载完基本环境配置之后，脚本会将Java执行环境位置和Spark项目需要的依赖jar包位置预先设定为执行变量，然后调用指令合成类来生成最终需要执行的指令。

```
#!/usr/bin/env bash
build_command() {
  "$RUNNER" -Xmx128m -cp "$LAUNCH_CLASSPATH" org.apache.spark.launcher.Main "$@"
  printf "%d\0" $?
}

# Turn off posix mode since it does not allow process substitution
set +o posix
CMD=()
while IFS= read -d '' -r ARG; do
  CMD+=("$ARG")
```
可以看到通过调用java -Xmx128m -cp "$LAUNCH_CLASSPATH" org.apache.spark.launcher.Main "$@"来唤org.apache.spark.launcher.Main类来做具体指令的拼接。还记得前面说的最开始加了具体执行类名么？没错，通过对比该类名来判断生成的具体方式
```
//判断类名
if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
      try {
        AbstractCommandBuilder builder = new SparkSubmitCommandBuilder(args);
        cmd = buildCommand(builder, env, printLaunchCommand);
      } catch (IllegalArgumentException e) {
            
      }
    } else {
      AbstractCommandBuilder builder = new SparkClassCommandBuilder(className, args);
      cmd = buildCommand(builder, env, printLaunchCommand);
    }

    if (isWindows()) {
      System.out.println(prepareWindowsCommand(cmd, env));
    } else {
      // In bash, use NULL as the arg separator since it cannot be used in an argument.
      List<String> bashCmd = prepareBashCommand(cmd, env);
      for (String c : bashCmd) {
        System.out.print(c);
        System.out.print('\0');
      }
    }
}
```
经过SparkSubmitCommandBuilder.buildCommand方法选择执行目的来找出buildSparkSubmitCommand方法生成最终的指令，最后返回指令。
```
private List<String> buildSparkSubmitCommand(Map<String, String> env)
      throws IOException, IllegalArgumentException {
    Map<String, String> config = getEffectiveConfig();
    boolean isClientMode = isClientMode(config);
    String extraClassPath = isClientMode ? config.get(SparkLauncher.DRIVER_EXTRA_CLASSPATH) : null;

    List<String> cmd = buildJavaCommand(extraClassPath);
    ......
  
    return cmd;
}
```
可以看到已本地提交方式最终执行的类是org.apache.spark.deploy.SparkSubmit，总体的调用流程如图：  
![1.jpg](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/article/restudy/1/pic/1.jpg)