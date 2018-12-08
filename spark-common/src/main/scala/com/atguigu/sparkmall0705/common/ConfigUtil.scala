package com.atguigu.sparkmall0705.common

import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}

object ConfigUtil {


  def apply(propertiesName:String) = {
    val configurationUtil = new ConfigUtil()
    if (configurationUtil.config == null) {
      configurationUtil.config = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
        .configure(new Parameters().properties().setFileName(propertiesName)).getConfiguration
    }
    configurationUtil
  }

  def main(args: Array[String]): Unit = {
    val config: FileBasedConfiguration = ConfigUtil("config.properties").config

    println(config.getString("jdbc.user"))

  }
}


class ConfigUtil(){
  var config:FileBasedConfiguration=null

}

