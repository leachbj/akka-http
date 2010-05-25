/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package se.scalablesolutions.akka.rest

import se.scalablesolutions.akka.config.Config.config
import se.scalablesolutions.akka.actor.ActorModules
import com.sun.jersey.api.core.ResourceConfig
import com.sun.jersey.spi.container.servlet.ServletContainer
import com.sun.jersey.spi.container.WebApplication

/**
 * Akka's servlet to be used when deploying actors exposed as REST services in a standard servlet container,
 * e.g. not using the Akka Kernel.
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class AkkaServlet extends ServletContainer {
  import scala.collection.JavaConversions._

  override def initiate(resourceConfig: ResourceConfig, webApplication: WebApplication) = {
    resourceConfig.getProperties.put(
       "com.sun.jersey.config.property.packages",
       config.getList("akka.rest.resource_packages").mkString(";")
    )
    resourceConfig.getProperties.put(
      "com.sun.jersey.spi.container.ResourceFilters",
      config.getList("akka.rest.filters").mkString(","))

    val cl = Thread.currentThread.getContextClassLoader
    Thread.currentThread.setContextClassLoader(ActorModules.loader_?.getOrElse(cl))
    try {
      webApplication.initiate(resourceConfig)
    }
    finally{
      Thread.currentThread.setContextClassLoader(cl)
    }
  }
}
