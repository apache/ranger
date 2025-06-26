/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.plugin.classloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerPluginClassLoader extends URLClassLoader {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPluginClassLoader.class);

    private static final String TAG_SERVICE_TYPE = "tag";

    private static final Map<String, RangerPluginClassLoader> PLUGIN_CLASS_LOADERS = new HashMap<>();

    private final ComponentClassLoader     componentClassLoader;
    private final ThreadLocal<ClassLoader> preActivateClassLoader = new ThreadLocal<>();

    public RangerPluginClassLoader(String pluginType, Class<?> pluginClass ) throws Exception {
        super(RangerPluginClassLoaderUtil.getInstance().getPluginFilesForServiceTypeAndPluginclass(pluginType, pluginClass), null);

        componentClassLoader = AccessController.doPrivileged((PrivilegedAction<ComponentClassLoader>) () -> new ComponentClassLoader(pluginClass));
    }

    public static RangerPluginClassLoader getInstance(final String pluginType, final Class<?> pluginClass ) throws Exception {
        RangerPluginClassLoader ret = PLUGIN_CLASS_LOADERS.get(pluginType);

        if (ret == null) {
            synchronized(RangerPluginClassLoader.class) {
                ret = PLUGIN_CLASS_LOADERS.get(pluginType);

                if (ret == null) {
                    if (pluginClass != null) {
                        ret = AccessController.doPrivileged((PrivilegedExceptionAction<RangerPluginClassLoader>) () -> new RangerPluginClassLoader(pluginType, pluginClass));
                    } else if (pluginType == null) { // let us pick an existing entry from pluginClassLoaders
                        if (!PLUGIN_CLASS_LOADERS.isEmpty()) {
                            // to be predictable, sort the keys
                            List<String> pluginTypes = new ArrayList<>(PLUGIN_CLASS_LOADERS.keySet());

                            Collections.sort(pluginTypes);

                            String pluginTypeToUse = pluginTypes.get(0);

                            ret = PLUGIN_CLASS_LOADERS.get(pluginTypeToUse);

                            LOG.info("RangerPluginClassLoader.getInstance(pluginType=null): using classLoader for pluginType={}", pluginTypeToUse);
                        }
                    }

                    if (ret != null) {
                        PLUGIN_CLASS_LOADERS.put(pluginType, ret);

                        if (pluginType != null && !pluginType.equals(TAG_SERVICE_TYPE)) {
                            PLUGIN_CLASS_LOADERS.put(TAG_SERVICE_TYPE, ret);
                        }
                    }
                }
            }
        }

        return ret;
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
        LOG.debug("==> RangerPluginClassLoader.findClass({})", name);

        Class<?> ret = null;

        try {
            // first we try to find a class inside the child classloader
            LOG.debug("RangerPluginClassLoader.findClass({}): calling childClassLoader().findClass() ", name);

            ret = super.findClass(name);
        } catch( Throwable e ) {
           // Use the Component ClassLoader findClass to load when childClassLoader fails to find
           LOG.debug("RangerPluginClassLoader.findClass({}): calling componentClassLoader.findClass()", name);

           ComponentClassLoader savedClassLoader = getComponentClassLoader();

           if (savedClassLoader != null) {
             ret = savedClassLoader.findClass(name);
           }
        }

        LOG.debug("<== RangerPluginClassLoader.findClass({}): {}", name, ret);

        return ret;
    }

    @Override
    public URL findResource(String name) {
        LOG.debug("==> RangerPluginClassLoader.findResource({}) ", name);

        URL ret =  super.findResource(name);

        if (ret == null) {
           LOG.debug("RangerPluginClassLoader.findResource({}): calling componentClassLoader.getResources()", name);

           ComponentClassLoader savedClassLoader = getComponentClassLoader();

           if (savedClassLoader != null) {
              ret = savedClassLoader.getResource(name);
            }
        }

        LOG.debug("<== RangerPluginClassLoader.findResource({}): {}", name, ret);

        return ret;
    }

    @Override
    public Enumeration<URL> findResources(String name) {
        final Enumeration<URL> ret;

        LOG.debug("==> RangerPluginClassLoader.findResources({}) ", name);

        ret = new MergeEnumeration(findResourcesUsingChildClassLoader(name),findResourcesUsingComponentClassLoader(name));

        LOG.debug("<== RangerPluginClassLoader.findResources({}) ", name);

        return ret;
    }

    @Override
    public synchronized Class<?> loadClass(String name) throws ClassNotFoundException {
        LOG.debug("==> RangerPluginClassLoader.loadClass({})", name);

        Class<?> ret = null;

        try {
            // first we try to load a class inside the child classloader
            LOG.debug("RangerPluginClassLoader.loadClass({}): calling childClassLoader.findClass()", name);

            ret = super.loadClass(name);
        } catch(Throwable e) {
            // Use the Component ClassLoader loadClass to load when childClassLoader fails to find
            LOG.debug("RangerPluginClassLoader.loadClass({}): calling componentClassLoader.loadClass()", name);

            ComponentClassLoader savedClassLoader = getComponentClassLoader();

            if(savedClassLoader != null) {
                ret = savedClassLoader.loadClass(name);
            }
        }

        LOG.debug("<== RangerPluginClassLoader.loadClass({}): {}", name, ret);

        return ret;
    }

    public Enumeration<URL> findResourcesUsingChildClassLoader(String name) {
        Enumeration<URL> ret = null;

        try {
            LOG.debug("RangerPluginClassLoader.findResourcesUsingChildClassLoader({}): calling childClassLoader.findResources()", name);

            ret =  super.findResources(name);
        } catch ( Throwable t) {
           //Ignore any exceptions. Null / Empty return is handle in following statements
           LOG.debug("RangerPluginClassLoader.findResourcesUsingChildClassLoader({}): class not found in child. Falling back to componentClassLoader", name, t);
        }

       return ret;
    }

    public Enumeration<URL> findResourcesUsingComponentClassLoader(String name) {
         Enumeration<URL> ret = null;

         try {
             LOG.debug("RangerPluginClassLoader.findResourcesUsingComponentClassLoader({}): calling componentClassLoader.getResources()", name);

             ComponentClassLoader savedClassLoader = getComponentClassLoader();

             if (savedClassLoader != null) {
                 ret = savedClassLoader.getResources(name);
             }

             LOG.debug("<== RangerPluginClassLoader.findResourcesUsingComponentClassLoader({}): {}", name, ret);
         } catch( Throwable t) {
             LOG.debug("RangerPluginClassLoader.findResourcesUsingComponentClassLoader({}): class not found in componentClassLoader.", name, t);
         }

         return ret;
     }

    public void activate() {
        LOG.debug("==> RangerPluginClassLoader.activate()");

        //componentClassLoader.set(new MyClassLoader(Thread.currentThread().getContextClassLoader()));

        preActivateClassLoader.set(Thread.currentThread().getContextClassLoader());

        Thread.currentThread().setContextClassLoader(this);

        LOG.debug("<== RangerPluginClassLoader.activate()");
    }

    public void deactivate() {
       LOG.debug("==> RangerPluginClassLoader.deactivate()");

       ClassLoader classLoader = preActivateClassLoader.get();

       if (classLoader != null) {
           preActivateClassLoader.remove();
       } else {
           ComponentClassLoader savedClassLoader = getComponentClassLoader();

           if (savedClassLoader != null && savedClassLoader.getParent() != null) {
               classLoader = savedClassLoader.getParent();
           }
       }

       if (classLoader != null) {
           Thread.currentThread().setContextClassLoader(classLoader);
       } else {
           LOG.warn("RangerPluginClassLoader.deactivate() was not successful. Couldn't get the saved classLoader...");
       }

       LOG.debug("<== RangerPluginClassLoader.deactivate()");
    }

    public ClassLoader getPrevActiveClassLoader() {
        ClassLoader ret = preActivateClassLoader.get();

        if (ret == null) {
            ComponentClassLoader savedClassLoader = getComponentClassLoader();

            if (savedClassLoader != null && savedClassLoader.getParent() != null) {
                ret = savedClassLoader.getParent();
            }
        }

        return ret;
    }

    private ComponentClassLoader getComponentClassLoader() {
        return componentClassLoader;
        //return componentClassLoader.get();
   }

   static class ComponentClassLoader extends ClassLoader {
        public ComponentClassLoader(Class<?> pluginShimClass) {
           super(getClassLoaderOfShimClassOrCurrentThread(pluginShimClass));
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException { //NO PMD
           return super.findClass(name);
        }

        private static ClassLoader getClassLoaderOfShimClassOrCurrentThread(Class<?> pluginShimClass) {
            return pluginShimClass != null ? pluginShimClass.getClassLoader() : Thread.currentThread().getContextClassLoader();
        }
    }

   static class MergeEnumeration implements Enumeration<URL> { //NO PMD
        final Enumeration<URL> e1;
        final Enumeration<URL> e2;

        public MergeEnumeration(Enumeration<URL> e1, Enumeration<URL> e2 ) {
            this.e1 = e1;
            this.e2 = e2;
        }

        @Override
        public boolean hasMoreElements() {
            return ( (e1 != null && e1.hasMoreElements() ) || ( e2 != null && e2.hasMoreElements()) );
        }

        @Override
        public URL nextElement() {
            final URL ret;

            if (e1 != null && e1.hasMoreElements()) {
                ret = e1.nextElement();
            } else if ( e2 != null && e2.hasMoreElements() ) {
                ret = e2.nextElement();
            } else {
                ret = null;
            }

            return ret;
        }
    }
}
