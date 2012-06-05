/*
 * Copyright (c) 2010-2012 Engine Yard, Inc.
 * Copyright (c) 2007-2009 Sun Microsystems, Inc.
 * This source code is available under the MIT license.
 * See the file LICENSE.txt for details.
 */
package org.jruby.rack;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A pooling application factory that creates runtimes and manages a fixed- or
 * unlimited-size pool.
 * <p>
 * It has several context init parameters to control behavior:
 * <ul>
 * <li> jruby.min.runtimes: Initial number of runtimes to create and put in
 *  the pool. Default is none.
 * <li> jruby.max.runtimes: Maximum size of the pool. Default is unlimited, in
 *  which case new requests for an application object will create one if none
 *  are available.
 * <li> jruby.runtime.timeout.sec: Value (in seconds) indicating when
 *  a thread will timeout when no runtimes are available. Default is 30.
 * <li> jruby.runtime.initializer.threads: Number of threads to use at startup to
 *  intialize and fill the pool. Default is 4.
 * </ul>
 *
 * @author nicksieger
 */
public class PoolingRackApplicationFactory implements RackApplicationFactory {
    
    protected RackContext rackContext;
    private final RackApplicationFactory realFactory;
    protected final Queue<RackApplication> applicationPool = new LinkedList<RackApplication>();
    private Integer initial;

    public PoolingRackApplicationFactory(RackApplicationFactory factory) {
        realFactory = factory;
    }

    public RackApplicationFactory getRealFactory() {
        return realFactory;
    }
    
    public void init(final RackContext rackContext) throws RackInitializationException {
        this.rackContext = rackContext;
        realFactory.init(rackContext);

        RackConfig config = rackContext.getConfig();

        initial = config.getInitialRuntimes();
        if (initial != null) {
            fillInitialPool();
        }
    }

    public RackApplication newApplication() throws RackInitializationException {
        return getApplication();
    }

    public RackApplication getApplication() throws RackInitializationException {
        synchronized (applicationPool) {
            if (!applicationPool.isEmpty()) {
                rackContext.log("Info: Getting app from pool, "+(applicationPool.size() - 1) + " left.");
                return applicationPool.remove();
            } else {
                rackContext.log("Warn: Pool empty, punting.");
                throw new RackInitializationException("timeout: all listeners busy", new InterruptedException());
            }
        }
    }

    public void finishedWithApplication(RackApplication app) {
        if (app == null) {
            // seems to sometimes happen when an error occurs during boot
            // and thus on destroy app.destroy(); will fail with a NPE !
            rackContext.log("Warn: ignoring null application");
            return;
        }
        synchronized (applicationPool) {
            if (applicationPool.contains(app)) {
                return;
            }
            applicationPool.add(app);
        }
    }

    public RackApplication getErrorApplication() {
        return realFactory.getErrorApplication();
    }

    public void destroy() {
        synchronized (applicationPool) {
            for (RackApplication app : applicationPool) {
                app.destroy();
            }
        }
        realFactory.destroy();
    }
    
    /** Used only by unit tests */
    public Collection<RackApplication> getApplicationPool() {
        return Collections.unmodifiableCollection(applicationPool);
    }

    /** This creates the application objects in the foreground thread to avoid
     * leakage when the web application is undeployed from the application server. */
    protected void fillInitialPool() throws RackInitializationException {
        Queue<RackApplication> apps = createApplications();
        launchInitializerThreads(apps);
    }

    protected void launchInitializerThreads(final Queue<RackApplication> apps) {
        Integer numThreads = rackContext.getConfig().getNumInitializerThreads();
        if (numThreads == null) {
            numThreads = 4;
        }

        for (int i = 0; i < numThreads; i++) {
            new Thread(new Runnable() {

                public void run() {
                    try {
                        while (true) {
                            RackApplication app = null;
                            synchronized (apps) {
                                if (apps.isEmpty()) {
                                    break;
                                }
                                app = apps.remove();
                            }
                            app.init();
                            synchronized (applicationPool) {
                                applicationPool.add(app);
                                rackContext.log("Info: add application to the pool. size now = " + applicationPool.size());
                            }
                        }
                    } catch (RackInitializationException ex) {
                        rackContext.log("Error: unable to initialize application", ex);
                    }
                }
            }, "JRuby-Rack-App-Init-" + i).start();
        }
    }

    protected Queue<RackApplication> createApplications() throws RackInitializationException {
        Queue<RackApplication> apps = new LinkedList<RackApplication>();
        for (int i = 0; i < initial; i++) {
            apps.add(realFactory.newApplication());
        }
        return apps;
    }
}
