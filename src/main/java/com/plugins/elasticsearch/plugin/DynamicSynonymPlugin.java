/**
 *
 */
package com.plugins.elasticsearch.plugin;

import com.plugins.elasticsearch.plugin.synonym.analysis.DynamicSynonymTokenFilterFactory;
import com.plugins.elasticsearch.plugin.synonym.service.DynamicSynonymAnalysisService;
import com.plugins.elasticsearch.plugin.synonym.service.DynamicSynonymComponent;
import com.plugins.elasticsearch.plugin.synonym.service.DynamicSynonymIndexEventListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;


public class DynamicSynonymPlugin extends Plugin implements AnalysisPlugin {
    private static Logger logger = LogManager.getLogger("dynamic-synonym");
    private DynamicSynonymComponent pluginComponent = new DynamicSynonymComponent();

    @Override
    public Collection<Object> createComponents(Client client,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService,
                                               ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry,
                                               Environment environment,
                                               NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry) {
        Collection<Object> components = new ArrayList<>();
        components.add(pluginComponent);
        return components;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return singletonList(DynamicSynonymAnalysisService.class);
    }

    //添加lister
    @Override
    public void onIndexModule(IndexModule indexModule) {
        indexModule.addIndexEventListener(new DynamicSynonymIndexEventListener());
    }

    //4.释放资源
    @Override
    public void close() {
        logger.info("DynamicSynonymPlugin close...");
        DynamicSynonymTokenFilterFactory.closeDynamicSynonym();
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.TokenFilterFactory>> extra = new HashMap<>();

        extra.put("dynamic_synonym", new AnalysisModule.AnalysisProvider<TokenFilterFactory>() {

            @Override
            public TokenFilterFactory get(IndexSettings indexSettings, Environment environment, String name, Settings settings)
                    throws IOException {
                logger.info("########## Index {} get TokenFilterFactory,  name = {}, settings = {}", indexSettings.getIndex().getName(),
                        name, settings.toDelimitedString(','));
                return new DynamicSynonymTokenFilterFactory(indexSettings, environment, name, settings, pluginComponent.getAnalysisRegistry());
            }

            @Override
            public boolean requiresAnalysisSettings() {
                return true;
            }
        });
        return extra;
    }

}