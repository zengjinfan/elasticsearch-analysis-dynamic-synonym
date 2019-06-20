package com.plugins.elasticsearch.plugin.synonym.service;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisRegistry;

/**
 * 插件生命周期内的服务,通过@Inject注解被es初始化，传递需要的Component进来
 */
public class DynamicSynonymAnalysisService extends AbstractLifecycleComponent {
    public static Logger logger = ESLoggerFactory.getLogger("dynamic-synonym");

    @Inject
    public DynamicSynonymAnalysisService(final Settings settings, final AnalysisRegistry analysisRegistry,
                                         final DynamicSynonymComponent pluginComponent) {
        super(settings);
        pluginComponent.setAnalysisRegistry(analysisRegistry);
    }

    @Override
    protected void doStart() {
        logger.info("############### DynamicSynonymAnalysisService doStart");
    }

    @Override
    protected void doStop() {
        logger.info("############### DynamicSynonymAnalysisService doStop");
    }

    @Override
    protected void doClose() {
        logger.info("############### DynamicSynonymAnalysisService doClose");
    }

}
