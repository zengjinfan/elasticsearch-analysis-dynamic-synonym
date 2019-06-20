package com.plugins.elasticsearch.plugin.synonym.service;

import org.elasticsearch.index.analysis.AnalysisRegistry;

/**
 * 插件生命周期内的组件
 */
public class DynamicSynonymComponent {

    private AnalysisRegistry analysisRegistry;

    public AnalysisRegistry getAnalysisRegistry() {
        return analysisRegistry;
    }

    /**
     * 该组件被传递给生命周期内的bean初始化时调用，保存同义词初始化需要用到的analysisRegistry
     *
     * @param analysisRegistry
     */
    public void setAnalysisRegistry(AnalysisRegistry analysisRegistry) {
        this.analysisRegistry = analysisRegistry;
    }

}
