package com.plugins.elasticsearch.plugin.synonym.analysis;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class DynamicSynonymTokenFilterFactory extends
        AbstractTokenFilterFactory {

    /**
     * Static id generator
     */
    private static final AtomicInteger id = new AtomicInteger(1);
    private static Logger logger = LogManager.getLogger("dynamic-synonym");
    private static ScheduledExecutorService pool = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setName("monitor-synonym-Thread-" + id.getAndAdd(1));
        return thread;
    });

    //配置属性
    protected final String indexName;
    protected final String location;
    protected final boolean ignoreCase;
    protected final boolean expand;
    protected final String format;
    protected final int interval;
    protected SynonymMap synonymMap;

//    private Map<DynamicSynonymFilter, Integer> dynamicSynonymFilters = new WeakHashMap<>();

    /**
     * 每个过滤器实例产生的资源-index级别
     */
    protected static ConcurrentHashMap<String, CopyOnWriteArrayList<DynamicSynonymFilter>> dynamicSynonymFilters = new ConcurrentHashMap();
    protected static ConcurrentHashMap<String, CopyOnWriteArrayList<ScheduledFuture>> scheduledFutures = new ConcurrentHashMap();
    /**
     * load调度器-node级别
     */
    private static ScheduledExecutorService monitorPool = Executors.newScheduledThreadPool(1,
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("monitor-synonym-Thread");
                    return thread;
                }
            });

    public DynamicSynonymTokenFilterFactory(
            IndexSettings indexSettings,
            Environment env,
            String name,
            Settings settings,
            AnalysisRegistry analysisRegistry
    ) throws IOException {

        super(indexSettings, name, settings);

        this.indexName = indexSettings.getIndex().getName();
        this.location = settings.get("synonyms_path");
        if (this.location == null) {
            throw new IllegalArgumentException(
                    "dynamic synonym requires `synonyms_path` to be configured");
        }
        this.interval = settings.getAsInt("interval", 60);
        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        this.expand = settings.getAsBoolean("expand", true);
        this.format = settings.get("format", "");

        String tokenizerName = settings.get("tokenizer", "whitespace");

        AnalysisModule.AnalysisProvider<TokenizerFactory> tokenizerFactoryFactory =
                analysisRegistry.getTokenizerProvider(tokenizerName, indexSettings);
        if (tokenizerFactoryFactory == null) {
            throw new IllegalArgumentException("failed to find tokenizer [" + tokenizerName + "] for synonym token filter");
        }
        final TokenizerFactory tokenizerFactory = tokenizerFactoryFactory.get(indexSettings, env, tokenizerName,
                AnalysisRegistry.getSettingsFromIndexSettings(indexSettings, AnalysisRegistry.INDEX_ANALYSIS_TOKENIZER + "." + tokenizerName));

        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = tokenizerFactory == null ? new WhitespaceTokenizer() : tokenizerFactory.create();
                TokenStream stream = ignoreCase ? new LowerCaseFilter(tokenizer) : tokenizer;
                return new TokenStreamComponents(tokenizer, stream);
            }
        };

        SynonymFile synonymFile;
        if (location.startsWith("http://") || location.startsWith("https://")) {
            synonymFile = new RemoteSynonymFile(env, analyzer, expand, format,
                    location);
        } else {
            synonymFile = new LocalSynonymFile(env, analyzer, expand, format,
                    location);
        }
        synonymMap = synonymFile.reloadSynonymMap();

        //加入监控队列，定时load
        scheduledFutures.putIfAbsent(this.indexName, new CopyOnWriteArrayList<>());
        scheduledFutures.get(this.indexName)
                .add(monitorPool.scheduleAtFixedRate(new Monitor(synonymFile), interval, interval, TimeUnit.SECONDS));
        logger.info("{} add Monitor to pool : {}", this, monitorPool.toString());

//        scheduledFuture = pool.scheduleAtFixedRate(new Monitor(synonymFile),
//                interval, interval, TimeUnit.SECONDS);
//        logger.info("{} add Monitor to pool : {}", this, pool.toString());
//
//        String monitorName = indexSettings.getIndex().getName() + "_" + name;
//        if(!monitorSet.contains(monitorName)){
//
//            monitorSet.add(monitorName);
//        }
    }


    /**
     * 每个索引下创建n个TokenStream，即create方法会调用多次，此方法有并发，被多线程调用
     */
    @Override
    public TokenStream create(TokenStream tokenStream) {
        logger.info("###################### create, synonymMap = {}", synonymMap);
        // fst is null means no synonyms
        if (synonymMap == null || synonymMap.fst == null) {
            return tokenStream;
        }

        DynamicSynonymFilter dynamicSynonymFilter = new DynamicSynonymFilter(tokenStream, synonymMap, ignoreCase);
        dynamicSynonymFilters.putIfAbsent(this.indexName, new CopyOnWriteArrayList<>());
        dynamicSynonymFilters.get(this.indexName).add(dynamicSynonymFilter);

        return dynamicSynonymFilter;
    }


    /**
     * 清理同义词资源
     */
    public static void closeIndDynamicSynonym(String indexName) {
        boolean isExists = false;
        CopyOnWriteArrayList<ScheduledFuture> futures = scheduledFutures.remove(indexName);
        if (futures != null && futures.size() > 0) {
            for (ScheduledFuture sf : futures) {
                sf.cancel(true);
            }
            isExists = true;
        }
        dynamicSynonymFilters.remove(indexName);
        if(isExists){
            logger.info("closeDynamicSynonym！ indexName:{} scheduledFutures.size:{} dynamicSynonymFilters.size:{}",
                    indexName, scheduledFutures.size(), dynamicSynonymFilters.size());
        }
    }

    /**
     * 清理插件资源
     */
    public static void closeDynamicSynonym() {
        dynamicSynonymFilters.clear();
        scheduledFutures.clear();
        monitorPool.shutdownNow();
    }

    public class Monitor implements Runnable {

        private SynonymFile synonymFile;

        Monitor(SynonymFile synonymFile) {
            this.synonymFile = synonymFile;
        }

        @Override
        public void run() {
            try {
                if (synonymFile.isNeedReloadSynonymMap()) {
                    SynonymMap newSynonymMap = synonymFile.reloadSynonymMap();
                    if (newSynonymMap == null || newSynonymMap.fst == null) {
                        logger.error("Monitor thread reload remote synonym non-null! indexName:{} path:{}",
                                indexName, synonymFile.getLocation());
                        return;
                    }
                    synonymMap = newSynonymMap;
                    CopyOnWriteArrayList<DynamicSynonymFilter> dynamicSynonymFilters = DynamicSynonymTokenFilterFactory.dynamicSynonymFilters.get(indexName);
                    if(dynamicSynonymFilters != null && dynamicSynonymFilters.size() > 0){
                        Iterator<DynamicSynonymFilter> filters = dynamicSynonymFilters.iterator();
                        while (filters.hasNext()) {
                            filters.next().update(synonymMap);
                            logger.info("success reload synonym success! indexName:{} path:{}", indexName, synonymFile.getLocation());
                        }
                    }else {
                        logger.info("there is not dynamicSynonymFilters exist for the indexName: {}", indexName);
                    }
                }
            } catch (Exception e) {
                logger.error("Monitor thread reload remote synonym error! indexName:{} path:{}",
                        indexName, synonymFile.getLocation(), e);
            }
        }
    }

}