/**
 *
 */
package com.plugins.elasticsearch.plugin.synonym.analysis;

import org.apache.lucene.analysis.synonym.SynonymMap;

import java.io.Reader;


public interface SynonymFile {

    SynonymMap reloadSynonymMap();

    boolean isNeedReloadSynonymMap();

    Reader getReader();

    public  String getLocation();
}