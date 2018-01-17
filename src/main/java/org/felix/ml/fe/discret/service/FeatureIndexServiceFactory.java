package org.felix.ml.fe.discret.service;

import org.felix.ml.fe.discret.service.impl.BlockMemFeatureIndexServiceImpl;
import org.felix.ml.fe.discret.service.impl.ListFeatureIndexServiceImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * 2
 */
public class FeatureIndexServiceFactory {
    public static IFeatureIndexService getFeatureIndexService(File dicFile)
            throws FileNotFoundException, IOException, UnsupportException {
        IFeatureIndexService featureIndexService = new BlockMemFeatureIndexServiceImpl();
        try {
            featureIndexService.init(dicFile);
        } catch (UnsupportException e) {
            featureIndexService = null;
            featureIndexService = new ListFeatureIndexServiceImpl();
            featureIndexService.init(dicFile);
        }
        return featureIndexService;
    }
}
