package org.felix.ml.fe.discret.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 *
 */
public interface IFeatureIndexService {
    public void init(File dicFile) throws FileNotFoundException, IOException, UnsupportException;

    public int size();

    public int search(String key);

    public String get(int index);
}
