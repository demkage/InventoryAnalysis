package com.eter.spark.app.inventoryanalysis.transformer;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Created by abosii on 5/29/2017.
 */
public class NormalizeScoreTransformer extends Transformer {
    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        return null;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return null;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return null;
    }

    public String uid() {
        return null;
    }
}
