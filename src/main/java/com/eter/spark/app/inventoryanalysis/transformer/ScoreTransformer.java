package com.eter.spark.app.inventoryanalysis.transformer;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Created by abosii on 5/24/2017.
 */
public class ScoreTransformer extends Transformer {
    private static final long serialVersionUID = 6334768909103748124L;
    private String salesCountCol = "sales";
    private String lastSaleCol = "lastsale";

    private String outputCol = "baseScore";


    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        return dataset.withColumn(outputCol,
                dataset.col(salesCountCol).divide(dataset.col(lastSaleCol).plus(1)));

    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return this;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        structType.add(outputCol, DataTypes.DoubleType);
        return structType;
    }

    public String uid() {
        return String.valueOf(serialVersionUID);
    }
}
