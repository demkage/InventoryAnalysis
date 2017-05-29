package com.eter.spark.app.inventoryanalysis.transformer;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.max;

/**
 * Created by abosii on 5/29/2017.
 */
public class NormalizeScoreTransformer extends Transformer {
    private static final long serialVersionUID = 6045677004593190401L;
    private String scoreCol = "baseScore";
    private String productCol = "productid";
    private String outputCol = "score";
    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> normal = dataset.groupBy(dataset.col(scoreCol))
                .agg(dataset.col(scoreCol), max(dataset.col(scoreCol)));

        normal = normal
                .withColumn(outputCol, normal.col("max(" + scoreCol + ")").minus(normal.col(scoreCol)));

        return normal;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return this;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        structType.add(outputCol, DataTypes.IntegerType);
        return structType;
    }

    public String uid() {
        return String.valueOf(serialVersionUID);
    }
}
